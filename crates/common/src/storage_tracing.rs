//! Logical storage-operation spans, outside retries and without HTTP/chunk spans.

use std::{
    future::Future,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use opendal::{
    Buffer, BytesRange, Capability, Metadata, OperationContext, Result,
    layers::{RetryEvent, RetryLayer},
    raw::*,
};
use tracing::{Instrument, Span, field, info_span};

tokio::task_local! {
    static RETRIES: OperationSpan;
}

/// Retries storage operations and traces them at INFO level. Apply outside timeout layers.
/// `exists` and `stat` both reach the service as `head`; deletion spans cover a
/// batch and identify its first path. No credentials, headers, or payloads are logged.
/// Retry counts accumulate across all body calls within an operation span.
#[derive(Debug, Clone, Default)]
pub struct TracedRetryLayer {
    retry: RetryLayer,
}

#[derive(Clone)]
struct OperationSpan {
    span: Span,
    retries: Arc<AtomicU64>,
}

#[derive(Debug)]
struct TracingService {
    inner: Servicer,
}

struct TracingReader {
    inner: Servicer,
    ctx: OperationContext,
    args: OpRead,
    path: String,
    parent: Span,
}

struct Traced<T> {
    inner: T,
    span: OperationSpan,
    has_path: bool,
}

impl TracedRetryLayer {
    /// Uses the supplied retry policy, replacing its interceptor with trace accounting.
    pub fn new(retry: RetryLayer) -> Self {
        Self { retry }
    }
}

impl Layer for TracedRetryLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        let inner = self
            .retry
            .clone()
            .with_notify(record_retry)
            .apply_service(inner);
        Arc::new(TracingService { inner })
    }
}

impl OperationSpan {
    fn in_scope<T>(&self, f: impl FnOnce() -> T) -> T {
        RETRIES.sync_scope(self.clone(), || self.span.in_scope(f))
    }

    async fn run<T>(&self, future: impl Future<Output = T>) -> T {
        // Poll-scoped state isolates concurrent operations and preserves counts
        // across streaming calls, where the retry layer resets its attempt number.
        RETRIES
            .scope(self.clone(), future.instrument(self.span.clone()))
            .await
    }
}

impl Deref for OperationSpan {
    type Target = Span;

    fn deref(&self) -> &Span {
        &self.span
    }
}

impl Service for TracingService {
    type Reader = TracingReader;
    type Writer = Traced<oio::Writer>;
    type Lister = Traced<oio::Lister>;
    type Deleter = Traced<oio::Deleter>;
    type Copier = Traced<oio::Copier>;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }
    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        // Reader construction is lazy so its retries belong to the actual GET,
        // rather than emitting an extra operation for creating a reader handle.
        Ok(TracingReader {
            inner: self.inner.clone(),
            ctx: ctx.clone(),
            args,
            path: path.to_owned(),
            parent: Span::current(),
        })
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let span = operation_span(&self.info(), "put", path, &Span::current());
        let result = span.in_scope(|| self.inner.write(ctx, path, args));
        record_result(&span, &result);
        result.map(|inner| Traced::new(inner, span))
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let span = operation_span(&self.info(), "head", path, &Span::current());
        let result = span.run(self.inner.stat(ctx, path, args)).await;
        record_result(&span, &result);
        result
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let span = operation_span(&self.info(), "list", path, &Span::current());
        let result = span.in_scope(|| self.inner.list(ctx, path, args));
        record_result(&span, &result);
        result.map(|inner| Traced::new(inner, span))
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let span = operation_span(&self.info(), "delete", "", &Span::current());
        let result = span.in_scope(|| self.inner.delete(ctx));
        record_result(&span, &result);
        result.map(|inner| Traced::new(inner, span))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<Self::Copier> {
        let span = operation_span(&self.info(), "copy", from, &Span::current());
        let result = span.in_scope(|| self.inner.copy(ctx, from, to, args, opts));
        record_result(&span, &result);
        result.map(|inner| Traced::new(inner, span))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        let span = operation_span(&self.info(), "rename", from, &Span::current());
        let result = span.run(self.inner.rename(ctx, from, to, args)).await;
        record_result(&span, &result);
        result
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let span = operation_span(&self.info(), "create_dir", path, &Span::current());
        let result = span.run(self.inner.create_dir(ctx, path, args)).await;
        record_result(&span, &result);
        result
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        let span = operation_span(&self.info(), "presign", path, &Span::current());
        let result = span.run(self.inner.presign(ctx, path, args)).await;
        record_result(&span, &result);
        result
    }
}

impl oio::Read for TracingReader {
    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let span = operation_span(&self.inner.info(), "get", &self.path, &self.parent);
        let result = span
            .run(async {
                let reader = self.inner.read(&self.ctx, &self.path, self.args.clone())?;
                reader.read(range).await
            })
            .await;
        record_result(&span, &result);
        result
    }

    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let span = operation_span(&self.inner.info(), "get", &self.path, &self.parent);
        let result = span
            .run(async {
                let reader = self.inner.read(&self.ctx, &self.path, self.args.clone())?;
                reader.open(range).await
            })
            .await;
        record_result(&span, &result);
        let (reply, stream) = result?;
        Ok((reply, Box::new(Traced::new(stream, span))))
    }
}

impl<T> Traced<T> {
    fn new(inner: T, span: OperationSpan) -> Self {
        Self {
            inner,
            span,
            has_path: false,
        }
    }
}

impl<T: oio::ReadStream> oio::ReadStream for Traced<T> {
    async fn read(&mut self) -> Result<Buffer> {
        let result = self.span.run(self.inner.read()).await;
        record_result(&self.span, &result);
        result
    }
}

impl<T: oio::Write> oio::Write for Traced<T> {
    async fn write(&mut self, bytes: Buffer) -> Result<()> {
        let result = self.span.run(self.inner.write(bytes)).await;
        record_result(&self.span, &result);
        result
    }

    async fn close(&mut self) -> Result<Metadata> {
        let result = self.span.run(self.inner.close()).await;
        record_result(&self.span, &result);
        result
    }

    async fn abort(&mut self) -> Result<()> {
        let result = self.span.run(self.inner.abort()).await;
        record_result(&self.span, &result);
        result
    }
}

impl<T: oio::List> oio::List for Traced<T> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let result = self.span.run(self.inner.next()).await;
        record_result(&self.span, &result);
        result
    }
}

impl<T: oio::Delete> oio::Delete for Traced<T> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        if !self.has_path {
            self.span.record("io.path", path);
            self.has_path = true;
        }
        let result = self.span.run(self.inner.delete(path, args)).await;
        record_result(&self.span, &result);
        result
    }

    async fn close(&mut self) -> Result<()> {
        let result = self.span.run(self.inner.close()).await;
        record_result(&self.span, &result);
        result
    }
}

impl<T: oio::Copy> oio::Copy for Traced<T> {
    async fn next(&mut self) -> Result<Option<usize>> {
        let result = self.span.run(self.inner.next()).await;
        record_result(&self.span, &result);
        result
    }

    async fn close(&mut self) -> Result<Metadata> {
        let result = self.span.run(self.inner.close()).await;
        record_result(&self.span, &result);
        result
    }

    async fn abort(&mut self) -> Result<()> {
        let result = self.span.run(self.inner.abort()).await;
        record_result(&self.span, &result);
        result
    }
}

fn operation_span(
    info: &ServiceInfo,
    operation: &'static str,
    path: &str,
    parent: &Span,
) -> OperationSpan {
    let span = info_span!(parent: parent, "storage.operation",
        io.operation = operation, io.bucket = %info.name(),
        io.root = %info.root(), io.path = path,
        io.retry_count = 0u64, io.retry_error_kind = field::Empty,
        error = field::Empty, error.kind = field::Empty);
    OperationSpan {
        span,
        retries: Arc::new(AtomicU64::new(0)),
    }
}

fn record_retry(event: RetryEvent<'_>) {
    let _ = RETRIES.try_with(|span| {
        let count = span.retries.fetch_add(1, Ordering::Relaxed) + 1;
        span.record("io.retry_count", count);
        span.record("io.retry_error_kind", field::display(event.err.kind()));
    });
}

fn record_result<T>(span: &Span, result: &Result<T>) {
    if let Err(error) = result {
        span.record("error", true);
        span.record("error.kind", field::display(error.kind()));
    }
}
