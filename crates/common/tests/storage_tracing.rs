// OpenDAL-specific tests are disabled while using native object_store backends.
#![cfg(any())]

use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use opendal::{
    Buffer, Capability, EntryMode, Error, ErrorKind, Metadata, OperationContext, Operator, Result,
    layers::RetryLayer, raw::*, services::Memory,
};
use se_common::storage_tracing::TracedRetryLayer;
use tracing::{
    Instrument, Subscriber,
    field::{self, Visit},
    info_span,
    span::{Attributes, Id, Record},
};
use tracing_subscriber::{
    Layer as SubscriberLayer, layer::Context, prelude::*, registry::LookupSpan,
};

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

#[derive(Default)]
struct Fields(BTreeMap<String, String>);

#[derive(Debug, Default)]
struct FlakyService {
    attempts: AtomicU64,
}

#[derive(Default)]
struct FlakyWriter {
    writes: usize,
    closes: usize,
    bytes: u64,
}

#[tokio::test]
async fn traces_public_storage_operations() {
    let capture = Capture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let _guard = tracing::subscriber::set_default(subscriber);
    let operator = Operator::new(Memory::default())
        .unwrap()
        .layer(TracedRetryLayer::default());

    async {
        let mut writer = operator.writer("data/file").await.unwrap();
        writer.write("abc").await.unwrap();
        writer.write("defghijkl").await.unwrap();
        writer.close().await.unwrap();
        drop(writer);
        let bytes = operator.read_with("data/file").range(2..7).await.unwrap();
        assert_eq!(bytes.to_vec(), b"cdefg");
        assert_eq!(
            operator.stat("data/file").await.unwrap().content_length(),
            12
        );
        assert!(!operator.exists("missing").await.unwrap());
        assert_eq!(operator.list("data/").await.unwrap().len(), 1);
        assert_eq!(
            operator.read("missing").await.unwrap_err().kind(),
            ErrorKind::NotFound
        );
        operator.delete("data/file").await.unwrap();
    }
    .instrument(info_span!("reconcile.materialize"))
    .await;

    insta::assert_yaml_snapshot!(*capture.0.lock().unwrap(), @r#"
    - io.bucket: "<memory>"
      io.operation: put
      io.path: data/file
      io.retry_count: "0"
      io.root: /
      level: INFO
      parent: reconcile.materialize
    - io.bucket: "<memory>"
      io.operation: get
      io.path: data/file
      io.retry_count: "0"
      io.root: /
      level: INFO
      parent: reconcile.materialize
    - io.bucket: "<memory>"
      io.operation: head
      io.path: data/file
      io.retry_count: "0"
      io.root: /
      level: INFO
      parent: reconcile.materialize
    - error: "true"
      error.kind: NotFound
      io.bucket: "<memory>"
      io.operation: head
      io.path: missing
      io.retry_count: "0"
      io.root: /
      level: INFO
      parent: reconcile.materialize
    - io.bucket: "<memory>"
      io.operation: list
      io.path: data/
      io.retry_count: "0"
      io.root: /
      level: INFO
      parent: reconcile.materialize
    - error: "true"
      error.kind: NotFound
      io.bucket: "<memory>"
      io.operation: get
      io.path: missing
      io.retry_count: "0"
      io.root: /
      level: INFO
      parent: reconcile.materialize
    - io.bucket: "<memory>"
      io.operation: delete
      io.path: data/file
      io.retry_count: "0"
      io.root: /
      level: INFO
      parent: reconcile.materialize
    "#);
}

#[tokio::test]
async fn traces_aborted_write() {
    let capture = Capture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let _guard = tracing::subscriber::set_default(subscriber);
    let operator = Operator::new(Memory::default())
        .unwrap()
        .layer(TracedRetryLayer::default());
    let mut writer = operator.writer("aborted").await.unwrap();
    writer.write("uncommitted").await.unwrap();
    writer.abort().await.unwrap();
    drop(writer);

    insta::assert_yaml_snapshot!(*capture.0.lock().unwrap(), @r#"
    - io.bucket: "<memory>"
      io.operation: put
      io.path: aborted
      io.retry_count: "0"
      io.root: /
      level: INFO
    "#);
}

#[tokio::test]
async fn counts_retries_across_streaming_calls_and_isolates_operations() {
    let capture = Capture::default();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    let operator = Operator::from_parts(OperationContext::new(), Arc::new(FlakyService::default()))
        .layer(TracedRetryLayer::new(
            RetryLayer::default()
                .with_max_times(2)
                .with_min_delay(Duration::ZERO)
                .with_max_delay(Duration::ZERO),
        ));

    assert_eq!(
        operator.stat("recovered").await.unwrap().content_length(),
        17
    );
    let mut writer = operator.writer("stream").await.unwrap();
    writer.write("abc").await.unwrap();
    // Interleave another operation while retaining the writer's retry state.
    assert_eq!(
        operator.stat("denied").await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    writer.write("defgh").await.unwrap();
    assert_eq!(writer.close().await.unwrap().content_length(), 8);
    drop(writer);
    assert_eq!(
        operator.stat("exhausted").await.unwrap_err().kind(),
        ErrorKind::Unexpected
    );

    insta::assert_yaml_snapshot!(*capture.0.lock().unwrap(), @r#"
    - io.bucket: test
      io.operation: head
      io.path: recovered
      io.retry_count: "2"
      io.retry_error_kind: Unexpected
      io.root: /
      level: INFO
    - error: "true"
      error.kind: PermissionDenied
      io.bucket: test
      io.operation: head
      io.path: denied
      io.retry_count: "0"
      io.root: /
      level: INFO
    - io.bucket: test
      io.operation: put
      io.path: stream
      io.retry_count: "4"
      io.retry_error_kind: Unexpected
      io.root: /
      level: INFO
    - error: "true"
      error.kind: Unexpected
      io.bucket: test
      io.operation: head
      io.path: exhausted
      io.retry_count: "2"
      io.retry_error_kind: Unexpected
      io.root: /
      level: INFO
    "#);
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &field::Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &field::Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> SubscriberLayer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if attrs.metadata().name() != "storage.operation" {
            return;
        }
        let span = ctx.span(id).unwrap();
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        // OpenDAL uses an allocation address as the memory backend's bucket name.
        if fields
            .0
            .get("io.bucket")
            .is_some_and(|bucket| bucket.starts_with("0x"))
        {
            fields.0.insert("io.bucket".into(), "<memory>".into());
        }
        fields
            .0
            .insert("level".into(), attrs.metadata().level().to_string());
        if let Some(parent) = span.parent() {
            fields.0.insert("parent".into(), parent.name().into());
        }
        span.extensions_mut().insert(fields);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        if let Some(fields) = ctx.span(id).unwrap().extensions_mut().get_mut::<Fields>() {
            values.record(fields);
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        if let Some(fields) = ctx.span(&id).unwrap().extensions_mut().remove::<Fields>() {
            self.0.lock().unwrap().push(fields.0);
        }
    }
}

impl Service for FlakyService {
    type Reader = ();
    type Writer = FlakyWriter;
    type Lister = ();
    type Deleter = ();
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new("test", "/", "test")
    }

    fn capability(&self) -> Capability {
        Capability {
            stat: true,
            write: true,
            write_can_multi: true,
            ..Default::default()
        }
    }

    async fn stat(&self, _: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let attempt = self.attempts.fetch_add(1, Ordering::Relaxed);
        match path {
            "denied" => Err(Error::new(ErrorKind::PermissionDenied, "denied")),
            "exhausted" => Err(Error::new(ErrorKind::Unexpected, "temporary").set_temporary()),
            _ if attempt < 2 => Err(Error::new(ErrorKind::Unexpected, "temporary").set_temporary()),
            _ => Ok(RpStat::new(
                Metadata::new(EntryMode::FILE).with_content_length(17),
            )),
        }
    }

    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        Ok(FlakyWriter::default())
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        ().create_dir(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        ().read(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        ().delete(ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        ().list(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<Self::Copier> {
        ().copy(ctx, from, to, args, opts)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        ().rename(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        ().presign(ctx, path, args).await
    }
}

impl oio::Write for FlakyWriter {
    async fn write(&mut self, bytes: Buffer) -> Result<()> {
        self.writes += 1;
        if self.writes % 2 == 1 {
            return Err(Error::new(ErrorKind::Unexpected, "temporary write").set_temporary());
        }
        self.bytes += bytes.len() as u64;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.closes += 1;
        if self.closes <= 2 {
            return Err(Error::new(ErrorKind::Unexpected, "temporary close").set_temporary());
        }
        Ok(Metadata::new(EntryMode::FILE).with_content_length(self.bytes))
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}
