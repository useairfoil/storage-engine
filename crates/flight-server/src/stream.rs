use std::{
    pin::Pin,
    task::{Context, Poll},
};

use tokio_stream::Stream;
use tonic::Status;
use tracing::Span;

/// Keeps the RPC span active while the transport polls its response stream.
pub(super) struct TracedStream<T> {
    inner: Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>,
    span: Span,
    messages: u64,
}

impl<T> TracedStream<T> {
    pub(super) fn new(inner: impl Stream<Item = Result<T, Status>> + Send + 'static) -> Self {
        Self {
            inner: Box::pin(inner),
            span: Span::current(),
            messages: 0,
        }
    }
}

impl<T> Stream for TracedStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let _entered = this.span.enter();
        let result = this.inner.as_mut().poll_next(cx);
        if let Poll::Ready(Some(item)) = &result {
            this.messages += 1;
            if let Err(status) = item {
                tracing::error!(code = ?status.code(), "Flight response stream failed");
            }
        }
        result
    }
}

impl<T> Drop for TracedStream<T> {
    fn drop(&mut self) {
        self.span.record("response_messages", self.messages);
    }
}
