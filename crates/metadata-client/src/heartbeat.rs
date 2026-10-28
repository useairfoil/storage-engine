use std::{future::Future, pin::Pin};

use crate::Result;

pub struct Heartbeat {
    beat: Box<dyn Fn() -> HeartbeatFuture + Send + Sync>,
}

type HeartbeatFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

impl Heartbeat {
    /// Creates an owned progress handle that sends a heartbeat only when explicitly beaten.
    pub fn new<F, Fut>(beat: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self {
            beat: Box::new(move || Box::pin(beat())),
        }
    }

    /// Reports progress and waits for the task lease renewal to succeed.
    pub async fn beat(&self) -> Result<()> {
        (self.beat)().await
    }
}
