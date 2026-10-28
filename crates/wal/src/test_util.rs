use std::sync::Arc;

use async_trait::async_trait;
use se_metadata::CatalogId;
use tokio::sync::{Notify, Semaphore};
use uuid::Uuid;

use crate::{
    BatchCommit, BatchDescriptor, BatchMetadata, BatchRead, InMemoryWal, Result, WalClient,
    WalClientFactory,
};

#[derive(Clone)]
pub struct BlockedCommit {
    pub wal: InMemoryWal,
    pub entered: Arc<Notify>,
    pub release: Arc<Semaphore>,
    result: Result<()>,
}

impl BlockedCommit {
    /// Blocks commits until released, then commits to memory or returns the supplied error.
    pub fn new(result: Result<()>) -> Self {
        Self {
            wal: InMemoryWal::default(),
            entered: Arc::new(Notify::new()),
            release: Arc::new(Semaphore::new(0)),
            result,
        }
    }
}

#[async_trait]
impl BatchCommit for BlockedCommit {
    async fn commit(&self, batch: BatchMetadata) -> Result<()> {
        self.entered.notify_one();
        // PANIC: The release semaphore must remain open while commits are blocked.
        self.release.acquire().await.unwrap().forget();
        self.result.clone()?;

        self.wal.commit(batch).await
    }
}

#[async_trait]
impl BatchRead for BlockedCommit {
    async fn claim_reader(&self) -> Result<u64> {
        self.wal.claim_reader().await
    }

    async fn read_after(&self, sequence: Option<u64>, max: usize) -> Result<Vec<BatchDescriptor>> {
        self.wal.read_after(sequence, max).await
    }

    async fn ack_through(&self, epoch: u64, sequence: u64) -> Result<()> {
        self.wal.ack_through(epoch, sequence).await
    }
}

impl WalClientFactory for BlockedCommit {
    fn wal_client(&self, _: CatalogId, _: Uuid) -> Arc<dyn WalClient> {
        Arc::new(self.clone())
    }
}
