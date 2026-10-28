use async_trait::async_trait;

use crate::{BatchMetadata, Result};

#[async_trait]
pub trait BatchCommit: Send + Sync {
    /// Commits metadata for an uploaded WAL batch file.
    async fn commit(&self, batch: BatchMetadata) -> Result<()>;
}
