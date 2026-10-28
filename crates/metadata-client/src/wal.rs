use std::sync::Arc;

use async_trait::async_trait;
pub use se_wal::WalRequest;
use se_wal::{BatchCommit, BatchDescriptor, BatchMetadata, BatchRead, Error, Result};
use uuid::Uuid;

use crate::{CatalogId, MetadataClient};

#[derive(Clone)]
pub struct WalClient {
    metadata: Arc<dyn MetadataClient>,
    catalog_id: CatalogId,
    table_uuid: Uuid,
}

impl WalClient {
    /// Scopes a metadata client to a catalog and table UUID for WAL operations.
    pub fn new(metadata: Arc<dyn MetadataClient>, catalog_id: CatalogId, table_uuid: Uuid) -> Self {
        Self {
            metadata,
            catalog_id,
            table_uuid,
        }
    }
}

#[async_trait]
impl BatchCommit for WalClient {
    async fn commit(&self, batch: BatchMetadata) -> Result<()> {
        self.metadata
            .wal_append(&self.catalog_id, self.table_uuid, batch)
            .await
    }
}

#[async_trait]
impl BatchRead for WalClient {
    async fn claim_reader(&self) -> Result<u64> {
        Err(Error::InvalidInput(
            "materialization ownership is acquired through task assignment".into(),
        ))
    }

    async fn read_after(&self, sequence: Option<u64>, max: usize) -> Result<Vec<BatchDescriptor>> {
        self.metadata
            .wal_read_after(&self.catalog_id, self.table_uuid, sequence, max)
            .await
    }

    async fn ack_through(&self, _epoch: u64, _sequence: u64) -> Result<()> {
        Err(Error::InvalidInput(
            "WAL is acknowledged through materialization completion".into(),
        ))
    }
}
