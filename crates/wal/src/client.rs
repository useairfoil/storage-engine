use std::sync::Arc;

use se_metadata::CatalogId;
use uuid::Uuid;

use crate::{BatchCommit, BatchRead};

pub trait WalClient: BatchCommit + BatchRead {}

pub trait WalClientFactory: Send + Sync {
    /// Creates a WAL client scoped to the supplied catalog and table.
    fn wal_client(&self, catalog_id: CatalogId, table_uuid: Uuid) -> Arc<dyn WalClient>;
}

impl<T: BatchCommit + BatchRead> WalClient for T {}
