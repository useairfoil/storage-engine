use std::{ops::Deref, sync::Arc};

use object_store::{ObjectStore, path::Path, prefix::PrefixStore};
use uuid::Uuid;

use super::{Error, Result, TableRead};

/// A read-only table database that follows the latest durable state.
///
/// Managed checkpoints protect referenced files from garbage collection and
/// require object-store writes. Separate reads are not snapshot-pinned.
pub struct TableReader {
    db: slatedb::DbReader,
    engine_object_store: Arc<dyn ObjectStore>,
}

impl TableReader {
    /// Opens an existing database without creating it, or returns `None` if absent.
    pub async fn try_load(path: Path, object_store: Arc<dyn ObjectStore>) -> Result<Option<Self>> {
        let admin = slatedb::admin::AdminBuilder::new(path.clone(), object_store.clone()).build();
        if admin.read_manifest(None).await?.is_none() {
            return Ok(None);
        }
        let db = slatedb::DbReader::builder(path, object_store.clone())
            .with_reader_mode(slatedb::DbReaderMode::ManagedCheckpoint)
            .build()
            .await?;
        let uuid = db.get_table_uuid().await?.ok_or(Error::TableUuidMissing)?;
        let engine_object_store = Arc::new(PrefixStore::new(object_store, uuid.to_string()));
        Ok(Some(Self {
            db,
            engine_object_store,
        }))
    }

    /// Reads the persisted table UUID, returning an error if it is missing.
    pub async fn get_table_uuid(&self) -> Result<Uuid> {
        self.db
            .get_table_uuid()
            .await?
            .ok_or(Error::TableUuidMissing)
    }

    /// Returns the cached object store rooted at the table UUID within its catalog.
    pub fn engine_object_store(&self) -> Arc<dyn ObjectStore> {
        self.engine_object_store.clone()
    }

    /// Closes the reader and its background tasks.
    pub async fn close(&self) -> Result<()> {
        Ok(self.db.close().await?)
    }
}

impl Deref for TableReader {
    type Target = slatedb::DbReader;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}
