use std::sync::Arc;

use object_store::{ObjectStore, path::Path};
use uuid::Uuid;

mod encoding;
mod read;
mod reader;
mod write;

pub use read::TableRead;
pub use reader::TableReader;
pub use write::TableWrite;

/// A database for a table's metadata.
#[derive(Clone)]
pub struct TableDb {
    db: slatedb::Db,
    engine_object_store: Arc<dyn ObjectStore>,
}

/// The error type for table database operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An existing database has no table UUID.
    #[error("table uuid is missing")]
    TableUuidMissing,
    /// SlateDB error.
    #[error("slatedb error: {0}")]
    SlateDb(#[from] slatedb::Error),
    /// The stored table UUID is not a 16-byte UUID.
    #[error("invalid table uuid: {0}")]
    InvalidTableUuid(#[from] uuid::Error),
    /// A stored numeric value has the wrong length.
    #[error("invalid numeric value: {0}")]
    InvalidNumericValue(#[from] std::array::TryFromSliceError),
    /// A schema or sort order could not be encoded or decoded.
    #[error("invalid metadata encoding: {0}")]
    MetadataEncoding(#[from] serde_json::Error),
    /// The database belongs to a different Iceberg table.
    #[error("table uuid mismatch: catalog {catalog_uuid}, stored {stored_uuid}")]
    UuidMismatch {
        catalog_uuid: Uuid,
        stored_uuid: Uuid,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl TableDb {
    /// Opens an existing database, returning `None` if no manifest exists.
    ///
    /// The existence check and open are not atomic with concurrent deletion.
    pub async fn try_load(path: Path, object_store: Arc<dyn ObjectStore>) -> Result<Option<Self>> {
        let admin = slatedb::admin::AdminBuilder::new(path.clone(), object_store.clone()).build();
        if admin.read_manifest(None).await?.is_none() {
            return Ok(None);
        }
        let db = Self::open_db(path, object_store.clone()).await?;
        let uuid = db.get_table_uuid().await?.ok_or(Error::TableUuidMissing)?;
        let engine_object_store = Arc::new(object_store::prefix::PrefixStore::new(
            object_store,
            uuid.to_string(),
        ));

        Ok(Some(Self {
            db,
            engine_object_store,
        }))
    }

    /// Opens or creates the database, initializing or validating the table UUID.
    pub async fn load_or_init(
        path: Path,
        table: iceberg::table::Table,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let db = Self::open_db(path, object_store.clone()).await?;
        let engine_object_store = Arc::new(object_store::prefix::PrefixStore::new(
            object_store,
            table.metadata().uuid().to_string(),
        ));
        let table_db = Self {
            db,
            engine_object_store,
        };

        let tx = table_db.begin_transaction().await?;

        // Assert the table uuid matches.
        let catalog_uuid = table.metadata().uuid();

        match tx.get_table_uuid().await? {
            Some(stored_uuid) if stored_uuid != catalog_uuid => {
                return Err(Error::UuidMismatch {
                    catalog_uuid,
                    stored_uuid,
                });
            }
            Some(_) => {}
            None => {
                tx.put_table_uuid(catalog_uuid)?;
                tx.put_current_schema_id(table.metadata().current_schema_id())?;
                tx.put_default_sort_order_id(table.metadata().default_sort_order_id())?;
                for schema in table.metadata().schemas_iter() {
                    tx.put_schema(schema.schema_id(), schema)?;
                }
                for sort_order in table.metadata().sort_orders_iter() {
                    tx.put_sort_order(sort_order.order_id, sort_order)?;
                }
            }
        }

        if let Some(handle) = tx.commit().await? {
            handle.await_durable().await?;
        }

        Ok(table_db)
    }

    /// Reads the persisted table UUID, returning an error if it is missing.
    pub async fn get_table_uuid(&self) -> Result<Uuid> {
        self.db
            .get_table_uuid()
            .await?
            .ok_or(Error::TableUuidMissing)
    }

    /// Begins a transaction that detects both read-write and write-write conflicts.
    pub async fn begin_transaction(&self) -> Result<slatedb::DbTransaction> {
        Ok(self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await?)
    }

    /// Returns a point-in-time view for consistent metadata reads.
    pub async fn snapshot(&self) -> Result<Arc<slatedb::DbSnapshot>> {
        Ok(self.db.snapshot().await?)
    }

    /// Returns the cached object store rooted at the table UUID within its catalog.
    pub fn engine_object_store(&self) -> Arc<dyn ObjectStore> {
        self.engine_object_store.clone()
    }

    /// Flushes pending writes, then closes the database, including all clones.
    pub async fn flush_and_close(self) -> Result<()> {
        self.db.flush().await?;
        self.db.close().await?;
        Ok(())
    }

    async fn open_db(path: Path, object_store: Arc<dyn ObjectStore>) -> Result<slatedb::Db> {
        // Disable garbage collector and compaction since we want to have control over it.
        let settings = slatedb::Settings {
            garbage_collector_options: None,
            compactor_options: None,
            ..Default::default()
        };

        let db = slatedb::Db::builder(path, object_store.clone())
            .with_settings(settings)
            .build()
            .await?;

        Ok(db)
    }
}
