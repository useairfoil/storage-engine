//! Storage Engine ingestion pipeline.

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
pub use buffer::{Clock, Error as WalError, SystemClock};
use buffer::{CompressionType, Producer, ProducerConfig};
use iceberg::arrow::schema_to_arrow_schema;
use se_meta_store::{
    catalog::{CatalogName, CatalogStore, IcebergErrorKind, NamespaceIdent, TableIdent},
    table::StoredTable,
};
use tokio::sync::{OnceCell, RwLock};

const WAL_MANIFEST_PATH: &str = "ingest/manifest";

/// The error type for ingestion operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The ingestor is shutting down or has closed.
    #[error("ingestor is closed")]
    Closed,
    /// The requested catalog does not exist.
    #[error("catalog not found: {0}")]
    CatalogNotFound(CatalogName),
    /// The requested table does not exist.
    #[error("table not found: {0}")]
    TableNotFound(TableIdent),
    /// Failed to access the catalog or its stored table metadata.
    #[error("catalog store error: {0}")]
    CatalogStore(#[from] se_meta_store::catalog::Error),
    /// The stored Iceberg schema could not be converted to an Arrow schema.
    #[error("invalid table schema: {0}")]
    TableSchema(#[from] iceberg::Error),
    /// Failed to initialize the producer.
    #[error("buffer error: {0}")]
    Buffer(#[from] buffer::Error),
}

/// The error type for record batch writes.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WriteError {
    /// The record batch schema does not match the table schema.
    #[error("invalid record batch schema")]
    InvalidSchema,
    /// The record batch contains no rows.
    #[error("record batch is empty")]
    EmptyBatch,
    /// The record batch could not be submitted to the buffer.
    #[error("failed to write record batch")]
    Write,
    /// The record batch could not be committed to the buffer.
    #[error("failed to flush record batch")]
    Flush,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Creates table-specific ingestors.
#[derive(Clone)]
pub struct Ingestor {
    inner: Arc<IngestorState>,
}

/// Ingests record batches into a single table.
///
/// Writes return owned futures so independent writes can run concurrently without
/// retaining a borrow of this handle.
#[derive(Clone)]
pub struct TableIngestor {
    state: Arc<TableState>,
}

type TableSlot = OnceCell<Arc<TableState>>;
type TableSlots = HashMap<(CatalogName, TableIdent), Arc<TableSlot>>;

struct IngestorState {
    catalog_store: CatalogStore,
    clock: Arc<dyn Clock>,
    tables: RwLock<Option<TableSlots>>,
}

struct TableState {
    producer: RwLock<Option<Producer>>,
    table_schema: SchemaRef,
    schema: SchemaRef,
}

/// Open a read-only WAL tailer for a stored table.
pub fn wal_tailer(table: &StoredTable) -> buffer::Tailer {
    buffer::Tailer::with_object_store(WAL_MANIFEST_PATH.into(), table.object_store())
}

impl Ingestor {
    pub fn new(catalog_store: CatalogStore, clock: Arc<dyn Clock>) -> Self {
        Self {
            inner: Arc::new(IngestorState {
                catalog_store,
                clock,
                tables: RwLock::new(Some(HashMap::new())),
            }),
        }
    }

    /// Creates an ingestor for an existing table.
    pub async fn for_table(
        &self,
        catalog: CatalogName,
        namespace: NamespaceIdent,
        table_name: String,
    ) -> Result<TableIngestor> {
        let table_ident = TableIdent::new(namespace, table_name);
        let cell = {
            let mut tables = self.inner.tables.write().await;
            tables
                .as_mut()
                .ok_or(Error::Closed)?
                .entry((catalog.clone(), table_ident.clone()))
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        // Keep close from collecting slots until any in-progress initialization finishes.
        let tables = self.inner.tables.read().await;
        if tables.is_none() {
            return Err(Error::Closed);
        }
        let state = cell
            .get_or_try_init(|| self.init_table(catalog, table_ident))
            .await?
            .clone();
        Ok(TableIngestor { state })
    }

    /// Stop accepting tables and writes, then flush and close every initialized producer.
    pub async fn close(&self) -> Result<()> {
        let mut tables = self.inner.tables.write().await;
        let Some(slots) = tables.take() else {
            return Ok(());
        };
        let mut first_error = None;
        for ((catalog, table), slot) in slots {
            let Some(state) = slot.get() else {
                continue;
            };
            let Some(producer) = state.producer.write().await.take() else {
                continue;
            };
            tracing::debug!(catalog = %catalog, table = %table, "closing ingestion producer");
            if let Err(error) = producer.close().await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), |error| Err(Error::Buffer(error)))
    }

    async fn init_table(
        &self,
        catalog: CatalogName,
        table_ident: TableIdent,
    ) -> Result<Arc<TableState>> {
        let stored_catalog = self
            .inner
            .catalog_store
            .get(catalog.clone())
            .await?
            .ok_or(Error::CatalogNotFound(catalog))?;
        let table = match stored_catalog.try_load_table(table_ident.clone()).await? {
            Some(table) => table,
            None => match stored_catalog.link_table(table_ident.clone()).await {
                Ok(table) => table,
                // Another ingestor may have linked this table while we loaded it.
                Err(source @ se_meta_store::catalog::Error::TableLinkAlreadyExists(_)) => {
                    stored_catalog
                        .try_load_table(table_ident.clone())
                        .await?
                        .ok_or(Error::CatalogStore(source))?
                }
                Err(se_meta_store::catalog::Error::Iceberg(source))
                    if source.kind() == IcebergErrorKind::TableNotFound =>
                {
                    return Err(Error::TableNotFound(table_ident));
                }
                Err(source) => return Err(source.into()),
            },
        };
        let table_schema = Arc::new(schema_to_arrow_schema(table.metadata().schema.as_ref())?);
        // The WAL can contain partial updates that omit even required table
        // columns, which are represented by null placeholders.
        let schema = Arc::new(Schema::new_with_metadata(
            table_schema
                .fields()
                .iter()
                .map(|field| Arc::new(field.as_ref().clone().with_nullable(true)))
                .collect::<Vec<_>>(),
            table_schema.metadata().clone(),
        ));
        let producer = Producer::new(
            ProducerConfig {
                data_path_prefix: "ingest".to_string(),
                manifest_path: WAL_MANIFEST_PATH.to_string(),
                flush_interval: Duration::from_secs(1),
                flush_size_bytes: 64 * 1024 * 1024,
                max_buffered_inputs: 1000,
                batch_compression: CompressionType::None,
            },
            table.object_store(),
            schema.clone(),
            self.inner.clock.clone(),
        )?;
        Ok(Arc::new(TableState {
            producer: RwLock::new(Some(producer)),
            table_schema,
            schema,
        }))
    }
}

impl std::fmt::Debug for Ingestor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ingestor").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for TableIngestor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableIngestor")
            .field("schema", &self.state.schema)
            .finish()
    }
}

impl TableIngestor {
    /// Returns the canonical table schema used by the producer.
    pub fn schema(&self) -> SchemaRef {
        self.state.schema.clone()
    }

    /// Returns the Iceberg table schema used to validate stream fields.
    pub fn table_schema(&self) -> SchemaRef {
        self.state.table_schema.clone()
    }

    /// Returns an owned future that writes one record batch to the table.
    pub fn write(
        &self,
        batch: RecordBatch,
    ) -> impl Future<Output = Result<(), WriteError>> + Send + 'static {
        let state = Arc::clone(&self.state);

        async move {
            if batch.num_rows() == 0 {
                return Err(WriteError::EmptyBatch);
            }
            if batch.schema() != state.schema {
                return Err(WriteError::InvalidSchema);
            }
            let mut handle = {
                let producer = state.producer.read().await;
                producer
                    .as_ref()
                    .ok_or(WriteError::Write)?
                    .produce(batch, Default::default())
                    .await
                    .map_err(|_| WriteError::Write)?
            };
            handle
                .watcher
                .await_durable()
                .await
                .map_err(|_| WriteError::Flush)
        }
    }
}
