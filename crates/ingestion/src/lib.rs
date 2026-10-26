//! Storage Engine ingestion pipeline.

pub mod metadata;

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
pub use buffer::{Clock, Error as WalError, SystemClock};
use buffer::{CompressionType, Producer, ProducerConfig};
use iceberg::arrow::schema_to_arrow_schema;
use se_common::clock::SystemClock as SchedulerClock;
use se_meta_store::{
    catalog::{CatalogName, CatalogStore, IcebergErrorKind, NamespaceIdent, TableIdent},
    table::{TableDb, TableRead, TableReader},
};
use se_scheduler::{Task, TaskScheduler};
use tokio::sync::{OnceCell, RwLock};
use tracing::{Span, instrument};

use crate::metadata::BatchMetadata;

/// Manifest path of each table's ingestion WAL, relative to its object store.
pub const WAL_MANIFEST_PATH: &str = "ingest/manifest";
/// Data path of each table's ingestion WAL, relative to its object store.
pub const WAL_DATA_PATH: &str = "ingest/wal";

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
    /// Failed to read the table database.
    #[error("table database error: {0}")]
    TableDb(#[from] se_meta_store::table::Error),
    /// The current schema ID is missing from the table database.
    #[error("current schema id is missing")]
    CurrentSchemaIdMissing,
    /// The current schema is missing from the table database.
    #[error("table schema is missing: {0}")]
    SchemaMissing(i32),
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
    scheduler_clock: Arc<dyn SchedulerClock>,
    scheduler: Arc<dyn TaskScheduler>,
    tables: RwLock<Option<TableSlots>>,
}

struct TableState {
    producer: RwLock<Option<Producer>>,
    current_schema_id: i32,
    table_schema: SchemaRef,
    catalog: CatalogName,
    table_ident: TableIdent,
}

/// Open a read-only WAL tailer for a stored table.
pub fn wal_tailer(table: &TableDb) -> buffer::Tailer {
    buffer::Tailer::with_object_store(WAL_MANIFEST_PATH.into(), table.engine_object_store())
}

/// Open a read-only WAL tailer from a table reader.
pub fn wal_tailer_from_reader(table: &TableReader) -> buffer::Tailer {
    buffer::Tailer::with_object_store(WAL_MANIFEST_PATH.into(), table.engine_object_store())
}

impl Ingestor {
    pub fn new(
        catalog_store: CatalogStore,
        clock: Arc<impl Clock + SchedulerClock + 'static>,
        scheduler: Arc<dyn TaskScheduler>,
    ) -> Self {
        Self {
            inner: Arc::new(IngestorState {
                catalog_store,
                scheduler_clock: clock.clone(),
                clock,
                scheduler,
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
            .ok_or_else(|| Error::CatalogNotFound(catalog.clone()))?;
        let table = match stored_catalog
            .try_load_table_reader(table_ident.clone())
            .await?
        {
            Some(table) => table,
            None => {
                match stored_catalog.link_table(table_ident.clone()).await {
                    Ok(table) => drop(table),
                    // Another ingestor may have linked this table while we loaded it.
                    Err(se_meta_store::catalog::Error::TableLinkAlreadyExists(_)) => {}
                    Err(se_meta_store::catalog::Error::Iceberg(source))
                        if source.kind() == IcebergErrorKind::TableNotFound =>
                    {
                        return Err(Error::TableNotFound(table_ident));
                    }
                    Err(source) => return Err(source.into()),
                }
                stored_catalog
                    .try_load_table_reader(table_ident.clone())
                    .await?
                    .ok_or_else(|| Error::TableNotFound(table_ident.clone()))?
            }
        };

        // Readers are not snapshot-pinned: schema definitions must remain stable by ID.
        let current_schema_id = table
            .get_current_schema_id()
            .await?
            .ok_or(Error::CurrentSchemaIdMissing)?;
        let schema = table
            .get_schema(current_schema_id)
            .await?
            .ok_or(Error::SchemaMissing(current_schema_id))?;

        let table_schema = Arc::new(schema_to_arrow_schema(&schema)?);

        let scheduler = self.inner.scheduler.clone();
        let clock = self.inner.scheduler_clock.clone();
        let flush_hook: buffer::FlushHook = Arc::new({
            let catalog = catalog.clone();
            let table_ident = table_ident.clone();
            move || {
                let scheduler = scheduler.clone();
                let task = Task::Reconcile {
                    catalog_name: catalog.clone(),
                    table_ident: table_ident.clone(),
                    deadline: clock.now(),
                };
                Box::pin(async move {
                    let Task::Reconcile {
                        catalog_name,
                        table_ident,
                        ..
                    } = &task;
                    if let Err(error) = scheduler.enqueue(task.clone()).await {
                        tracing::warn!(%catalog_name, %table_ident, %error, "failed to schedule reconciliation");
                    }
                })
            }
        });

        let producer = Producer::new_with_flush_hook(
            ProducerConfig {
                data_path_prefix: WAL_DATA_PATH.to_string(),
                manifest_path: WAL_MANIFEST_PATH.to_string(),
                flush_interval: Duration::from_secs(1),
                flush_size_bytes: 64 * 1024 * 1024,
                max_buffered_inputs: 1000,
                batch_compression: CompressionType::None,
            },
            table.engine_object_store(),
            table_schema.clone(),
            self.inner.clock.clone(),
            Some(flush_hook),
        )?;

        Ok(Arc::new(TableState {
            producer: RwLock::new(Some(producer)),
            current_schema_id,
            table_schema,
            catalog,
            table_ident,
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
            .field("current_schema_id", &self.state.current_schema_id)
            .field("table_schema", &self.state.table_schema)
            .finish()
    }
}

impl TableIngestor {
    /// Returns the schema ID captured when this table's producer was initialized.
    pub fn current_schema_id(&self) -> i32 {
        self.state.current_schema_id
    }

    /// Returns the Iceberg table schema used to validate stream fields and write batches.
    pub fn table_schema(&self) -> SchemaRef {
        self.state.table_schema.clone()
    }

    /// Returns an owned future that writes one record batch to the table.
    /// Buffer metadata contains the producer's schema ID as a four-byte little-endian i32.
    #[instrument(
        name = "table_ingestor.write",
        skip_all,
        fields(catalog, table, batch_size_bytes)
    )]
    pub fn write(
        &self,
        batch: RecordBatch,
    ) -> impl Future<Output = Result<(), WriteError>> + Send + 'static {
        let state = Arc::clone(&self.state);

        async move {
            Span::current()
                .record("catalog", tracing::field::display(&state.catalog))
                .record("table", tracing::field::display(&state.table_ident))
                .record("batch_size_bytes", batch.get_array_memory_size());

            if batch.num_rows() == 0 {
                return Err(WriteError::EmptyBatch);
            }

            if batch.schema() != state.table_schema {
                return Err(WriteError::InvalidSchema);
            }

            let mut handle = {
                let producer = state.producer.read().await;
                producer
                    .as_ref()
                    .ok_or(WriteError::Write)?
                    .produce(
                        batch,
                        BatchMetadata {
                            schema_id: state.current_schema_id,
                        }
                        .encode(),
                    )
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
