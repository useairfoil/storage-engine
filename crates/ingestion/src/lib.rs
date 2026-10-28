//! Storage Engine ingestion pipeline.

use std::{collections::HashMap, future::Future, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use iceberg::{ErrorKind, NamespaceIdent, TableIdent, arrow::schema_to_arrow_schema};
use object_store::ObjectStore;
use se_common::{clock::SystemClock, rand::SeRand};
use se_metadata::CatalogId;
pub use se_wal::Error as WalError;
use se_wal::{Producer, ProducerOptions, WalClientFactory, schema::TableMetadata};
use tokio::sync::{OnceCell, RwLock};
use tracing::{Span, debug, instrument};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("ingestor is closed")]
    Closed,

    #[error("table not found: {0}")]
    TableNotFound(TableIdent),

    #[error("table metadata error: {0}")]
    TableMetadata(#[from] iceberg::Error),

    #[error("WAL error: {0}")]
    Wal(#[from] se_wal::Error),
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WriteError {
    #[error("invalid record batch schema")]
    InvalidSchema,

    #[error("record batch is empty")]
    EmptyBatch,

    #[error("failed to write record batch")]
    Write,

    #[error("failed to flush record batch")]
    Flush,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct Ingestor {
    inner: Arc<IngestorState>,
}

#[derive(Clone)]
pub struct TableIngestor {
    state: Arc<TableState>,
}

type TableSlot = OnceCell<Arc<TableState>>;
type TableSlots = HashMap<(CatalogId, TableIdent), Arc<TableSlot>>;

struct IngestorState {
    table_metadata: Arc<dyn TableMetadata>,
    wal_client_factory: Arc<dyn WalClientFactory>,
    object_store: Arc<dyn ObjectStore>,
    clock: Arc<dyn SystemClock>,
    rng: Arc<SeRand>,
    tables: RwLock<Option<TableSlots>>,
}

struct TableState {
    producer: RwLock<Option<Producer>>,
    current_schema_id: i32,
    table_schema: SchemaRef,
    catalog: CatalogId,
    table_ident: TableIdent,
}

impl Ingestor {
    /// Creates an ingestor using table metadata, table-scoped WAL clients, and shared object storage.
    pub fn new(
        table_metadata: Arc<dyn TableMetadata>,
        wal_client_factory: Arc<dyn WalClientFactory>,
        object_store: Arc<dyn ObjectStore>,
        clock: Arc<dyn SystemClock>,
        rng: Arc<SeRand>,
    ) -> Self {
        Self {
            inner: Arc::new(IngestorState {
                table_metadata,
                wal_client_factory,
                object_store,
                clock,
                rng,
                tables: RwLock::new(Some(HashMap::new())),
            }),
        }
    }

    /// Creates or reuses an ingestor for an existing table with a fixed schema.
    #[instrument(skip_all, fields(catalog = %catalog, namespace = %namespace, table = %table_name), err)]
    pub async fn for_table(
        &self,
        catalog: CatalogId,
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

    /// Stops accepting tables and writes, then flushes and closes every initialized producer.
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

        first_error.map_or(Ok(()), |error| Err(Error::Wal(error)))
    }

    async fn init_table(
        &self,
        catalog: CatalogId,
        table_ident: TableIdent,
    ) -> Result<Arc<TableState>> {
        let metadata = self
            .inner
            .table_metadata
            .load_table(catalog.clone(), table_ident.clone())
            .await
            .map_err(|error| {
                if error.kind() == ErrorKind::TableNotFound {
                    Error::TableNotFound(table_ident.clone())
                } else {
                    Error::TableMetadata(error)
                }
            })?;

        let current_schema_id = metadata.current_schema_id();
        let table_schema = Arc::new(schema_to_arrow_schema(metadata.current_schema())?);
        let table_uuid = metadata.uuid();

        debug!(table_uuid = %table_uuid, "creating table producer");

        let producer = Producer::new(
            ProducerOptions {
                data_path_prefix: format!("/catalogs/{catalog}/tables/{table_uuid}/wal"),
                ..ProducerOptions::default()
            },
            self.inner.object_store.clone(),
            self.inner
                .wal_client_factory
                .wal_client(catalog.clone(), table_uuid),
            table_schema.clone(),
            current_schema_id,
            self.inner.clock.clone(),
            self.inner.rng.clone(),
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

    /// Returns an owned future that completes once the batch is uploaded and committed to the WAL.
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
                    .produce(batch)
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
