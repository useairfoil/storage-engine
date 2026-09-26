//! Storage Engine ingestion pipeline.

mod r#loop;

use std::{future::Future, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use iceberg::arrow::schema_to_arrow_schema;
use se_meta_store::{
    catalog::{CatalogName, CatalogStore, IcebergErrorKind, NamespaceIdent, TableIdent},
    table::StoredTable,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::r#loop::{Command, Lookup};

const CHANNEL_CAPACITY: usize = 32;

/// The error type for ingestion operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
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
    /// The ingestion loop is no longer running.
    #[error("ingestion loop closed")]
    LoopClosed,
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
    /// The record batch could not be flushed.
    #[error("failed to flush record batch")]
    Flush,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Creates table-specific ingestors.
#[derive(Debug, Clone)]
pub struct Ingestor {
    catalog_store: CatalogStore,
    sender: mpsc::Sender<Command>,
}

/// Ingests record batches into a single table.
///
/// Writes return owned futures so independent writes can run concurrently without
/// retaining a borrow of this handle.
#[derive(Clone)]
pub struct TableIngestor {
    state: Arc<TableState>,
}

struct TableState {
    key: (CatalogName, TableIdent),
    table: Arc<StoredTable>,
    schema: Arc<ArrowSchema>,
    sender: mpsc::Sender<Command>,
}

pub(crate) struct IngestMessage {
    batch: RecordBatch,
    reply: oneshot::Sender<std::result::Result<(), WriteError>>,
    // Keep the actor discoverable while work remains, even if the caller cancels.
    state: Arc<TableState>,
}

/// Starts the shared ingestion actor and returns its task and a handle for table ingestion.
pub fn start_ingestion_loop(catalog_store: CatalogStore) -> (JoinHandle<()>, Ingestor) {
    let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
    let task = tokio::spawn(r#loop::ingestion_loop(receiver));
    (
        task,
        Ingestor {
            catalog_store,
            sender,
        },
    )
}

impl Ingestor {
    /// Creates an ingestor for an existing table.
    pub async fn for_table(
        &self,
        catalog: CatalogName,
        namespace: NamespaceIdent,
        table_name: String,
    ) -> Result<TableIngestor> {
        let table_ident = TableIdent::new(namespace, table_name);
        let key = (catalog.clone(), table_ident.clone());
        let gate = match self.lookup(&key).await? {
            Lookup::Active(state) => return Ok(TableIngestor { state }),
            Lookup::Cold(gate) => gate,
        };
        let _guard = gate.lock().await;
        if let Lookup::Active(state) = self.lookup(&key).await? {
            return Ok(TableIngestor { state });
        }

        let stored_catalog = self
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
        let schema = Arc::new(schema_to_arrow_schema(table.metadata().schema.as_ref())?);
        let state = Arc::new(TableState {
            key,
            table: Arc::new(table),
            schema,
            sender: self.sender.clone(),
        });
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Command::Register { state, reply })
            .await
            .map_err(|_| Error::LoopClosed)?;
        Ok(TableIngestor {
            state: response.await.map_err(|_| Error::LoopClosed)?,
        })
    }

    async fn lookup(&self, key: &(CatalogName, TableIdent)) -> Result<Lookup> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Command::Lookup {
                key: key.clone(),
                reply,
            })
            .await
            .map_err(|_| Error::LoopClosed)?;
        response.await.map_err(|_| Error::LoopClosed)
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
            if batch.schema().fields().len() != state.schema.fields().len()
                || !state
                    .schema
                    .fields()
                    .iter()
                    .zip(batch.schema().fields())
                    .all(|(expected, actual)| expected.contains(actual))
            {
                return Err(WriteError::InvalidSchema);
            }

            let (reply, response) = oneshot::channel();
            state
                .sender
                .send(Command::Write(IngestMessage {
                    batch,
                    reply,
                    state: Arc::clone(&state),
                }))
                .await
                .map_err(|_| WriteError::Flush)?;
            response.await.unwrap_or(Err(WriteError::Flush))
        }
    }
}
