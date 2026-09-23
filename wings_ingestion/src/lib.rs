//! Wings ingestion pipeline.

use std::{future::Future, sync::Arc};

use arrow_array::RecordBatch;
use wings_meta_store::catalog::{CatalogName, CatalogStore, NamespaceIdent, TableIdent};

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
}

/// Ingests record batches into a single table.
///
/// Writes return owned futures so independent writes can run concurrently without
/// retaining a borrow of this handle.
#[derive(Debug, Clone)]
pub struct TableIngestor {
    _private: Arc<()>,
}

impl Ingestor {
    /// Creates an ingestor backed by the supplied catalog store.
    pub fn new(catalog_store: CatalogStore) -> Self {
        Self { catalog_store }
    }

    /// Creates an ingestor for an existing table.
    pub async fn for_table(
        &self,
        catalog: CatalogName,
        namespace: NamespaceIdent,
        table_name: String,
    ) -> Result<TableIngestor> {
        let _ = (&self.catalog_store, catalog, namespace, table_name);
        todo!()
    }
}

impl TableIngestor {
    /// Returns an owned future that writes one record batch to the table.
    pub fn write(
        &self,
        batch: RecordBatch,
    ) -> impl Future<Output = Result<(), WriteError>> + Send + 'static {
        let state = Arc::clone(&self._private);

        async move {
            let _ = (state, batch);
            todo!()
        }
    }
}
