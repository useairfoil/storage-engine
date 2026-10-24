//! Materialization and publication of a table's ingested data.

use std::sync::Arc;

use buffer::Clock;
use iceberg::Catalog;
use iceberg_storage_opendal::OpenDalStorageFactory;
use se_commit::run_commit;
use se_materialization::{MaterializationOptions, run_materialization};
use se_meta_store::{
    catalog::{CatalogName, CatalogStore, NamespaceIdent, TableIdent},
    table::TableDb,
};
use tokio::sync::Mutex;

/// The error type for reconciliation operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("catalog not found: {0}")]
    CatalogNotFound(CatalogName),
    #[error("table not found: {0}")]
    TableNotFound(TableIdent),
    #[error("catalog store error: {0}")]
    CatalogStore(#[from] se_meta_store::catalog::Error),
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),
    #[error("table database error: {0}")]
    TableDb(#[from] se_meta_store::table::Error),
    /// Failed to create or run the table materializer.
    #[error("materialization error: {0}")]
    Materialization(#[from] se_materialization::Error),
    /// Failed to create or run the table committer.
    #[error("commit error: {0}")]
    Commit(#[from] se_commit::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Creates table-specific reconcilers.
#[derive(Debug, Clone)]
pub struct Reconciler {
    catalog_store: CatalogStore,
}

/// Materializes and then publishes an existing table's ingested data.
#[derive(Clone)]
pub struct TableReconciler {
    table: TableDb,
    table_ident: TableIdent,
    catalog: Arc<dyn Catalog>,
    reconciliation_lock: Arc<Mutex<()>>,
}

impl Reconciler {
    pub fn new(catalog_store: CatalogStore, _clock: Arc<dyn Clock>) -> Self {
        Self { catalog_store }
    }

    /// Creates a reconciler for a table already linked in the meta store.
    /// Metadata and storage credentials are refreshed at the start of each pass.
    pub async fn for_table(
        &self,
        catalog: CatalogName,
        namespace: NamespaceIdent,
        table_name: String,
    ) -> Result<TableReconciler> {
        let table_ident = TableIdent::new(namespace, table_name);

        let stored_catalog = self
            .catalog_store
            .get(catalog.clone())
            .await?
            .ok_or(Error::CatalogNotFound(catalog))?;

        let table = stored_catalog
            .try_load_table(table_ident.clone())
            .await?
            .ok_or_else(|| Error::TableNotFound(table_ident.clone()))?;

        let catalog = stored_catalog
            .to_catalog_with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            }))
            .await?;

        Ok(TableReconciler {
            table,
            table_ident,
            catalog,
            reconciliation_lock: Arc::new(Mutex::new(())),
        })
    }
}

impl TableReconciler {
    /// Returns the current table db.
    pub fn table(&self) -> &TableDb {
        &self.table
    }

    /// Consumes the reconciler, flushing pending table writes and closing its database.
    /// Closing the database also closes any other clones of the table handle.
    pub async fn flush_and_close_table_db(self) -> Result<()> {
        self.table.flush_and_close().await?;
        Ok(())
    }

    /// Refreshes metadata, then runs one materialization pass and one commit pass.
    ///
    /// Does not run the committer if materialization fails.
    pub async fn run_reconciliation(&self) -> Result<()> {
        let _guard = self.reconciliation_lock.lock().await;

        let iceberg_table = self.catalog.load_table(&self.table_ident).await?;

        self.table
            .refresh_metadata(iceberg_table.metadata())
            .await?;

        let file_io = iceberg_table.file_io();

        run_materialization(
            &self.table,
            &self.table_ident,
            file_io,
            MaterializationOptions,
        )
        .await?;

        run_commit(
            &self.table,
            &self.table_ident,
            self.catalog.as_ref(),
            file_io,
        )
        .await?;

        Ok(())
    }
}

impl std::fmt::Debug for TableReconciler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableReconciler")
            .field("table_ident", &self.table_ident)
            .finish_non_exhaustive()
    }
}
