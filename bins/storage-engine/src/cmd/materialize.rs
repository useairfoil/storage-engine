use std::sync::Arc;

use clap::Args;
use se_materialization::{Clock, MaterializationOptions, Materializer};
use se_meta_store::catalog::{CatalogName, CatalogStore, TableIdent};

use crate::{object_store::ObjectStoreArgs, secret_store::SecretStoreArgs};

/// Start materialization of a table's ingestion WAL.
#[derive(Debug, Args)]
pub struct MaterializeArgs {
    #[command(flatten)]
    object_store: ObjectStoreArgs,
    #[command(flatten)]
    secret_store: SecretStoreArgs,
    /// ID of the catalog containing the table.
    #[arg(long)]
    catalog_id: String,
    /// Fully qualified table name, e.g. analytics.events.page_views.
    #[arg(long = "table", value_parser = parse_table_name)]
    table: TableIdent,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid catalog id: {0}")]
    InvalidCatalogId(String),
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("secret store error: {0}")]
    SecretStore(#[from] se_secret_store::Error),
    #[error("materialization error: {0}")]
    Materialization(#[from] se_materialization::Error),
}

impl MaterializeArgs {
    pub async fn run(self, clock: Arc<dyn Clock>) -> Result<(), Error> {
        let catalog = CatalogName::new(&self.catalog_id)
            .map_err(|_| Error::InvalidCatalogId(self.catalog_id.clone()))?;

        let object_store = self.object_store.create_object_store()?;
        let secret_store = self
            .secret_store
            .create_secret_store(object_store.clone())
            .await?;

        let materializer = Materializer::new(CatalogStore::new(secret_store, object_store), clock);

        let table_materializer = materializer
            .for_table(catalog, self.table.namespace, self.table.name)
            .await?;

        table_materializer
            .run_materialization(MaterializationOptions)
            .await?;

        Ok(())
    }
}

fn parse_table_name(value: &str) -> Result<TableIdent, String> {
    if value.split('.').any(str::is_empty) {
        return Err("table must be a dot-separated namespace and table name".into());
    }
    TableIdent::from_strs(value.split('.'))
        .map_err(|_| "table must be a dot-separated namespace and table name".into())
}
