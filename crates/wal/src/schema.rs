use std::sync::Arc;

use async_trait::async_trait;
use iceberg::{Catalog, Result, TableIdent, spec::TableMetadata as IcebergTableMetadata};
use se_metadata::CatalogId;

#[async_trait]
pub trait TableMetadata: Send + Sync {
    /// Loads the Iceberg metadata for a table in the given catalog.
    async fn load_table(
        &self,
        catalog: CatalogId,
        table: TableIdent,
    ) -> Result<IcebergTableMetadata>;
}

#[async_trait]
impl TableMetadata for Arc<dyn Catalog> {
    /// Loads table metadata from this catalog, ignoring the supplied catalog ID.
    async fn load_table(
        &self,
        _catalog: CatalogId,
        table: TableIdent,
    ) -> Result<IcebergTableMetadata> {
        Ok(self.as_ref().load_table(&table).await?.metadata().clone())
    }
}
