use std::{collections::HashSet, sync::Arc};

use iceberg::{
    Catalog, ErrorKind, NamespaceIdent, TableCreation, TableIdent,
    spec::{Schema, SortOrder},
    table::Table,
};
use se_iceberg::storage::ObjectStoreStorageFactory;
use se_metadata_client::{CatalogId, MetadataClient};
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to fetch catalog configuration: {0}")]
    Metadata(#[from] se_metadata_client::Error),
    #[error("invalid catalog ID")]
    InvalidCatalogId,
    #[error("catalog '{0}' not found")]
    CatalogNotFound(String),
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),
    #[error("table '{0}' has a different schema")]
    SchemaMismatch(String),
    #[error("table '{0}' has a different sort order")]
    SortOrderMismatch(String),
}

/// Loads catalog credentials from metadata and connects using the shared storage factory.
pub async fn load(
    metadata: Arc<dyn MetadataClient>,
    catalog_id: &str,
) -> Result<Arc<dyn Catalog>, Error> {
    debug!(catalog.id = catalog_id, "loading catalog configuration");
    let id = CatalogId::new(catalog_id).map_err(|_| Error::InvalidCatalogId)?;
    let config = metadata
        .get_catalog(&id)
        .await?
        .ok_or_else(|| Error::CatalogNotFound(catalog_id.to_owned()))?;

    let catalog = config
        .to_catalog(&id, Arc::new(ObjectStoreStorageFactory))
        .await?;
    debug!(catalog.id = catalog_id, "catalog initialized");
    Ok(catalog)
}

/// Creates or loads a table and validates its schema and sort order.
pub async fn ensure_table(
    catalog: &dyn Catalog,
    namespace: &NamespaceIdent,
    name: &str,
    schema: &Schema,
    sort_order: &SortOrder,
) -> Result<Table, Error> {
    let ident = TableIdent::new(namespace.clone(), name.to_owned());
    debug!(table.ident = %ident, "looking up table");
    let table = if catalog.table_exists(&ident).await? {
        debug!(table.ident = %ident, "loading existing table");
        catalog.load_table(&ident).await?
    } else {
        debug!(table.ident = %ident, "creating table");
        let creation = TableCreation::builder()
            .name(name.to_owned())
            .schema(schema.clone())
            .sort_order(sort_order.clone())
            .properties([("format-version".to_owned(), "3".to_owned())])
            .build();

        match catalog.create_table(namespace, creation).await {
            Ok(table) => {
                debug!(table.ident = %ident, "table created");
                table
            }
            Err(error) if error.kind() == ErrorKind::TableAlreadyExists => {
                debug!(table.ident = %ident, "table already created; loading it");
                catalog.load_table(&ident).await?
            }
            Err(error) => return Err(error.into()),
        }
    };

    let actual = table.current_schema_ref();
    if actual.as_struct() != schema.as_struct()
        || actual.identifier_field_ids().collect::<HashSet<_>>()
            != schema.identifier_field_ids().collect()
    {
        return Err(Error::SchemaMismatch(ident.to_string()));
    }

    if table.metadata().default_sort_order().fields != sort_order.fields {
        return Err(Error::SortOrderMismatch(ident.to_string()));
    }

    debug!(table.ident = %ident, "table schema validated");
    Ok(table)
}
