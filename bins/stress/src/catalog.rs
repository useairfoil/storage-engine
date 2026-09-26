use std::{collections::HashSet, sync::Arc};

use iceberg::{
    CatalogBuilder, ErrorKind, TableCreation, io::MemoryStorageFactory, spec::Schema, table::Table,
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use se_meta_store::catalog::{Catalog, CatalogConfig, CatalogName, NamespaceIdent, TableIdent};
use serde::Deserialize;
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to fetch catalog configuration: {0}")]
    Http(#[from] reqwest::Error),
    #[error("iceberg error: {0}")]
    Iceberg(#[from] se_meta_store::catalog::IcebergError),
    #[error("namespace '{0}' does not exist")]
    NamespaceNotFound(String),
    #[error("table '{0}' has a different schema")]
    SchemaMismatch(String),
}

#[derive(Deserialize)]
struct CatalogResponse {
    name: CatalogName,
    #[serde(flatten)]
    config: CatalogConfig,
}

pub async fn load(engine_url: &str, catalog_id: &str) -> Result<Arc<dyn Catalog>, Error> {
    debug!(catalog_id, "loading catalog configuration");
    let url = format!("{}/catalogs/{catalog_id}", engine_url.trim_end_matches('/'));
    let response: CatalogResponse = reqwest::Client::new()
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let CatalogConfig::Rest(config) = response.config;
    let mut props = config.properties;
    props.insert(REST_CATALOG_PROP_URI.to_owned(), config.uri);
    if let Some(warehouse) = config.warehouse {
        props.insert(REST_CATALOG_PROP_WAREHOUSE.to_owned(), warehouse);
    }

    // REST returns metadata inline, but Iceberg still requires a FileIO on the table object.
    // The stress tool never reads or writes table files.
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(MemoryStorageFactory))
        .load(&response.name.to_string(), props)
        .await?;
    debug!(catalog_id, "catalog initialized");
    Ok(Arc::new(catalog))
}

pub async fn ensure_table(
    catalog: &dyn Catalog,
    namespace: &NamespaceIdent,
    name: &str,
    schema: &Schema,
) -> Result<Table, Error> {
    let ident = TableIdent::new(namespace.clone(), name.to_owned());
    debug!(table = %ident, "looking up table");
    let table = if catalog.table_exists(&ident).await? {
        debug!(table = %ident, "loading existing table");
        catalog.load_table(&ident).await?
    } else {
        debug!(table = %ident, "creating table");
        let creation = TableCreation::builder()
            .name(name.to_owned())
            .schema(schema.clone())
            .build();

        match catalog.create_table(namespace, creation).await {
            Ok(table) => {
                debug!(table = %ident, "table created");
                table
            }
            Err(error) if error.kind() == ErrorKind::TableAlreadyExists => {
                debug!(table = %ident, "table already created; loading it");
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

    debug!(table = %ident, "table schema validated");
    Ok(table)
}
