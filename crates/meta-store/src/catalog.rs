use std::{collections::HashMap, sync::Arc};

use iceberg::CatalogBuilder;
pub use iceberg::{Catalog, Error as IcebergError, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, path::Path};
use se_secret_store::{SecretName, SecretStore};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::table::{StoredTable, TableMetadata, TableStore};

se_common::resource_type!(Catalog, "catalogs");

/// Configuration of an Iceberg catalog.
///
/// Currently, only REST catalogs are supported.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogConfig {
    /// An Iceberg REST catalog.
    Rest(RestCatalogConfig),
}

/// Configuration of an Iceberg REST catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestCatalogConfig {
    /// URI of the REST catalog server.
    pub uri: String,
    /// Warehouse location, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
    /// Additional properties for the REST catalog server.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

/// A catalog, as stored in the meta store.
#[derive(Debug, Clone)]
pub struct StoredCatalog {
    object_store: Arc<dyn ObjectStore>,
    name: CatalogName,
    config: CatalogConfig,
}

/// The error type for catalog store operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Catalog already exists.
    #[error("catalog already exists: {0}")]
    AlreadyExists(CatalogName),
    /// Table link already exists.
    #[error("table link already exists: {0}")]
    TableLinkAlreadyExists(TableIdent),
    /// The metadata of a linked table is missing.
    #[error("table metadata is missing: {0}")]
    TableMetadataMissing(Uuid),
    /// The uuid of the stored table does not match the uuid of the catalog table.
    #[error("table uuid mismatch: catalog {catalog_uuid}, stored {stored_uuid}")]
    UuidMismatch {
        catalog_uuid: Uuid,
        stored_uuid: Uuid,
    },
    /// Failed to serialize a catalog configuration.
    #[error("failed to serialize catalog config: {source}")]
    Serialize {
        #[source]
        source: serde_json::Error,
    },
    /// Failed to deserialize a catalog configuration.
    #[error("failed to deserialize catalog config: {source}")]
    Deserialize {
        #[source]
        source: serde_json::Error,
    },
    /// Secret store error.
    #[error("secret store error: {0}")]
    SecretStore(#[from] se_secret_store::Error),
    /// Object store error.
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    /// Table store error.
    #[error("table store error: {0}")]
    TableStore(#[from] crate::table::Error),
    /// Failed to parse the uuid of a linked table.
    #[error("failed to parse table uuid: {source}")]
    ParseUuid {
        #[source]
        source: uuid::Error,
    },
    /// Failed to create the iceberg catalog.
    #[error("failed to create iceberg catalog: {0}")]
    Iceberg(#[from] iceberg::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A store for catalog configurations.
///
/// The store persists catalog configurations in a [`SecretStore`].
#[derive(Debug, Clone)]
pub struct CatalogStore {
    secret_store: Arc<dyn SecretStore>,
    object_store: Arc<dyn ObjectStore>,
}

/// A link from a table name in a catalog to the uuid of its metadata
/// in the meta store.
///
/// The link is stored as an object at `tables/<table-full-name>` in the
/// object store of the catalog, holding the uuid of the table.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TableLink {
    table_ident: TableIdent,
    table_uuid: Uuid,
}

impl CatalogConfig {
    /// Creates the iceberg [`Catalog`] described by this configuration.
    ///
    /// The `name` is used as the internal name of the created catalog.
    pub async fn to_catalog(&self, name: &str) -> Result<Arc<dyn Catalog>> {
        match self {
            CatalogConfig::Rest(config) => {
                let mut props = config.properties.clone();
                props.insert(REST_CATALOG_PROP_URI.to_string(), config.uri.clone());
                if let Some(warehouse) = &config.warehouse {
                    props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
                }

                let catalog = RestCatalogBuilder::default().load(name, props).await?;
                Ok(Arc::new(catalog))
            }
        }
    }
}

impl StoredCatalog {
    /// Creates a new stored catalog with the given name and configuration.
    pub fn new(
        parent_object_store: Arc<dyn ObjectStore>,
        name: CatalogName,
        config: CatalogConfig,
    ) -> Self {
        use object_store::prefix::PrefixStore;
        let object_store = Arc::new(PrefixStore::new(parent_object_store, name.to_string()));

        Self {
            object_store,
            name,
            config,
        }
    }

    /// Returns the name of the catalog.
    pub fn name(&self) -> &CatalogName {
        &self.name
    }

    /// Returns the configuration of the catalog.
    pub fn config(&self) -> &CatalogConfig {
        &self.config
    }

    /// Creates the iceberg [`Catalog`] described by this stored catalog.
    pub async fn to_catalog(&self) -> Result<Arc<dyn Catalog>> {
        self.config.to_catalog(&self.name.to_string()).await
    }

    /// Links a table from the iceberg catalog into the meta store.
    ///
    /// The table metadata is fetched from the catalog, stored in the object
    /// store of the catalog at `<table-uuid>/metadata.json`, and a link file
    /// is created for the table.
    pub async fn link_table(&self, table_ident: TableIdent) -> Result<StoredTable> {
        let catalog = self.to_catalog().await?;
        let table = catalog.load_table(&table_ident).await?;
        let table_uuid = table.metadata().uuid();

        let table_store = Arc::new(TableStore::new(
            self.object_store.clone(),
            table_metadata_path(table_uuid),
        ));
        let stored_table = match StoredTable::try_load(table_store.clone()).await? {
            Some(stored_table) => {
                let stored_uuid = stored_table.metadata().uuid;
                if stored_uuid != table_uuid {
                    return Err(Error::UuidMismatch {
                        catalog_uuid: table_uuid,
                        stored_uuid,
                    });
                }
                stored_table
            }
            None => StoredTable::init(table_store, TableMetadata::from(table.metadata())).await?,
        };

        TableLink::create(self.object_store.clone(), table_ident, table_uuid).await?;

        Ok(stored_table)
    }

    pub async fn try_load_table(&self, table_ident: TableIdent) -> Result<Option<StoredTable>> {
        let Some(link) = TableLink::try_load(self.object_store.clone(), table_ident).await? else {
            return Ok(None);
        };

        let table_store = Arc::new(TableStore::new(
            self.object_store.clone(),
            table_metadata_path(link.uuid()),
        ));
        let Some(stored_table) = StoredTable::try_load(table_store).await? else {
            return Err(Error::TableMetadataMissing(link.uuid()));
        };
        Ok(Some(stored_table))
    }

    /// Removes the link file for the given table.
    ///
    /// The stored table metadata is kept. Unlinking a table without a link
    /// file succeeds.
    pub async fn unlink_table(&self, table_ident: TableIdent) -> Result<()> {
        let path = table_link_path(&table_ident);
        self.object_store.delete(&path).await?;
        Ok(())
    }
}

impl CatalogStore {
    /// Creates a new catalog store on top of the given secret store.
    pub fn new(secret_store: Arc<dyn SecretStore>, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            secret_store,
            object_store,
        }
    }

    /// Returns the catalog with the given name, or `None` if it does not exist.
    pub async fn get(&self, name: CatalogName) -> Result<Option<StoredCatalog>> {
        let secret_name = secret_name(&name);
        match self.secret_store.get(&secret_name).await {
            Ok(secret) => {
                let config = serde_json::from_slice::<CatalogConfig>(secret.value().as_ref())
                    .map_err(|source| Error::Deserialize { source })?;
                Ok(Some(StoredCatalog::new(
                    self.object_store.clone(),
                    name,
                    config,
                )))
            }
            Err(se_secret_store::Error::NotFound(_)) => Ok(None),
            Err(source) => Err(Error::SecretStore(source)),
        }
    }

    /// Creates a catalog with the given name and configuration.
    ///
    /// Returns an error if a catalog with the given name already exists.
    pub async fn create(&self, name: CatalogName, config: CatalogConfig) -> Result<()> {
        let secret_name = secret_name(&name);
        if self.secret_store.get(&secret_name).await.is_ok() {
            return Err(Error::AlreadyExists(name));
        }

        let value = serde_json::to_vec(&config).map_err(|source| Error::Serialize { source })?;
        self.secret_store.put(&secret_name, value.into()).await?;
        Ok(())
    }

    /// Deletes the catalog with the given name.
    pub async fn delete(&self, name: CatalogName) -> Result<()> {
        self.secret_store.delete(&secret_name(&name)).await?;
        Ok(())
    }
}

impl TableLink {
    /// Creates a link file for the table with the given ident and uuid.
    ///
    /// Returns [`Error::TableLinkAlreadyExists`] if a link for the table
    /// already exists.
    async fn create(
        object_store: Arc<dyn ObjectStore>,
        table_ident: TableIdent,
        table_uuid: Uuid,
    ) -> Result<Self> {
        let path = table_link_path(&table_ident);
        object_store
            .put_opts(
                &path,
                PutPayload::from(table_uuid.to_string()),
                PutOptions::from(PutMode::Create),
            )
            .await
            .map_err(|source| match source {
                object_store::Error::AlreadyExists { .. } => {
                    Error::TableLinkAlreadyExists(table_ident.clone())
                }
                source => source.into(),
            })?;
        Ok(Self {
            table_ident,
            table_uuid,
        })
    }

    /// Loads the link for the given table ident. If there is no link file
    /// at the link path then this fn returns `None`.
    async fn try_load(
        object_store: Arc<dyn ObjectStore>,
        table_ident: TableIdent,
    ) -> Result<Option<Self>> {
        let path = table_link_path(&table_ident);
        let value = match object_store.get(&path).await {
            Ok(result) => result.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(source) => return Err(source.into()),
        };
        let content = String::from_utf8_lossy(value.as_ref());
        let table_uuid = Uuid::parse_str(&content).map_err(|source| Error::ParseUuid { source })?;
        Ok(Some(Self {
            table_ident,
            table_uuid,
        }))
    }

    /// Returns the ident of the linked table.
    #[allow(dead_code)]
    fn ident(&self) -> &TableIdent {
        &self.table_ident
    }

    /// Returns the uuid of the linked table.
    fn uuid(&self) -> Uuid {
        self.table_uuid
    }
}

/// Returns the name of the secret holding the configuration of the catalog
/// with the given name.
fn secret_name(name: &CatalogName) -> SecretName {
    // PANIC: the name is never empty.
    SecretName::new_unchecked(format!("catalog/{}", name.id()))
}

/// Returns the full name of a table, obtained by joining the namespace
/// components and the table name with `.`.
fn table_full_name(table_ident: &TableIdent) -> String {
    format!("{}.{}", table_ident.namespace.join("."), table_ident.name)
}

/// Returns the path of the link file for the table with the given ident,
/// relative to the object store of the catalog.
fn table_link_path(table_ident: &TableIdent) -> Path {
    // PANIC: the path is never empty.
    Path::from(format!("tables/{}", table_full_name(table_ident)))
}

/// Returns the path of the metadata object for the table with the given
/// uuid, relative to the object store of the catalog.
fn table_metadata_path(table_uuid: Uuid) -> Path {
    // PANIC: the path is never empty.
    Path::from(format!("{table_uuid}/metadata.json"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use se_secret_store::memory::InMemorySecretStore;

    use super::*;

    fn store() -> CatalogStore {
        use object_store::memory::InMemory;

        CatalogStore::new(
            Arc::new(InMemorySecretStore::new()),
            Arc::new(InMemory::new()),
        )
    }

    fn name() -> CatalogName {
        CatalogName::new("test-catalog").unwrap()
    }

    fn rest_config() -> CatalogConfig {
        CatalogConfig::Rest(RestCatalogConfig {
            uri: "https://rest.catalog.example.com".to_string(),
            warehouse: None,
            properties: HashMap::new(),
        })
    }

    fn table_ident() -> TableIdent {
        TableIdent::new(
            iceberg::NamespaceIdent::from_strs(["sales", "2024"]).unwrap(),
            "orders".to_string(),
        )
    }

    #[tokio::test]
    async fn table_link_create_and_try_load() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let table_ident = table_ident();
        let table_uuid = Uuid::from_u128(7);

        insta::assert_compact_debug_snapshot!(
            TableLink::try_load(object_store.clone(), table_ident.clone())
                .await
                .unwrap(),
            @"None"
        );

        TableLink::create(object_store.clone(), table_ident.clone(), table_uuid)
            .await
            .unwrap();
        let link = TableLink::try_load(object_store.clone(), table_ident.clone())
            .await
            .unwrap()
            .unwrap();
        insta::assert_compact_debug_snapshot!(link.ident(), @r#"TableIdent { namespace: NamespaceIdent(["sales", "2024"]), name: "orders" }"#);
        insta::assert_snapshot!(link.uuid(), @"00000000-0000-0000-0000-000000000007");

        insta::assert_snapshot!(
            TableLink::create(object_store, table_ident, table_uuid)
                .await
                .unwrap_err()
                .to_string(),
            @"table link already exists: sales.2024.orders"
        );
    }

    #[tokio::test]
    async fn table_link_layout() {
        let parent: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let catalog_store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
            parent.clone(),
            "cat-id",
        ));

        TableLink::create(catalog_store, table_ident(), Uuid::from_u128(7))
            .await
            .unwrap();

        let objects = parent
            .list_with_delimiter(Some(&Path::from("cat-id/tables")))
            .await
            .unwrap()
            .objects;
        let locations: Vec<String> = objects
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect();
        insta::assert_compact_debug_snapshot!(
            locations,
            @r#"["cat-id/tables/sales.2024.orders"]"#
        );
    }

    #[test]
    fn config_serialization() {
        let config = CatalogConfig::Rest(RestCatalogConfig {
            uri: "https://rest.catalog.example.com".to_string(),
            warehouse: Some("s3://warehouse".to_string()),
            properties: HashMap::from([("token".to_string(), "hunter2".to_string())]),
        });

        let json = serde_json::to_string(&config).unwrap();
        insta::assert_snapshot!(
            json,
            @r#"{"rest":{"uri":"https://rest.catalog.example.com","warehouse":"s3://warehouse","properties":{"token":"hunter2"}}}"#
        );

        let deserialized: CatalogConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, config);
    }

    #[tokio::test]
    async fn catalog_lifecycle() {
        let store = store();
        let name = name();

        assert!(store.get(name.clone()).await.unwrap().is_none());

        store.create(name.clone(), rest_config()).await.unwrap();
        let catalog = store.get(name.clone()).await.unwrap().unwrap();
        assert_eq!(catalog.name(), &name);
        assert_eq!(catalog.config(), &rest_config());

        insta::assert_compact_debug_snapshot!(
            store.create(name.clone(), rest_config()).await.unwrap_err(),
            @"AlreadyExists(CatalogName { id: \"test-catalog\" })"
        );

        store.delete(name.clone()).await.unwrap();
        assert!(store.get(name.clone()).await.unwrap().is_none());

        store.delete(name).await.unwrap();
    }

    #[tokio::test]
    async fn unlink_table() {
        let store = store();
        let catalog = StoredCatalog::new(store.object_store.clone(), name(), rest_config());
        let table_ident = table_ident();
        let table_uuid = Uuid::from_u128(7);

        catalog.unlink_table(table_ident.clone()).await.unwrap();
        insta::assert_compact_debug_snapshot!(
            TableLink::try_load(catalog.object_store.clone(), table_ident.clone())
                .await
                .unwrap(),
            @"None"
        );

        TableLink::create(
            catalog.object_store.clone(),
            table_ident.clone(),
            table_uuid,
        )
        .await
        .unwrap();
        catalog.unlink_table(table_ident.clone()).await.unwrap();
        insta::assert_compact_debug_snapshot!(
            TableLink::try_load(catalog.object_store, table_ident).await.unwrap(),
            @"None"
        );
    }

    #[tokio::test]
    async fn try_load_table() {
        use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Type};

        let store = store();
        let catalog = StoredCatalog::new(store.object_store.clone(), name(), rest_config());
        let table_ident = table_ident();
        let table_uuid = Uuid::from_u128(7);

        insta::assert_compact_debug_snapshot!(
            catalog
                .try_load_table(table_ident.clone())
                .await
                .unwrap()
                .map(|table| table.metadata().clone()),
            @"None"
        );

        TableLink::create(
            catalog.object_store.clone(),
            table_ident.clone(),
            table_uuid,
        )
        .await
        .unwrap();
        insta::assert_snapshot!(
            catalog
                .try_load_table(table_ident.clone())
                .await
                .map(|table| table.map(|table| table.metadata().uuid))
                .map_err(|error| error.to_string())
                .unwrap_err(),
            @"table metadata is missing: 00000000-0000-0000-0000-000000000007"
        );

        let table_store = Arc::new(TableStore::new(
            catalog.object_store.clone(),
            table_metadata_path(table_uuid),
        ));
        let metadata = TableMetadata {
            uuid: table_uuid,
            schema: Arc::new(
                Schema::builder()
                    .with_schema_id(1)
                    .with_fields(vec![
                        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    ])
                    .build()
                    .unwrap(),
            ),
            schema_id: 0,
            partition_spec: Arc::new(PartitionSpec::unpartition_spec()),
            partition_spec_id: 0,
            sort_order: Arc::new(SortOrder::unsorted_order()),
            sort_order_id: 0,
        };
        StoredTable::init(table_store, metadata).await.unwrap();

        let table = catalog.try_load_table(table_ident).await.unwrap().unwrap();
        assert_eq!(table.metadata().uuid, table_uuid);
    }

    #[tokio::test]
    async fn to_catalog() {
        let store = store();
        let name = name();

        let config = CatalogConfig::Rest(RestCatalogConfig {
            uri: "https://rest.catalog.example.com".to_string(),
            warehouse: Some("s3://warehouse".to_string()),
            properties: HashMap::from([("token".to_string(), "hunter2".to_string())]),
        });
        store.create(name.clone(), config).await.unwrap();
        let catalog = store.get(name.clone()).await.unwrap().unwrap();

        let iceberg_catalog = catalog.to_catalog().await.unwrap();
        insta::assert_debug_snapshot!(iceberg_catalog, @r#"
        RestCatalog {
            user_config: RestCatalogConfig {
                name: Some(
                    "catalogs/test-catalog",
                ),
                uri: "https://rest.catalog.example.com",
                warehouse: Some(
                    "s3://warehouse",
                ),
                props: {
                    "token": "hunter2",
                },
                client: None,
            },
            ctx: OnceCell {
                value: None,
            },
            storage_factory: None,
            runtime: Runtime,
        }
        "#);

        let config = CatalogConfig::Rest(RestCatalogConfig {
            uri: String::new(),
            warehouse: None,
            properties: HashMap::new(),
        });
        let catalog = StoredCatalog::new(store.object_store.clone(), name, config);

        insta::assert_snapshot!(
            catalog.to_catalog().await.unwrap_err().to_string(),
            @"failed to create iceberg catalog: DataInvalid => Catalog uri is required"
        );
    }
}
