use std::{collections::HashMap, sync::Arc};

pub use iceberg::{
    Catalog, Error as IcebergError, ErrorKind as IcebergErrorKind, NamespaceIdent, TableIdent,
};
use iceberg::{
    CatalogBuilder,
    io::{MemoryStorageFactory, StorageFactory},
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, path::Path};
use se_secret_store::{SecretName, SecretStore};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::table::{TableDb, TableReader};

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
        // Linking uses the catalog's table metadata, not its data file IO.
        self.to_catalog_with_storage_factory(name, Arc::new(MemoryStorageFactory))
            .await
    }

    /// Creates a catalog whose tables use the supplied storage factory for file IO.
    pub async fn to_catalog_with_storage_factory(
        &self,
        name: &str,
        storage_factory: Arc<dyn StorageFactory>,
    ) -> Result<Arc<dyn Catalog>> {
        match self {
            CatalogConfig::Rest(config) => {
                let mut props = config.properties.clone();
                props.insert(REST_CATALOG_PROP_URI.to_string(), config.uri.clone());
                if let Some(warehouse) = &config.warehouse {
                    props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
                }

                let catalog = RestCatalogBuilder::default()
                    .with_storage_factory(storage_factory)
                    .load(name, props)
                    .await?;
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

    /// Creates a catalog with the supplied storage factory and requests vended credentials.
    /// S3 file IO does not fall back to ambient AWS credentials.
    pub async fn to_catalog_with_storage_factory(
        &self,
        storage_factory: Arc<dyn StorageFactory>,
    ) -> Result<Arc<dyn Catalog>> {
        let mut config = self.config.clone();
        let CatalogConfig::Rest(rest) = &mut config;
        rest.properties.insert(
            "header.X-Iceberg-Access-Delegation".into(),
            "vended-credentials".into(),
        );
        rest.properties
            .insert("s3.disable-config-load".into(), "true".into());
        rest.properties
            .insert("s3.disable-ec2-metadata".into(), "true".into());
        config
            .to_catalog_with_storage_factory(&self.name.to_string(), storage_factory)
            .await
    }

    /// Links a table from the iceberg catalog into the meta store.
    pub async fn link_table(&self, table_ident: TableIdent) -> Result<TableDb> {
        let catalog = self.to_catalog().await?;
        let table = catalog.load_table(&table_ident).await?;
        let table_uuid = table.metadata().uuid();

        let path = table_metadata_path(table_uuid);
        let table_db = TableDb::load_or_init(path, table, self.object_store.clone()).await?;

        TableLink::create(self.object_store.clone(), table_ident, table_uuid).await?;

        Ok(table_db)
    }

    pub async fn try_load_table(&self, table_ident: TableIdent) -> Result<Option<TableDb>> {
        let Some(link) = TableLink::try_load(self.object_store.clone(), table_ident).await? else {
            return Ok(None);
        };

        let Some(table_db) =
            TableDb::try_load(table_metadata_path(link.uuid()), self.object_store.clone()).await?
        else {
            return Err(Error::TableMetadataMissing(link.uuid()));
        };
        Ok(Some(table_db))
    }

    /// Loads a linked table without acquiring writer ownership.
    pub async fn try_load_table_reader(
        &self,
        table_ident: TableIdent,
    ) -> Result<Option<TableReader>> {
        let Some(link) = TableLink::try_load(self.object_store.clone(), table_ident).await? else {
            return Ok(None);
        };
        let reader =
            TableReader::try_load(table_metadata_path(link.uuid()), self.object_store.clone())
                .await?
                .ok_or(Error::TableMetadataMissing(link.uuid()))?;

        Ok(Some(reader))
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

/// Returns the path of the metadata database for the table with the given
/// uuid, relative to the object store of the catalog.
fn table_metadata_path(table_uuid: Uuid) -> Path {
    // PANIC: the path is never empty.
    Path::from(format!("{table_uuid}/metadata"))
}
