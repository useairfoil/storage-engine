use std::{collections::HashMap, fmt, sync::Arc};

use iceberg::{Catalog, CatalogBuilder, io::StorageFactory};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use se_common::id::IdGenerator;
use se_secret_store::SecretName;
use serde::{Deserialize, Serialize};
use slatedb::IsolationLevel;

use crate::{
    Error, Metadata, Result,
    encoding::{CatalogKey, CatalogValue},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CatalogId(String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogConfig {
    Rest(RestCatalogConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestCatalogConfig {
    pub uri: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

impl CatalogId {
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();

        if id.is_empty() || id.len() > 32 {
            return Err(Error::InvalidCatalogId(id));
        }

        Ok(Self(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CatalogId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<String> for CatalogId {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl From<CatalogId> for String {
    fn from(value: CatalogId) -> Self {
        value.0
    }
}

impl CatalogConfig {
    /// Creates a catalog using the supplied storage factory and catalog-vended credentials.
    #[tracing::instrument(name = "metadata.to_catalog", skip_all, fields(catalog_id = %id))]
    pub async fn to_catalog(
        &self,
        id: &CatalogId,
        storage_factory: Arc<dyn StorageFactory>,
    ) -> iceberg::Result<Arc<dyn Catalog>> {
        let Self::Rest(config) = self;
        let mut properties = config.properties.clone();
        properties.insert(REST_CATALOG_PROP_URI.into(), config.uri.clone());
        if let Some(warehouse) = &config.warehouse {
            properties.insert(REST_CATALOG_PROP_WAREHOUSE.into(), warehouse.clone());
        }

        properties.insert(
            "header.X-Iceberg-Access-Delegation".into(),
            "vended-credentials".into(),
        );
        properties.insert("s3.disable-config-load".into(), "true".into());
        properties.insert("s3.disable-ec2-metadata".into(), "true".into());

        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(storage_factory)
            .load(id.as_str(), properties)
            .await?;

        Ok(Arc::new(catalog))
    }
}

impl Metadata {
    /// Reads the catalog configuration, returning `None` if no mapping exists.
    #[tracing::instrument(name = "metadata.get_catalog", skip_all, fields(catalog_id = %id))]
    pub async fn get_catalog(&self, id: &CatalogId) -> Result<Option<CatalogConfig>> {
        let Some(value) = self.db.get(CatalogKey { id }.encode()).await? else {
            return Ok(None);
        };

        let CatalogValue { secret_id } = CatalogValue::decode(&value)?;
        let name = SecretName::new(secret_id.to_string())?;
        let secret = self.secret_store.get(&name).await?;

        Ok(Some(serde_json::from_str(secret.value())?))
    }

    /// Creates a catalog without replacing an existing catalog.
    #[tracing::instrument(name = "metadata.create_catalog", skip_all, fields(catalog_id = %id))]
    pub async fn create_catalog(&self, id: CatalogId, config: CatalogConfig) -> Result<()> {
        let key = CatalogKey { id: &id }.encode();
        let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        if tx.get(&key).await?.is_some() {
            return Err(Error::CatalogAlreadyExists(id));
        }

        let secret_id = rand::rng().gen_ulid(self.clock.as_ref());
        let name = SecretName::new(secret_id.to_string())?;
        let value = serde_json::to_string(&config)?;
        self.secret_store.put(&name, value).await?;

        let result: Result<()> = async {
            tx.put(key, CatalogValue { secret_id }.encode())?;

            if let Some(handle) = tx.commit().await? {
                handle.await_durable().await?;
            }

            Ok(())
        }
        .await;

        if result.is_err() {
            let _ = self.secret_store.delete(&name).await;
        }

        result
    }

    /// Replaces an existing catalog's configuration, or returns `CatalogNotFound`.
    #[tracing::instrument(name = "metadata.update_catalog", skip_all, fields(catalog_id = %id))]
    pub async fn update_catalog(&self, id: &CatalogId, config: CatalogConfig) -> Result<()> {
        let key = CatalogKey { id }.encode();
        let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        let Some(value) = tx.get(&key).await? else {
            return Err(Error::CatalogNotFound(id.clone()));
        };

        let old = CatalogValue::decode(&value)?;
        let old_name = SecretName::new(old.secret_id.to_string())?;

        let secret_id = rand::rng().gen_ulid(self.clock.as_ref());
        let name = SecretName::new(secret_id.to_string())?;
        self.secret_store
            .put(&name, serde_json::to_string(&config)?)
            .await?;

        let result: Result<()> = async {
            tx.put(key, CatalogValue { secret_id }.encode())?;

            if let Some(handle) = tx.commit().await? {
                handle.await_durable().await?;
            }

            Ok(())
        }
        .await;

        if let Err(error) = result {
            let _ = self.secret_store.delete(&name).await;

            return Err(error);
        }

        self.secret_store.delete(&old_name).await?;

        Ok(())
    }

    /// Removes the catalog mapping and its secret, doing nothing if the catalog is absent.
    #[tracing::instrument(name = "metadata.delete_catalog", skip_all, fields(catalog_id = %id))]
    pub async fn delete_catalog(&self, id: &CatalogId) -> Result<()> {
        let key = CatalogKey { id }.encode();
        let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        let Some(value) = tx.get(&key).await? else {
            return Ok(());
        };

        let CatalogValue { secret_id } = CatalogValue::decode(&value)?;
        let name = SecretName::new(secret_id.to_string())?;

        tx.delete(key)?;

        if let Some(handle) = tx.commit().await? {
            handle.await_durable().await?;
        }

        self.secret_store.delete(&name).await?;

        Ok(())
    }
}
