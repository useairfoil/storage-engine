use std::{collections::HashMap, fmt, sync::Arc};

use iceberg::{Catalog, CatalogBuilder, io::StorageFactory};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use se_common::id::IdGenerator;
use se_iceberg::storage::ObjectStoreStorageFactory;
use se_secret_store::SecretName;
use serde::{Deserialize, Serialize};
use slatedb::{ErrorKind, IsolationLevel};
use tracing::{info, instrument};
use uuid::Uuid;

use crate::{
    Error, Metadata, Result,
    encoding::{CatalogKey, CatalogValue, decode, encode},
};

const RESERVED_REST_PROPERTIES: [&str; 2] = [REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE];

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CatalogId(Uuid);

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogConfigUpdate {
    Rest(RestCatalogUpdate),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestCatalogUpdate {
    pub properties: HashMap<String, Option<String>>,
}

impl CatalogId {
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        Uuid::parse_str(&id)
            .map(Self)
            .map_err(|_| Error::InvalidCatalogId(id))
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl fmt::Display for CatalogId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Uuid> for CatalogId {
    fn from(value: Uuid) -> Self {
        Self(value)
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
        value.to_string()
    }
}

impl CatalogConfig {
    /// Creates a catalog using the supplied storage factory and catalog-vended credentials.
    #[instrument(name = "metadata.to_catalog", skip_all, fields(catalog.id = %id))]
    pub async fn to_catalog(
        &self,
        id: &CatalogId,
        storage_factory: Arc<dyn StorageFactory>,
    ) -> iceberg::Result<Arc<dyn Catalog>> {
        let catalog = self.to_rest_catalog(id, storage_factory).await?;

        Ok(Arc::new(catalog))
    }

    async fn to_rest_catalog(
        &self,
        id: &CatalogId,
        storage_factory: Arc<dyn StorageFactory>,
    ) -> iceberg::Result<RestCatalog> {
        info!(catalog.id = %id, "creating catalog client");

        let Self::Rest(config) = self;
        let mut properties = config.properties.clone();
        properties.insert(REST_CATALOG_PROP_URI.into(), config.uri.clone());

        if let Some(warehouse) = &config.warehouse {
            properties.insert(REST_CATALOG_PROP_WAREHOUSE.into(), warehouse.clone());
        } else {
            properties.remove(REST_CATALOG_PROP_WAREHOUSE);
        }

        properties.insert(
            "header.X-Iceberg-Access-Delegation".into(),
            "vended-credentials".into(),
        );
        properties.insert("s3.disable-config-load".into(), "true".into());
        properties.insert("s3.disable-ec2-metadata".into(), "true".into());

        RestCatalogBuilder::default()
            .with_storage_factory(storage_factory)
            .load(&id.to_string(), properties)
            .await
    }

    fn validate(&self) -> Result<()> {
        let Self::Rest(config) = self;

        validate_rest_properties(&config.properties)
    }

    fn apply_update(&self, update: CatalogConfigUpdate) -> Result<Self> {
        let (Self::Rest(current), CatalogConfigUpdate::Rest(update)) = (self, update);

        validate_rest_properties(&update.properties)?;

        let mut properties = current.properties.clone();

        for (key, value) in update.properties {
            match value {
                Some(value) => {
                    properties.insert(key, value);
                }
                None => {
                    properties.remove(&key);
                }
            }
        }

        Ok(Self::Rest(RestCatalogConfig {
            uri: current.uri.clone(),
            warehouse: current.warehouse.clone(),
            properties,
        }))
    }

    async fn probe(&self, id: &CatalogId) -> Result<()> {
        // RestCatalogBuilder::load is lazy; invalidate_token forces context initialization
        // (OAuth2 token exchange if configured and GET /v1/config) on this ephemeral client.
        let catalog = self
            .to_rest_catalog(id, Arc::new(ObjectStoreStorageFactory))
            .await
            .map_err(Error::CatalogProbeFailed)?;

        catalog
            .invalidate_token()
            .await
            .map_err(Error::CatalogProbeFailed)?;

        Ok(())
    }
}

impl CatalogConfigUpdate {
    fn validate(&self) -> Result<()> {
        let Self::Rest(update) = self;

        validate_rest_properties(&update.properties)
    }
}

impl Metadata {
    /// Reads the catalog configuration, returning `None` if no mapping exists.
    #[instrument(name = "metadata.get_catalog", skip_all, fields(catalog.id = %id))]
    pub async fn get_catalog(&self, id: &CatalogId) -> Result<Option<CatalogConfig>> {
        info!(catalog.id = %id, "getting catalog");

        let Some(value) = self.db.get(CatalogKey { id }.encode()).await? else {
            return Ok(None);
        };

        let catalog: CatalogValue = decode(&value)?;

        Ok(Some(self.read_catalog_secret(&catalog).await?))
    }

    /// Creates a catalog after validating and probing its configuration.
    #[instrument(name = "metadata.create_catalog", skip_all, fields(catalog.id = %id))]
    pub async fn create_catalog(&self, id: CatalogId, config: CatalogConfig) -> Result<()> {
        info!(catalog.id = %id, "creating catalog");

        config.validate()?;

        let key = CatalogKey { id: &id }.encode();

        if self.db.get(&key).await?.is_some() {
            return Err(Error::CatalogAlreadyExists(id));
        }

        config.probe(&id).await?;

        let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        if tx.get(&key).await?.is_some() {
            return Err(Error::CatalogAlreadyExists(id));
        }

        let secret_id = rand::rng().gen_ulid(self.clock.as_ref());
        let name = SecretName::new(secret_id.to_string())?;
        let value = serde_json::to_string(&config)?;
        self.secret_store.put(&name, value).await?;

        let result: Result<()> = async {
            tx.put(key, encode(&CatalogValue { secret_id })?)?;

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

    /// Updates an existing catalog's mutable configuration after validating and probing it.
    #[instrument(name = "metadata.update_catalog", skip_all, fields(catalog.id = %id))]
    pub async fn update_catalog(&self, id: &CatalogId, update: CatalogConfigUpdate) -> Result<()> {
        info!(catalog.id = %id, "updating catalog");

        update.validate()?;

        let key = CatalogKey { id }.encode();
        let mut probed: Option<CatalogConfig> = None;

        loop {
            let Some(value) = self.db.get(&key).await? else {
                return Err(Error::CatalogNotFound(id.clone()));
            };

            let initial: CatalogValue = decode(&value)?;
            let current = match self.read_catalog_secret(&initial).await {
                Ok(config) => config,
                Err(Error::SecretStore(se_secret_store::Error::NotFound(_)))
                    if self
                        .db
                        .get(&key)
                        .await?
                        .map(|bytes| decode::<CatalogValue>(&bytes))
                        .transpose()?
                        .is_none_or(|latest| latest.secret_id != initial.secret_id) =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };

            let updated = current.apply_update(update.clone())?;

            if probed.as_ref() != Some(&updated) {
                updated.probe(id).await?;
                probed = Some(updated.clone());
            }

            if updated == current {
                let Some(latest) = self.db.get(&key).await? else {
                    return Err(Error::CatalogNotFound(id.clone()));
                };
                let latest: CatalogValue = decode(&latest)?;

                if latest.secret_id != initial.secret_id {
                    continue;
                }

                return Ok(());
            }

            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

            let Some(value) = tx.get(&key).await? else {
                return Err(Error::CatalogNotFound(id.clone()));
            };

            let old: CatalogValue = decode(&value)?;

            if old.secret_id != initial.secret_id {
                continue;
            }

            let old_name = SecretName::new(old.secret_id.to_string())?;
            let secret_id = rand::rng().gen_ulid(self.clock.as_ref());
            let name = SecretName::new(secret_id.to_string())?;
            self.secret_store
                .put(&name, serde_json::to_string(&updated)?)
                .await?;

            let result: Result<()> = async {
                tx.put(key.clone(), encode(&CatalogValue { secret_id })?)?;

                if let Some(handle) = tx.commit().await? {
                    handle.await_durable().await?;
                }

                Ok(())
            }
            .await;

            match result {
                Ok(()) => {
                    self.secret_store.delete(&old_name).await?;

                    return Ok(());
                }
                Err(Error::SlateDb(error)) if error.kind() == ErrorKind::Transaction => {
                    let _ = self.secret_store.delete(&name).await;
                    continue;
                }
                Err(error) => {
                    let _ = self.secret_store.delete(&name).await;

                    return Err(error);
                }
            }
        }
    }

    /// Removes the catalog mapping and its secret, doing nothing if the catalog is absent.
    #[instrument(name = "metadata.delete_catalog", skip_all, fields(catalog.id = %id))]
    pub async fn delete_catalog(&self, id: &CatalogId) -> Result<()> {
        info!(catalog.id = %id, "deleting catalog");

        let key = CatalogKey { id }.encode();
        let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        let Some(value) = tx.get(&key).await? else {
            return Ok(());
        };

        let CatalogValue { secret_id } = decode(&value)?;
        let name = SecretName::new(secret_id.to_string())?;

        tx.delete(key)?;

        if let Some(handle) = tx.commit().await? {
            handle.await_durable().await?;
        }

        self.secret_store.delete(&name).await?;

        Ok(())
    }

    async fn read_catalog_secret(&self, value: &CatalogValue) -> Result<CatalogConfig> {
        let name = SecretName::new(value.secret_id.to_string())?;
        let secret = self.secret_store.get(&name).await?;

        Ok(serde_json::from_str(secret.value())?)
    }
}

fn validate_rest_properties<V>(properties: &HashMap<String, V>) -> Result<()> {
    for key in RESERVED_REST_PROPERTIES {
        if properties.contains_key(key) {
            return Err(Error::ReservedCatalogProperty(key));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_ids_parse_uuid_identity() -> Result<()> {
        let id = CatalogId::new("01234567-89AB-CDEF-FEDC-BA9876543210")?;
        let compact = CatalogId::new("0123456789abcdeffedcba9876543210")?;

        assert_eq!(id, compact);

        assert_eq!(
            id,
            CatalogId::from(Uuid::from_u128(0x0123456789abcdeffedcba9876543210))
        );

        insta::assert_snapshot!(id.to_string(), @"01234567-89ab-cdef-fedc-ba9876543210");

        for id in ["", "catalog", "01234567-89ab-cdef-fedc-ba987654321g"] {
            assert!(matches!(
                CatalogId::new(id),
                Err(Error::InvalidCatalogId(_))
            ));
        }

        Ok(())
    }

    #[test]
    fn catalog_config_update_rejects_unknown_or_missing_fields() {
        let parsed: CatalogConfigUpdate =
            serde_json::from_str(r#"{"rest":{"properties":{"scope":"all","token":null}}}"#)
                .unwrap();

        assert_eq!(
            parsed,
            CatalogConfigUpdate::Rest(RestCatalogUpdate {
                properties: HashMap::from([
                    ("scope".into(), Some("all".into())),
                    ("token".into(), None),
                ]),
            })
        );

        for invalid in [
            r#"{"rest":{}}"#,
            r#"{"rest":{"uri":"https://other.example","properties":{}}}"#,
            r#"{"rest":{"warehouse":"other","properties":{}}}"#,
            r#"{"rest":{"unknown":true,"properties":{}}}"#,
        ] {
            assert!(serde_json::from_str::<CatalogConfigUpdate>(invalid).is_err());
        }
    }
}
