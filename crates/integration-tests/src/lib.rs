use std::{collections::HashMap, sync::Arc};

use buffer::Clock;
use object_store::{ObjectStore, prefix::PrefixStore};
use object_store_opendal::OpendalStore;
use opendal::Operator;
use se_common::{clock::DefaultSystemClock, id::IdGenerator};
use se_ingestion::Ingestor;
use se_meta_store::catalog::{CatalogName, CatalogStore, NamespaceIdent, TableIdent};
use se_reconciliation::Reconciler;
use se_secret_store::{SecretName, SecretStore, azure::AzureKeyVaultSecretStore};
use serde::Deserialize;

/// Errors setting up an integration test environment.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cannot read environment variable {name}: {source}")]
    Environment {
        name: &'static str,
        #[source]
        source: std::env::VarError,
    },
    #[error("invalid catalog name: {0}")]
    CatalogName(#[from] se_common::name::ResourceError),
    #[error("secret store error: {0}")]
    SecretStore(#[from] se_secret_store::Error),
    #[error("invalid object store configuration: {0}")]
    ObjectStoreConfig(#[from] serde_json::Error),
    #[error("opendal error: {0}")]
    OpenDal(#[from] opendal::Error),
    #[error("catalog not found: {0}")]
    CatalogNotFound(CatalogName),
    #[error("catalog store error: {0}")]
    CatalogStore(#[from] se_meta_store::catalog::Error),
    #[error("iceberg catalog error: {0}")]
    Iceberg(#[from] se_meta_store::catalog::IcebergError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An ingestor and reconciler sharing isolated, persistent engine state.
///
/// Each fixture gets one unique identifier used for both its engine storage
/// prefix and its namespace in the shared catalog. Tests create all tables in
/// that namespace. No remote resources are deleted automatically.
pub struct TestEnvironment {
    /// Prefix relative to the supplied engine store, retained for diagnosis and cleanup.
    pub storage_prefix: String,
    /// Engine store rooted at this environment's prefix, also usable for restarts.
    pub object_store: Arc<dyn ObjectStore>,
    /// Namespace created in the shared catalog, identical to the engine prefix.
    pub namespace: NamespaceIdent,
    pub catalog_name: CatalogName,
    pub catalog_store: CatalogStore,
    pub clock: Arc<dyn Clock>,
    pub ingestor: Ingestor,
    pub reconciler: Reconciler,
}

#[derive(Deserialize)]
struct ObjectStoreConfig {
    scheme: String,
    options: HashMap<String, String>,
}

impl TestEnvironment {
    pub async fn from_env() -> Result<Self> {
        let catalog_name = CatalogName::new(env_var("SE_CI_CATALOG_NAME")?)?;
        let object_store_id = env_var("SE_CI_OBJECT_STORE")?;

        let vault_url = env_var("SE_CI_KEY_VAULT_URL")?;
        let secret_store: Arc<dyn SecretStore> =
            Arc::new(AzureKeyVaultSecretStore::from_url(&vault_url)?);

        let secret = secret_store
            .get(&SecretName::new(format!("object-store/{object_store_id}"))?)
            .await?;

        let config: ObjectStoreConfig = serde_json::from_str(secret.value())?;
        let operator = Operator::via_iter(config.scheme, config.options)?;
        let object_store = Arc::new(OpendalStore::new(operator));

        Self::from_store(object_store, catalog_name, secret_store).await
    }

    /// Identifies a table inside this fixture's namespace.
    pub fn table_ident(&self, name: impl Into<String>) -> TableIdent {
        TableIdent::new(self.namespace.clone(), name.into())
    }

    async fn from_store(
        object_store: Arc<dyn ObjectStore>,
        catalog_name: CatalogName,
        secret_store: Arc<dyn SecretStore>,
    ) -> Result<Self> {
        let run_prefix = format!("test_{}", rand::rng().gen_uuid().simple());
        let namespace = NamespaceIdent::new(run_prefix.clone());

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(PrefixStore::new(object_store, run_prefix.clone()));

        let catalog_store = CatalogStore::new(secret_store, object_store.clone());

        let stored_catalog = catalog_store
            .get(catalog_name.clone())
            .await?
            .ok_or_else(|| Error::CatalogNotFound(catalog_name.clone()))?;

        let catalog = stored_catalog.to_catalog().await?;
        catalog
            .create_namespace(&namespace, Default::default())
            .await?;

        let clock: Arc<_> = DefaultSystemClock::default().into();

        let ingestor = Ingestor::new(catalog_store.clone(), clock.clone());

        let reconciler = Reconciler::new(catalog_store.clone(), clock.clone());

        Ok(Self {
            storage_prefix: run_prefix,
            object_store,
            namespace,
            catalog_name,
            catalog_store,
            clock,
            ingestor,
            reconciler,
        })
    }
}

fn env_var(name: &'static str) -> Result<String> {
    std::env::var(name).map_err(|source| Error::Environment { name, source })
}
