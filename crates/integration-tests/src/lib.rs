use std::{sync::Arc, time::Duration};

use iceberg::{Catalog, NamespaceIdent, TableIdent, table::Table};
use object_store::{ObjectStore, prefix::PrefixStore};
use se_common::{
    clock::{DefaultSystemClock, SystemClock},
    id::IdGenerator,
    object_store::ObjectStoreConfig,
    rand::SeRand,
};
use se_iceberg::storage::ObjectStoreStorageFactory;
use se_ingestion::Ingestor;
use se_metadata::{CatalogConfig, CatalogId, Metadata};
use se_metadata_client::{HttpMetadataClient, MetadataClient};
use se_secret_store::memory::InMemorySecretStore;
use se_worker::Worker;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

const PUBLICATION_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cannot read environment variable {name}: {source}")]
    Environment {
        name: &'static str,
        #[source]
        source: std::env::VarError,
    },
    #[error("invalid configuration: {0}")]
    Config(#[from] serde_json::Error),
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("metadata error: {0}")]
    Metadata(#[from] se_metadata::Error),
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),
    #[error("ingestion error: {0}")]
    Ingestion(#[from] se_ingestion::Error),
    #[error("worker error: {0}")]
    Worker(#[from] se_worker::Error),
    #[error("background task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("timed out waiting for publication; table state: {0}")]
    PublicationTimeout(String),
    #[error("worker stopped before publication completed")]
    WorkerStopped,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct TestEnvironment {
    pub storage_prefix: String,
    pub object_store: Arc<dyn ObjectStore>,
    pub namespace: NamespaceIdent,
    pub catalog_id: CatalogId,
    pub catalog: Arc<dyn Catalog>,
    pub metadata: Metadata,
    pub metadata_client: Arc<dyn MetadataClient>,
    pub clock: Arc<dyn SystemClock>,
    pub ingestor: Ingestor,
    worker_cancel: CancellationToken,
    worker: AbortOnDropHandle<se_worker::Result<()>>,
    _server: AbortOnDropHandle<std::io::Result<()>>,
}

impl TestEnvironment {
    /// Starts an isolated metadata server, ingestor, and single worker against real storage.
    pub async fn from_env() -> Result<Self> {
        let catalog_config: CatalogConfig =
            serde_json::from_str(&env_var("SE_CI_CATALOG_CONFIG")?)?;

        let object_store_config: ObjectStoreConfig =
            serde_json::from_str(&env_var("SE_CI_OBJECT_STORE_CONFIG")?)?;
        let object_store = object_store_config.build()?;

        let rng = Arc::new(SeRand::default());

        let run_prefix = format!("test_{}", rng.rng().gen_uuid().simple());

        let namespace = NamespaceIdent::new(run_prefix.clone());
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(PrefixStore::new(object_store, run_prefix.clone()));

        let clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::default());
        let catalog_id = CatalogId::new("test-catalog")?;
        let catalog = catalog_config
            .to_catalog(&catalog_id, Arc::new(ObjectStoreStorageFactory))
            .await?;

        catalog
            .create_namespace(&namespace, Default::default())
            .await?;

        let metadata = Metadata::new(
            "metadata",
            object_store.clone(),
            Arc::new(InMemorySecretStore::new()),
            clock.clone(),
        )
        .await?;
        metadata
            .create_catalog(catalog_id.clone(), catalog_config)
            .await?;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?)
            .parse()
            .map_err(std::io::Error::other)?;
        let client: Arc<dyn MetadataClient> = Arc::new(HttpMetadataClient::new(url));
        let router = se_metadata_server::router(metadata.clone());
        let server =
            AbortOnDropHandle::new(tokio::spawn(
                async move { axum::serve(listener, router).await },
            ));
        let ingestor = Ingestor::new(
            client.clone(),
            object_store.clone(),
            rng.clone(),
            clock.clone(),
        );
        let worker = Worker::new(
            rng.rng().gen_uuid(),
            client.clone(),
            object_store.clone(),
            Arc::new(ObjectStoreStorageFactory),
            clock.clone(),
        );
        let worker_cancel = CancellationToken::new();
        let cancel = worker_cancel.clone();
        let worker = AbortOnDropHandle::new(tokio::spawn(worker.run(cancel)));

        Ok(Self {
            storage_prefix: run_prefix,
            object_store,
            namespace,
            catalog_id,
            catalog,
            metadata,
            metadata_client: client,
            clock,
            ingestor,
            worker_cancel,
            worker,
            _server: server,
        })
    }

    /// Identifies a table inside this fixture's namespace.
    pub fn table_ident(&self, name: impl Into<String>) -> TableIdent {
        TableIdent::new(self.namespace.clone(), name.into())
    }

    /// Waits for the table's WAL to be materialized and its desired revision to be published.
    pub async fn wait_for_publication(&self, table: &Table) -> Result<()> {
        let wait = async {
            loop {
                if self.worker.is_finished() {
                    return Err(Error::WorkerStopped);
                }
                let state = self
                    .metadata
                    .debug_table(&self.catalog_id, table.metadata().uuid())
                    .await?;
                if state.state.desired_revision > 0
                    && state.state.desired_revision == state.state.reconciled_revision
                    && state.wal.is_empty()
                {
                    return Ok(());
                }
                self.clock.sleep(Duration::from_millis(100)).await;
            }
        };
        tokio::select! {
            result = wait => result,
            _ = self.clock.sleep(PUBLICATION_TIMEOUT) => {
                let state = self.metadata.debug_table(&self.catalog_id, table.metadata().uuid()).await?;
                Err(Error::PublicationTimeout(serde_json::to_string_pretty(&state)?))
            }
        }
    }

    /// Flushes ingestion and stops the worker before closing metadata, retaining remote artifacts.
    pub async fn close(self) -> Result<()> {
        let ingestion = self.ingestor.close().await.map_err(Error::from);
        self.worker_cancel.cancel();
        let worker = match self.worker.await {
            Ok(result) => result.map_err(Error::from),
            Err(error) => Err(error.into()),
        };
        let metadata = self.metadata.close().await.map_err(Error::from);
        ingestion.and(worker).and(metadata)
    }
}

fn env_var(name: &'static str) -> Result<String> {
    std::env::var(name).map_err(|source| Error::Environment { name, source })
}
