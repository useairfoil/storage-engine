use std::{collections::HashMap, sync::Arc, time::Duration};

use datafusion::{arrow::util::pretty::pretty_format_batches, prelude::SessionContext};
use iceberg::{
    Catalog, ErrorKind, NamespaceIdent, TableIdent, spec::DataContentType, table::Table,
};
use iceberg_datafusion::IcebergStaticTableProvider;
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
use serde_json::{Value, json};
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

/// Runs a query against the test table and pretty-prints the resulting rows.
pub async fn query_rows(table: Table, query: &str) -> Result<String, Box<dyn std::error::Error>> {
    let name = table.identifier().name.clone();
    let provider = IcebergStaticTableProvider::try_new_from_table(table).await?;
    let context = SessionContext::new();
    context.register_table(name, Arc::new(provider))?;
    let batches = context.sql(query).await?.collect().await?;
    Ok(pretty_format_batches(&batches)?.to_string())
}

/// Loads the current snapshot's manifest hierarchy with stable file aliases and no volatile metrics.
pub async fn snapshot_shape(table: &Table) -> Result<Value, Box<dyn std::error::Error>> {
    let snapshot = table
        .metadata()
        .current_snapshot()
        .ok_or("missing published snapshot")?;

    let list = table.manifest_list_reader(snapshot).load().await?;
    let mut files = list.entries().iter().collect::<Vec<_>>();
    files.sort_by_key(|file| (file.sequence_number, file.content as u8));

    let mut manifests = Vec::new();
    let mut aliases = HashMap::new();
    let mut data_count = 0;
    let mut delete_count = 0;
    for file in files {
        let manifest = file.load_manifest(table.file_io()).await?;
        for entry in manifest.entries() {
            aliases
                .entry(entry.file_path().to_owned())
                .or_insert_with(|| {
                    let (kind, count) = if entry.content_type() == DataContentType::Data {
                        ("data", &mut data_count)
                    } else {
                        ("delete", &mut delete_count)
                    };
                    *count += 1;
                    format!(
                        "{kind}-{count}.{}",
                        entry.file_format().to_string().to_lowercase()
                    )
                });
        }
        manifests.push((file, manifest));
    }

    let manifests = manifests
        .into_iter()
        .map(|(file, manifest)| {
            let entries = manifest
                .entries()
                .iter()
                .map(|entry| {
                    let data = entry.data_file();
                    let mut data_file = json!({
                        "path": aliases.get(entry.file_path()),
                        "content": format!("{:?}", entry.content_type()),
                        "format": data.file_format().to_string(),
                        "rows": data.record_count(),
                    });
                    if let Some(target) = data.referenced_data_file() {
                        data_file["referenced_data_file"] = json!(
                            aliases
                                .get(&target)
                                .map(String::as_str)
                                .unwrap_or("unlisted-data-file")
                        );
                    }
                    json!({
                        "status": format!("{:?}", entry.status()),
                        "sequence_number": entry.sequence_number(),
                        "data_file": data_file,
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "content": format!("{:?}", file.content),
                "sequence_number": file.sequence_number,
                "files": {
                    "added": file.added_files_count,
                    "existing": file.existing_files_count,
                    "deleted": file.deleted_files_count,
                },
                "entries": entries,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({"snapshot": {
        "sequence_number": snapshot.sequence_number(),
        "operation": snapshot.summary().operation.as_str(),
        "manifest_list": {"manifests": manifests},
    }}))
}

impl TestEnvironment {
    /// Starts an isolated stack in a named child of SE_CI_NAMESPACE, defaulting to a random root.
    pub async fn from_env(test_name: &str) -> Result<Self> {
        let catalog_config: CatalogConfig =
            serde_json::from_str(&env_var("SE_CI_CATALOG_CONFIG")?)?;

        let object_store_config: ObjectStoreConfig =
            serde_json::from_str(&env_var("SE_CI_OBJECT_STORE_CONFIG")?)?;
        let object_store = object_store_config.build()?;

        let rng = Arc::new(SeRand::default());

        let root = match env_var("SE_CI_NAMESPACE") {
            Ok(root) => root,
            Err(_) => format!("test_{}", rng.rng().gen_uuid().simple()),
        };
        let namespace = NamespaceIdent::from_strs([root.as_str(), test_name])?;
        let run_prefix = format!("{root}/{test_name}");

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(PrefixStore::new(object_store, run_prefix.clone()));

        let clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::default());
        let catalog_id = CatalogId::new("test-catalog")?;
        let catalog = catalog_config
            .to_catalog(&catalog_id, Arc::new(ObjectStoreStorageFactory))
            .await?;

        match catalog
            .create_namespace(&NamespaceIdent::new(root), Default::default())
            .await
        {
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NamespaceAlreadyExists => {}
            Err(error) => return Err(error.into()),
        }

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
