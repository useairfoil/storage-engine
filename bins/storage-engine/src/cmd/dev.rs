use std::sync::Arc;

use clap::Args;
use object_store::path::Path;
use se_common::clock::{DefaultSystemClock, SystemClock};
use se_meta_store::catalog::CatalogStore;
use se_reconciliation::Reconciler;
use se_scheduler::object_store::{
    DEFAULT_LOCATION, ObjectStoreBroker, ObjectStoreSchedulerOptions,
};
use se_secret_store::{SecretName, SecretStore};
use se_worker::{WorkerPool, WorkerPoolOptions};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tonic::service::Routes;

use crate::{object_store::ObjectStoreArgs, secret_store::SecretStoreArgs, server::ServerArgs};

#[derive(Debug, Args)]
pub struct DevArgs {
    #[command(flatten)]
    pub server: ServerArgs,
    #[command(flatten)]
    pub object_store: ObjectStoreArgs,
    #[command(flatten)]
    pub secret_store: SecretStoreArgs,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("opendal error: {0}")]
    OpenDal(#[from] opendal::Error),
    #[error("secret store error: {0}")]
    SecretStore(#[from] se_secret_store::Error),
    #[error("ingestion shutdown error: {0}")]
    Ingestion(#[from] se_ingestion::Error),
    #[error("scheduler error: {0}")]
    Scheduler(#[from] se_scheduler::Error),
    #[error("worker pool error: {0}")]
    Worker(#[from] se_worker::Error),
    #[error("broker task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

impl DevArgs {
    pub async fn run(self, ct: CancellationToken) -> Result<(), Error> {
        let clock: Arc<_> = DefaultSystemClock::default().into();

        let startup = async {
            let object_store = self.object_store.create_object_store()?;
            let secret_store = self
                .secret_store
                .create_secret_store(object_store.clone())
                .await?;

            validate_secret_store(secret_store.clone(), clock.clone()).await?;

            let catalog_store = CatalogStore::new(secret_store, object_store.clone());

            let listener = self.server.bind_listener().await?;

            let address = listener.local_addr()?;

            let broker = ObjectStoreBroker::new(
                object_store,
                Path::from(DEFAULT_LOCATION),
                address.to_string(),
                ObjectStoreSchedulerOptions::default(),
                clock.clone(),
            )
            .await?;

            Ok::<_, Error>((catalog_store, listener, address, broker))
        };

        let (catalog_store, listener, address, broker) = tokio::select! {
            biased;
            () = ct.cancelled() => return Ok(()),
            result = startup => result?,
        };

        let (broker_handle, scheduler) = broker.run(ct.clone());

        let options = WorkerPoolOptions::default();
        tracing::info!(
            concurrency = options.concurrency.get(),
            "starting worker pool"
        );

        let pool = WorkerPool::new(
            scheduler.clone(),
            Reconciler::new(catalog_store.clone(), clock.clone()),
            options,
        );

        let ingestor = se_ingestion::Ingestor::new(catalog_store.clone(), clock, scheduler);

        let router = Routes::from(se_server::router(catalog_store))
            .add_service(se_ingestion_server::service(ingestor.clone(), ct.clone()))
            .into_axum_router();

        tracing::info!(%address, "server listening");

        let broker = async {
            let _cancel = ct.clone().drop_guard();
            broker_handle.await?.map_err(Error::from)
        };

        let workers = async {
            let _cancel = ct.clone().drop_guard();
            pool.run(ct.clone()).await.map_err(Error::from)
        };

        let server = async {
            let _cancel = ct.clone().drop_guard();
            let result = axum::serve(listener, router)
                .with_graceful_shutdown(ct.clone().cancelled_owned())
                .await
                .map_err(Error::from);

            let close_result = ingestor.close().await.map_err(Error::from);
            drop(ingestor);
            result.and(close_result)
        };

        let (broker_result, worker_result, server_result) = tokio::join!(broker, workers, server);

        broker_result.and(worker_result).and(server_result)
    }
}

async fn validate_secret_store(
    store: Arc<dyn SecretStore>,
    clock: Arc<dyn SystemClock>,
) -> Result<(), se_secret_store::Error> {
    let now = clock.now();
    let value = now.to_rfc3339();

    let name = SecretName::new("se-secret-store-init")?;
    store.put(&name, value).await?;

    Ok(())
}
