use std::sync::Arc;

use clap::{Args, Subcommand};
use se_common::clock::DefaultSystemClock;
use se_meta_store::catalog::CatalogStore;
use se_scheduler::NoopScheduler;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tonic::service::Routes;

use crate::{
    object_store::ObjectStoreArgs,
    secret_store::{SecretStoreArgs, validate_secret_store},
    server::ServerArgs,
};

#[derive(Debug, Args)]
pub struct StartArgs {
    #[command(subcommand)]
    command: StartCommand,
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
}

#[derive(Debug, Subcommand)]
enum StartCommand {
    /// Start Arrow Flight and the catalog API without a scheduler or worker pool.
    Server(StartServerArgs),
}

#[derive(Debug, Args)]
struct StartServerArgs {
    #[command(flatten)]
    server: ServerArgs,
    #[command(flatten)]
    object_store: ObjectStoreArgs,
    #[command(flatten)]
    secret_store: SecretStoreArgs,
}

impl StartArgs {
    pub async fn run(self, ct: CancellationToken) -> Result<(), Error> {
        match self.command {
            StartCommand::Server(args) => args.run(ct).await,
        }
    }
}

impl StartServerArgs {
    async fn run(self, ct: CancellationToken) -> Result<(), Error> {
        let clock: Arc<_> = DefaultSystemClock::default().into();

        let Some(catalog_store) = ct
            .run_until_cancelled(async {
                let object_store = self.object_store.create_object_store()?;
                let secret_store = self
                    .secret_store
                    .create_secret_store(object_store.clone())
                    .await?;

                validate_secret_store(secret_store.clone(), clock.clone()).await?;

                let catalog_store = CatalogStore::new(secret_store, object_store);

                Ok::<_, Error>(catalog_store)
            })
            .await
            .transpose()?
        else {
            return Ok(());
        };

        let ingestor =
            se_ingestion::Ingestor::new(catalog_store.clone(), clock, Arc::new(NoopScheduler));

        let router = Routes::from(se_server::router(catalog_store.clone()))
            .add_service(se_flight_server::service(
                catalog_store,
                ingestor.clone(),
                ct.clone(),
            ))
            .into_axum_router();

        let listener = self.server.bind_listener().await?;
        let address = listener.local_addr()?;

        tracing::info!(%address, "server listening");

        let _cancel = ct.clone().drop_guard();

        let result = axum::serve(listener, router)
            .with_graceful_shutdown(ct.cancelled_owned())
            .await
            .map_err(Error::from);

        result.and(ingestor.close().await.map_err(Error::from))
    }
}
