use clap::Args;
use se_meta_store::catalog::CatalogStore;
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
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("secret store error: {0}")]
    SecretStore(#[from] se_secret_store::Error),
}

impl DevArgs {
    pub async fn run(self, ct: CancellationToken) -> Result<(), Error> {
        let object_store = self.object_store.create_object_store()?;
        let secret_store = self
            .secret_store
            .create_secret_store(object_store.clone())
            .await?;
        let catalog_store = CatalogStore::new(secret_store, object_store);

        let listener = self.server.bind_listener().await?;

        let router = Routes::from(se_server::router(catalog_store.clone()))
            .add_service(se_ingestion_server::service(catalog_store, ct.clone()))
            .into_axum_router();

        tracing::info!(address = %listener.local_addr()?, "server listening");
        axum::serve(listener, router)
            .with_graceful_shutdown(ct.cancelled_owned())
            .await?;

        Ok(())
    }
}
