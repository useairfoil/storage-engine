use std::sync::Arc;

use clap::Args;
use se_common::clock::SystemClock;
use se_metadata::Metadata;
use tokio_util::sync::CancellationToken;

use crate::{
    object_store::ObjectStoreArgs,
    secret_store::{SecretStoreArgs, validate_secret_store},
    server::ServerArgs,
};

pub const METADB_PATH: &str = "metadb";

#[derive(Debug, Args)]
pub struct MetadataArgs {
    #[command(flatten)]
    server: ServerArgs,

    #[command(flatten)]
    object_store: ObjectStoreArgs,

    #[command(flatten)]
    secret_store: SecretStoreArgs,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("secret store error: {0}")]
    SecretStore(#[from] se_secret_store::Error),

    #[error("metadata error: {0}")]
    Metadata(#[from] se_metadata::Error),
}

impl MetadataArgs {
    /// Starts the metadata HTTP server and closes the database on shutdown.
    pub async fn run(
        self,
        ct: CancellationToken,
        clock: Arc<dyn SystemClock>,
    ) -> Result<(), Error> {
        let listener = self.server.bind_listener().await?;
        let address = listener.local_addr()?;

        let Some(metadata) = ct
            .run_until_cancelled(async {
                let object_store = self.object_store.create_object_store()?;
                let secret_store = self
                    .secret_store
                    .create_secret_store(object_store.clone())
                    .await?;

                validate_secret_store(secret_store.clone(), clock.clone()).await?;

                Ok::<_, Error>(Metadata::new(METADB_PATH, object_store, secret_store, clock).await?)
            })
            .await
            .transpose()?
        else {
            return Ok(());
        };

        let router = se_metadata_server::router(metadata.clone());

        tracing::info!(%address, "metadata server listening");

        let result = axum::serve(listener, router)
            .with_graceful_shutdown(ct.cancelled_owned())
            .await
            .map_err(Error::from);

        result.and(metadata.close().await.map_err(Error::from))
    }
}
