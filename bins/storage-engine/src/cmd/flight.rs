use std::sync::Arc;

use clap::Args;
use reqwest::Url;
use se_common::{clock::SystemClock, rand::SeRand};
use se_flight_server::FlightService;
use se_iceberg::storage::ObjectStoreStorageFactory;
use se_metadata_client::{HttpMetadataClient, MetadataClient};
use tokio_util::sync::CancellationToken;
use tonic::service::Routes;

use crate::{object_store::ObjectStoreArgs, server::ServerArgs};

#[derive(Debug, Args)]
pub struct FlightArgs {
    #[command(flatten)]
    server: ServerArgs,

    #[command(flatten)]
    object_store: ObjectStoreArgs,

    /// Base URL of the metadata HTTP server.
    #[arg(
        long = "metadata.url",
        env = "SE_METADATA_URL",
        default_value = "http://127.0.0.1:7777"
    )]
    metadata_url: Url,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("ingestion shutdown error: {0}")]
    Ingestion(#[from] se_ingestion::Error),
}

impl FlightArgs {
    /// Starts the Flight SQL server using the remote metadata server.
    pub async fn run(
        self,
        ct: CancellationToken,
        clock: Arc<dyn SystemClock>,
    ) -> Result<(), Error> {
        let listener = self.server.bind_listener().await?;
        let address = listener.local_addr()?;
        let metadata: Arc<dyn MetadataClient> =
            Arc::new(HttpMetadataClient::new(self.metadata_url));
        let service = FlightService::new(
            metadata,
            Arc::new(ObjectStoreStorageFactory),
            self.object_store.create_object_store()?,
            clock,
            Arc::new(SeRand::default()),
            ct.clone(),
        );
        let router = Routes::new(
            arrow_flight::flight_service_server::FlightServiceServer::new(service.clone()),
        )
        .into_axum_router();

        tracing::info!(%address, "Flight server listening");

        let result = axum::serve(listener, router)
            .with_graceful_shutdown(ct.cancelled_owned())
            .await
            .map_err(Error::from);

        result.and(service.close().await.map_err(Error::from))
    }
}
