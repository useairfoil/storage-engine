use std::sync::Arc;

use clap::Args;
use reqwest::Url;
use se_iceberg::storage::TracedStorageFactory;
use se_metadata_client::MetadataClient;
use tokio_util::sync::CancellationToken;
use tonic::service::Routes;

use crate::server::ServerArgs;

#[derive(Debug, Args)]
pub struct FlightArgs {
    #[command(flatten)]
    server: ServerArgs,

    /// Base URL of the metadata HTTP server.
    #[arg(long = "metadata.url", env = "SE_METADATA_URL")]
    metadata_url: Url,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl FlightArgs {
    /// Starts the Flight SQL server using the remote metadata server.
    pub async fn run(self, ct: CancellationToken) -> Result<(), Error> {
        let listener = self.server.bind_listener().await?;
        let address = listener.local_addr()?;
        let metadata = MetadataClient::new(self.metadata_url);
        let router = Routes::new(se_flight_server::service(
            metadata,
            Arc::new(TracedStorageFactory),
            ct.clone(),
        ))
        .into_axum_router();

        tracing::info!(%address, "Flight server listening");

        axum::serve(listener, router)
            .with_graceful_shutdown(ct.cancelled_owned())
            .await?;

        Ok(())
    }
}
