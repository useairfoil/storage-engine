use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    sync::Arc,
};

use clap::Args;
use se_common::{clock::SystemClock, id::IdGenerator, rand::SeRand};
use se_flight_server::FlightService;
use se_iceberg::storage::ObjectStoreStorageFactory;
use se_metadata::Metadata;
use se_metadata_client::{HttpMetadataClient, MetadataClient};
use se_worker::Worker;
use tokio_util::sync::CancellationToken;
use tonic::service::Routes;

use crate::{
    cmd::metadata::METADB_PATH,
    object_store::ObjectStoreArgs,
    secret_store::{SecretStoreArgs, validate_secret_store},
    server::ServerArgs,
};

#[derive(Debug, Args)]
pub struct DevArgs {
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

    #[error("ingestion shutdown error: {0}")]
    Ingestion(#[from] se_ingestion::Error),

    #[error("worker error: {0}")]
    Worker(#[from] se_worker::Error),
}

impl DevArgs {
    /// Starts metadata, Flight SQL, and one worker using the HTTP metadata client.
    pub async fn run(
        self,
        clock: Arc<dyn SystemClock>,
        ct: CancellationToken,
    ) -> Result<(), Error> {
        let listener = self.server.bind_listener().await?;
        let address = listener.local_addr()?;
        let mut metadata_address = address;

        if address.ip().is_unspecified() {
            metadata_address.set_ip(match address.ip() {
                IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
                IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
            });
        }

        let url = format!("http://{metadata_address}")
            .parse()
            .map_err(std::io::Error::other)?;

        let client: Arc<dyn MetadataClient> = Arc::new(HttpMetadataClient::new(url));
        let object_store = self.object_store.create_object_store()?;

        let Some(metadata) = ct
            .run_until_cancelled(async {
                let secret_store = self
                    .secret_store
                    .create_secret_store(object_store.clone())
                    .await?;

                validate_secret_store(secret_store.clone(), clock.clone()).await?;

                Ok::<_, Error>(
                    Metadata::new(
                        METADB_PATH,
                        object_store.clone(),
                        secret_store,
                        clock.clone(),
                    )
                    .await?,
                )
            })
            .await
            .transpose()?
        else {
            return Ok(());
        };

        let rng = SeRand::default();
        let worker_id = rng.rng().gen_uuid();
        let worker = Worker::new(
            worker_id,
            client.clone(),
            object_store.clone(),
            Arc::new(ObjectStoreStorageFactory),
            clock.clone(),
        );
        let service = FlightService::new(
            client,
            Arc::new(ObjectStoreStorageFactory),
            object_store,
            Arc::new(rng),
            clock,
            ct.clone(),
        );
        let router = Routes::from(se_metadata_server::router(metadata.clone()))
            .add_service(
                arrow_flight::flight_service_server::FlightServiceServer::new(service.clone()),
            )
            .into_axum_router();

        tracing::info!(%address, %worker_id, "development server listening with one worker");

        let server = async {
            let _cancel = ct.clone().drop_guard();
            axum::serve(listener, router)
                .with_graceful_shutdown(ct.clone().cancelled_owned())
                .await
                .map_err(Error::from)
        };
        let worker = async {
            let _cancel = ct.clone().drop_guard();
            worker.run(ct.clone()).await.map_err(Error::from)
        };
        let (server_result, worker_result) = tokio::join!(server, worker);

        let ingestion_result = service.close().await.map_err(Error::from);
        let metadata_result = metadata.close().await.map_err(Error::from);

        server_result
            .and(worker_result)
            .and(ingestion_result)
            .and(metadata_result)
    }
}
