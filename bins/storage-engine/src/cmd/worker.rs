use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use clap::Args;
use reqwest::Url;
use se_common::{clock::SystemClock, id::IdGenerator, rand::SeRand};
use se_iceberg::storage::ObjectStoreStorageFactory;
use se_metadata_client::HttpMetadataClient;
use se_worker::Worker;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::object_store::ObjectStoreArgs;

#[derive(Debug, Args)]
pub struct WorkerArgs {
    #[command(flatten)]
    object_store: ObjectStoreArgs,

    /// Number of workers to run concurrently.
    #[arg(long, default_value = "1")]
    workers: NonZeroUsize,

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
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
}

impl WorkerArgs {
    /// Supervises workers using remote metadata until cancellation.
    pub async fn run(
        self,
        clock: Arc<dyn SystemClock>,
        ct: CancellationToken,
    ) -> Result<(), Error> {
        let metadata = Arc::new(HttpMetadataClient::new(self.metadata_url));
        let object_store = self.object_store.create_object_store()?;
        let rng = SeRand::default();

        let mut workers = JoinSet::new();

        let spawn_worker = |workers: &mut JoinSet<_>, restarting: bool| {
            let metadata = metadata.clone();
            let object_store = object_store.clone();
            let clock = clock.clone();
            let ct = ct.clone();
            let worker_id = rng.rng().gen_uuid();

            workers.spawn(async move {
                if restarting {
                    ct.run_until_cancelled(clock.sleep(Duration::from_secs(1)))
                        .await;
                }

                if ct.is_cancelled() {
                    return Ok(());
                }

                tracing::info!(%worker_id, "starting worker");

                Worker::new(
                    worker_id,
                    metadata,
                    object_store,
                    Arc::new(ObjectStoreStorageFactory),
                    clock,
                )
                .run(ct)
                .await
            });
        };

        for _ in 0..self.workers.get() {
            spawn_worker(&mut workers, false);
        }

        while let Some(result) = workers.join_next().await {
            match result {
                Ok(Err(_)) => {
                    tracing::warn!("worker failed")
                }
                Err(_) => {
                    tracing::warn!("worker task failed")
                }
                Ok(Ok(())) => {}
            }

            if !ct.is_cancelled() {
                spawn_worker(&mut workers, true);
            }
        }

        Ok(())
    }
}
