use futures::{StreamExt, TryStreamExt, stream};
use object_store::{ObjectStoreExt, path::Path};
use se_metadata_client::{Heartbeat, TaskToken, WalDeletion};
use tracing::instrument;

use crate::{Error, Result, Worker};

const DELETE_CONCURRENCY: usize = 8;

impl Worker {
    #[instrument(name = "worker.garbage_collect_wal", skip_all)]
    pub(super) async fn garbage_collect_wal(
        &self,
        token: TaskToken,
        batches: Vec<WalDeletion>,
        heartbeat: Heartbeat,
    ) -> Result<()> {
        heartbeat.beat().await.map_err(Error::Heartbeat)?;

        let mut deletions = stream::iter(batches)
            .map(|batch| {
                let heartbeat = &heartbeat;
                async move {
                    heartbeat.beat().await.map_err(Error::Heartbeat)?;

                    let path = Path::from(batch.metadata.location.as_str());
                    match self.object_store.delete(&path).await {
                        Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                        Err(error) => return Err(Error::from(error)),
                    }

                    heartbeat.beat().await.map_err(Error::Heartbeat)
                }
            })
            .buffer_unordered(DELETE_CONCURRENCY);

        while deletions.try_next().await?.is_some() {}

        heartbeat.beat().await.map_err(Error::Heartbeat)?;

        self.metadata
            .complete_wal_cleanup(token)
            .await
            .map_err(Into::into)
    }
}
