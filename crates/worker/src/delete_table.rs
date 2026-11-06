use futures::TryStreamExt;
use object_store::{ObjectStoreExt, path::Path};
use se_metadata_client::{Heartbeat, TaskToken};
use tracing::instrument;

use crate::{Error, Result, Worker};

impl Worker {
    #[instrument(name = "worker.delete_table", skip_all)]
    pub(super) async fn delete_table(&self, token: TaskToken, heartbeat: Heartbeat) -> Result<()> {
        heartbeat.beat().await.map_err(Error::Heartbeat)?;

        let prefix = Path::from("catalogs")
            .join(token.catalog_id.to_string())
            .join("tables")
            .join(token.table_uuid.to_string());

        let mut objects = self.object_store.list(Some(&prefix));
        while let Some(object) = objects.try_next().await? {
            match self.object_store.delete(&object.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }

            heartbeat.beat().await.map_err(Error::Heartbeat)?;
        }

        heartbeat.beat().await.map_err(Error::Heartbeat)?;

        self.metadata
            .complete_delete_table(token)
            .await
            .map_err(Into::into)
    }
}
