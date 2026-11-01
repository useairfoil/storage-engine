use std::sync::Arc;

use object_store::path::Path;
use se_iceberg::schema::{SchemaExt, TableMode};
use se_materialization::TableIndex;
use se_metadata_client::{IndexDelta, TaskToken, WalClient, WalRange};

use crate::{Error, Result, Worker};

impl Worker {
    #[tracing::instrument(
        name = "worker.materialize",
        skip_all,
        fields(wal.read_bytes, wal.read_rows),
    )]
    pub(super) async fn materialize(
        &self,
        token: TaskToken,
        range: WalRange,
        schema_id: i32,
        epoch: u64,
    ) -> Result<()> {
        let heartbeat = self.metadata.heartbeat_task(token.clone());
        let table = self.load_table(&token).await?;
        let wal = WalClient::new(
            self.metadata.clone(),
            token.catalog_id.clone(),
            token.table_uuid,
        );
        let schema = table
            .metadata()
            .schema_by_id(schema_id)
            .ok_or(se_materialization::Error::SchemaMissing(schema_id))?;

        let (result, index) = if schema.table_mode() == TableMode::Upsert {
            let path = Path::from("catalogs")
                .join(token.catalog_id.as_str())
                .join("tables")
                .join(token.table_uuid.to_string())
                .join("pk-index");
            let index =
                TableIndex::open(path, self.object_store.clone(), epoch, self.clock.clone())
                    .await?;
            let result = async {
                let applied = index.applied_through().await?;
                let input = self
                    .metadata
                    .materialization_input(token.clone(), applied)
                    .await?;
                index.catch_up(&input.deltas, &heartbeat).await?;
                Ok::<_, Error>(
                    se_materialization::run_upsert_materialization(
                        &table,
                        Arc::new(wal),
                        self.object_store.clone(),
                        &token,
                        range,
                        schema_id,
                        &index,
                        &heartbeat,
                    )
                    .await?,
                )
            }
            .await;

            match result {
                Ok(result) => (result, Some(index)),
                Err(error) => {
                    index.close().await?;
                    return Err(error);
                }
            }
        } else {
            (
                se_materialization::run_materialization(
                    &table,
                    Arc::new(wal),
                    self.object_store.clone(),
                    range,
                    schema_id,
                    heartbeat,
                )
                .await?,
                None,
            )
        };

        let delta = result.index_delta.map(|id| IndexDelta {
            id,
            range: result.range,
        });
        let completion = self.metadata.complete_materialization(token, result).await;

        if let Some(index) = index {
            // A timeout is not a rejection: recover from metadata on the next assignment.
            let applied = if completion.is_ok() {
                if let Some(delta) = delta {
                    index.apply(&delta).await
                } else {
                    Ok(())
                }
            } else {
                Ok(())
            };
            let closed = index.close().await;
            if let Err(error) = applied.and(closed) {
                // Local maintenance must not turn an accepted completion into a failed task.
                tracing::warn!(error = %error, message = "index maintenance failed; recovering on next assignment");
            }
        }

        completion.map_err(Into::into)
    }
}
