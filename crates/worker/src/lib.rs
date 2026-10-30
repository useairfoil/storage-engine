//! A single worker executing tasks from the metadata server.

pub mod reconciliation;

use std::{sync::Arc, time::Duration};

use iceberg::{io::StorageFactory, table::Table};
use object_store::{ObjectStore, path::Path};
use se_common::clock::SystemClock;
use se_iceberg::schema::{SchemaExt, TableMode};
use se_materialization::TableIndex;
use se_metadata_client::{
    IndexDelta, MetadataClient, ReconciliationPlan, Task, TaskAssignment, TaskToken, WalClient,
    WalRange,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const POLL_INTERVAL: Duration = Duration::from_secs(1);

pub struct Worker {
    worker_id: Uuid,
    metadata: Arc<dyn MetadataClient>,
    clock: Arc<dyn SystemClock>,
    object_store: Arc<dyn ObjectStore>,
    storage_factory: Arc<dyn StorageFactory>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("metadata request failed: {0}")]
    Metadata(#[from] se_metadata_client::Error),

    #[error("catalog not found")]
    CatalogNotFound,

    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    #[error("loaded table UUID {actual} does not match assigned table UUID {expected}")]
    TableUuidMismatch { expected: Uuid, actual: Uuid },

    #[error("materialization failed: {0}")]
    Materialization(#[from] se_materialization::Error),

    #[error("progress heartbeat failed: {0}")]
    Heartbeat(se_metadata_client::Error),

    #[error("invalid reconciliation input: {0}")]
    InvalidReconciliation(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Worker {
    pub fn new(
        worker_id: Uuid,
        metadata: Arc<dyn MetadataClient>,
        object_store: Arc<dyn ObjectStore>,
        storage_factory: Arc<dyn StorageFactory>,
        clock: Arc<dyn SystemClock>,
    ) -> Self {
        Self {
            worker_id,
            metadata,
            clock,
            object_store,
            storage_factory,
        }
    }

    /// Executes one task at a time, finishing and closing the current task before cancellation.
    #[tracing::instrument(skip_all, fields(worker_id = %self.worker_id), err)]
    pub async fn run(self, ct: CancellationToken) -> Result<()> {
        loop {
            let Some(assignment) = ct
                .run_until_cancelled(self.metadata.claim_task(self.worker_id))
                .await
            else {
                return Ok(());
            };

            if let Some(assignment) = assignment? {
                self.run_task(assignment).await?;
            }

            if ct
                .run_until_cancelled(self.clock.sleep(POLL_INTERVAL))
                .await
                .is_none()
            {
                return Ok(());
            }
        }
    }

    #[tracing::instrument(
        name = "worker.task",
        skip_all,
        fields(
            worker_id = %self.worker_id,
            attempt_id = %assignment.token.attempt_id,
            catalog.id = %assignment.token.catalog_id,
            table.ident = %assignment.token.table_ident,
            table.uuid = %assignment.token.table_uuid,
            task.kind = assignment.task.task_kind(),
            task.start_delay_ms,
        ),
        err
    )]
    async fn run_task(&self, assignment: TaskAssignment) -> Result<()> {
        if let Some(eligible_at) = assignment.eligible_at {
            let delay = (self.clock.now() - eligible_at)
                .to_std()
                .unwrap_or_default();
            tracing::Span::current().record("task.start_delay_ms", delay.as_secs_f64() * 1000.0);
        }

        let result = match assignment.task {
            Task::Materialize {
                range,
                schema_id,
                epoch,
            } => {
                self.materialize(assignment.token, range, schema_id, epoch)
                    .await
            }
            Task::Reconcile { plan } => self.reconcile(assignment.token, plan).await,
        };

        match result {
            Ok(()) | Err(Error::Metadata(se_metadata_client::Error::TaskFenced)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    #[tracing::instrument(
        name = "worker.materialize",
        skip_all,
        fields(wal.read_bytes, wal.read_rows),
        err
    )]
    async fn materialize(
        &self,
        token: TaskToken,
        range: WalRange,
        schema_id: i32,
        epoch: u64,
    ) -> Result<()> {
        let heartbeat = self.metadata.heartbeat_task(token.clone());

        let result = async {
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
            if schema.table_mode() == TableMode::Upsert {
                let path = Path::from("table-index")
                    .join(token.catalog_id.as_str())
                    .join(token.table_uuid.to_string());
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
                return match result {
                    Ok(result) => Ok((result, Some(index))),
                    Err(error) => {
                        index.close().await?;
                        Err(error)
                    }
                };
            }
            Ok((
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
            ))
        }
        .await;
        match result {
            Ok((result, index)) => {
                let delta = result.index_delta.map(|id| IndexDelta {
                    id,
                    range: result.range,
                });
                let completion = self
                    .metadata
                    .complete_materialization(token.clone(), result)
                    .await;
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
                    applied?;
                    closed?;
                }
                match completion {
                    Ok(()) | Err(se_metadata_client::Error::TaskFenced) => Ok(()),
                    Err(_) => {
                        // Either completion or failure wins atomically; recovery follows metadata.
                        tracing::warn!(worker_id = %self.worker_id, attempt_id = %token.attempt_id, "materialization unconfirmed; retrying from accepted index state");
                        self.metadata.fail_task(token.clone()).await
                    }
                }.map_err(Into::into)
            }
            Err(error) => self.task_failed(token, error).await,
        }
    }

    #[tracing::instrument(name = "worker.reconcile", skip_all, err)]
    async fn reconcile(&self, token: TaskToken, plan: ReconciliationPlan) -> Result<()> {
        let heartbeat = self.metadata.heartbeat_task(token.clone());
        let result = async {
            let input = self.metadata.reconciliation_input(token.clone()).await?;
            if input.plan != plan {
                return Err(Error::InvalidReconciliation(
                    "assigned plan does not match input".into(),
                ));
            }
            // A persisted payload is canonical even if the catalog has advanced.
            if let Some(commit) = input.prepared {
                heartbeat.beat().await.map_err(Error::Heartbeat)?;
                return Ok(commit);
            }
            let table = self.load_table(&token).await?;
            Ok(reconciliation::prepare_reconciliation(
                &table,
                &plan,
                &input.files,
                &input.added_vectors,
                &input.removed_vectors,
                &heartbeat,
            )
            .await?)
        }
        .await;
        match result {
            Ok(commit) => {
                match self
                    .metadata
                    .complete_reconciliation(token.clone(), commit)
                    .await
                {
                    Ok(()) | Err(se_metadata_client::Error::TaskFenced) => Ok(()),
                    Err(_) => {
                        // Release only the lease; the durable plan/payload survives uncertain commits.
                        tracing::warn!(worker_id = %self.worker_id, attempt_id = %token.attempt_id, "publication unconfirmed; retrying reconciliation");
                        self.metadata
                            .fail_task(token.clone())
                            .await
                            .map_err(Into::into)
                    }
                }
            }
            Err(error) => self.task_failed(token, error).await,
        }
    }

    async fn task_failed(&self, token: TaskToken, error: Error) -> Result<()> {
        match error {
            Error::Materialization(se_materialization::Error::Heartbeat(error))
            | Error::Heartbeat(error) => match error {
                se_metadata_client::Error::TaskFenced => Ok(()),
                error => Err(error.into()),
            },
            _ => {
                tracing::warn!(worker_id = %self.worker_id, attempt_id = %token.attempt_id, "task failed");
                self.metadata.fail_task(token).await.map_err(Into::into)
            }
        }
    }

    async fn load_table(&self, token: &TaskToken) -> Result<Table> {
        let config = self
            .metadata
            .get_catalog(&token.catalog_id)
            .await?
            .ok_or(Error::CatalogNotFound)?;

        let catalog = config
            .to_catalog(&token.catalog_id, self.storage_factory.clone())
            .await?;

        let table = catalog.load_table(&token.table_ident).await?;
        let actual = table.metadata().uuid();

        if actual != token.table_uuid {
            return Err(Error::TableUuidMismatch {
                expected: token.table_uuid,
                actual,
            });
        }

        Ok(table)
    }
}
