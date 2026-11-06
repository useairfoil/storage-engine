//! A single worker executing tasks from the metadata server.

mod delete_table;
mod garbage_collect_wal;
mod materialize;
mod reconcile;
pub mod reconciliation;

use std::{sync::Arc, time::Duration};

use iceberg::{io::StorageFactory, table::Table};
use object_store::ObjectStore;
use se_common::clock::SystemClock;
use se_metadata_client::{MetadataClient, Task, TaskAssignment, TaskToken};
use tokio_util::sync::CancellationToken;
use tracing::{Span, info, instrument};
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

    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

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

    /// Executes and supervises one task at a time, finishing the current task before cancellation.
    pub async fn run(self, ct: CancellationToken) -> Result<()> {
        loop {
            let Some(assignment) = ct
                .run_until_cancelled(self.metadata.claim_task(self.worker_id))
                .await
            else {
                return Ok(());
            };

            match assignment? {
                Some(assignment) => {
                    let timeout = assignment.timeout;
                    let assignment = assignment.assignment;
                    let token = assignment.token.clone();
                    let task_kind = assignment.task.task_kind();
                    info!(worker.id = %self.worker_id, attempt.id = %token.attempt_id, catalog.id = %token.catalog_id, table.ident = %token.table_ident, table.uuid = %token.table_uuid, task.kind = task_kind, "task assigned");

                    match self.run_task(assignment, timeout).await {
                        Ok(()) => {
                            info!(worker.id = %self.worker_id, attempt.id = %token.attempt_id, catalog.id = %token.catalog_id, table.ident = %token.table_ident, table.uuid = %token.table_uuid, task.kind = task_kind, "task completed");
                        }
                        Err(error) if error.is_fenced() => {
                            info!(worker.id = %self.worker_id, attempt.id = %token.attempt_id, catalog.id = %token.catalog_id, table.ident = %token.table_ident, table.uuid = %token.table_uuid, task.kind = task_kind, "task fenced");
                        }
                        Err(error) => {
                            tracing::error!(error = %error, worker.id = %self.worker_id, attempt.id = %token.attempt_id, catalog.id = %token.catalog_id, table.ident = %token.table_ident, table.uuid = %token.table_uuid, task.kind = task_kind, message = "task failed");

                            if error.is_heartbeat() {
                                return Err(error);
                            }

                            // Release only the attempt; accepted progress and prepared commits survive.
                            match self.metadata.fail_task(token).await {
                                Ok(()) | Err(se_metadata_client::Error::TaskFenced) => {}
                                Err(error) => return Err(error.into()),
                            }
                        }
                    }
                }
                None => {
                    if ct
                        .run_until_cancelled(self.clock.sleep(POLL_INTERVAL))
                        .await
                        .is_none()
                    {
                        return Ok(());
                    }
                }
            }
        }
    }

    #[instrument(
        name = "worker.task",
        skip_all,
        fields(
            worker.id = %self.worker_id,
            attempt.id = %assignment.token.attempt_id,
            catalog.id = %assignment.token.catalog_id,
            table.ident = %assignment.token.table_ident,
            table.uuid = %assignment.token.table_uuid,
            task.kind = assignment.task.task_kind(),
            task.start_delay_ms,
        ),
        err
    )]
    async fn run_task(&self, assignment: TaskAssignment, timeout: Duration) -> Result<()> {
        if let Some(eligible_at) = assignment.eligible_at {
            let delay = (self.clock.now() - eligible_at)
                .to_std()
                .unwrap_or_default();
            Span::current().record("task.start_delay_ms", delay.as_secs_f64() * 1000.0);
        }

        let heartbeat =
            self.metadata
                .heartbeat_task(assignment.token.clone(), timeout, self.clock.clone());
        match assignment.task {
            Task::Materialize {
                range,
                schema_id,
                epoch,
            } => {
                self.materialize(assignment.token, range, schema_id, epoch, heartbeat)
                    .await
            }
            Task::Reconcile { plan } => self.reconcile(assignment.token, plan, heartbeat).await,
            Task::DeleteTable => self.delete_table(assignment.token, heartbeat).await,
            Task::GarbageCollectWal { batches } => {
                self.garbage_collect_wal(assignment.token, batches, heartbeat)
                    .await
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

impl Error {
    /// Returns whether metadata fenced the task attempt.
    pub fn is_fenced(&self) -> bool {
        matches!(
            self,
            Self::Metadata(se_metadata_client::Error::TaskFenced)
                | Self::Heartbeat(se_metadata_client::Error::TaskFenced)
                | Self::Materialization(se_materialization::Error::Heartbeat(
                    se_metadata_client::Error::TaskFenced,
                ))
        )
    }

    /// Returns whether a task progress heartbeat failed.
    pub fn is_heartbeat(&self) -> bool {
        matches!(
            self,
            Self::Heartbeat(_) | Self::Materialization(se_materialization::Error::Heartbeat(_))
        )
    }
}
