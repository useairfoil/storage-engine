//! A single worker executing tasks from the metadata server.

pub mod reconciliation;

use std::{sync::Arc, time::Duration};

use iceberg::{io::StorageFactory, table::Table};
use object_store::ObjectStore;
use se_common::clock::SystemClock;
use se_metadata_client::{
    Heartbeat, MaterializationResult, MetadataClient, PreparedCommit, Task, TaskAssignment,
    WalClient,
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

enum TaskResult {
    Materialization(MaterializationResult),
    Reconciliation(PreparedCommit),
}

impl Worker {
    pub fn new(
        worker_id: Uuid,
        metadata: Arc<dyn MetadataClient>,
        clock: Arc<dyn SystemClock>,
        object_store: Arc<dyn ObjectStore>,
        storage_factory: Arc<dyn StorageFactory>,
    ) -> Self {
        Self {
            worker_id,
            metadata,
            clock,
            object_store,
            storage_factory,
        }
    }

    /// Executes one task at a time until cancellation or a metadata error.
    pub async fn run(self, ct: CancellationToken) -> Result<()> {
        loop {
            let Some(assignment) = ct
                .run_until_cancelled(self.metadata.claim_task(self.worker_id))
                .await
            else {
                return Ok(());
            };

            if let Some(assignment) = assignment? {
                let Some(result) = ct.run_until_cancelled(self.run_task(assignment)).await else {
                    return Ok(());
                };
                result?;
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

    async fn run_task(&self, assignment: TaskAssignment) -> Result<()> {
        let heartbeat = self.metadata.heartbeat_task(assignment.token.clone());
        let result = self.execute(&assignment, heartbeat).await;
        let result = match result {
            Ok(TaskResult::Materialization(result)) => {
                self.metadata
                    .complete_materialization(assignment.token.clone(), result)
                    .await
            }
            Ok(TaskResult::Reconciliation(commit)) => {
                match self
                    .metadata
                    .complete_reconciliation(assignment.token.clone(), commit)
                    .await
                {
                    Ok(()) => Ok(()),
                    Err(se_metadata_client::Error::TaskFenced) => Ok(()),
                    Err(error) => {
                        // Release only the lease; the durable plan/payload survives uncertain commits.
                        tracing::warn!(worker_id = %self.worker_id, attempt_id = %assignment.token.attempt_id, %error, "publication unconfirmed; retrying reconciliation");
                        self.metadata.fail_task(assignment.token.clone()).await
                    }
                }
            }
            Err(Error::Materialization(se_materialization::Error::Heartbeat(error)))
            | Err(Error::Heartbeat(error)) => {
                return match error {
                    se_metadata_client::Error::TaskFenced => Ok(()),
                    error => Err(error.into()),
                };
            }
            Err(error) => {
                tracing::warn!(worker_id = %self.worker_id, attempt_id = %assignment.token.attempt_id, %error, "task failed");
                self.metadata.fail_task(assignment.token.clone()).await
            }
        };
        match result {
            Ok(()) | Err(se_metadata_client::Error::TaskFenced) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn execute(
        &self,
        assignment: &TaskAssignment,
        heartbeat: Heartbeat,
    ) -> Result<TaskResult> {
        match &assignment.task {
            Task::Materialize {
                range, schema_id, ..
            } => {
                let table = self.load_table(assignment).await?;
                let token = &assignment.token;
                let wal = WalClient::new(
                    self.metadata.clone(),
                    token.catalog_id.clone(),
                    token.table_uuid,
                );
                Ok(TaskResult::Materialization(
                    se_materialization::run_materialization(
                        &table,
                        Arc::new(wal),
                        self.object_store.clone(),
                        *range,
                        *schema_id,
                        heartbeat,
                    )
                    .await?,
                ))
            }
            Task::Reconcile { plan } => {
                let input = self
                    .metadata
                    .reconciliation_input(assignment.token.clone())
                    .await?;
                if input.plan != *plan {
                    return Err(Error::InvalidReconciliation(
                        "assigned plan does not match input".into(),
                    ));
                }
                // A persisted payload is canonical even if the catalog has advanced.
                if let Some(commit) = input.prepared {
                    heartbeat.beat().await.map_err(Error::Heartbeat)?;
                    return Ok(TaskResult::Reconciliation(commit));
                }
                let table = self.load_table(assignment).await?;
                Ok(TaskResult::Reconciliation(
                    reconciliation::prepare_reconciliation(&table, plan, &input.files, &heartbeat)
                        .await?,
                ))
            }
        }
    }

    async fn load_table(&self, assignment: &TaskAssignment) -> Result<Table> {
        let token = &assignment.token;
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
