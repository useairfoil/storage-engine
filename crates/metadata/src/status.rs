use chrono::{DateTime, Utc};
use iceberg::TableIdent;
use serde::Serialize;
use slatedb::DbSnapshot;
use uuid::Uuid;

use crate::{
    BatchMetadata, CatalogId, Error, Metadata, Result, Scheduled, TableWork, TaskFailureCode,
    encoding::{TableKey, TableRecordType, WalStateValue, decode},
    tasks::{PersistedTaskFailure, TaskKind},
};

const TRANSIENT_FAILURE_THRESHOLD: u32 = 5;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableStatus {
    pub table_uuid: Uuid,
    pub table_ident: TableIdent,
    pub checkpoint_interval_sec: u64,
    pub observed_at: DateTime<Utc>,
    pub lifecycle: TableLifecycle,
    pub health: TableHealth,
    pub ingestion: TableIngestionStatus,
    pub maintenance: TableMaintenanceStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TableLifecycle {
    Active,
    Deleting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TableHealth {
    Healthy,
    Degraded,
    Failing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableIngestionStatus {
    pub wal: TableWalStatus,
    pub iceberg: TableIcebergStatus,
    pub materialization: TableTaskStatus,
    pub reconciliation: TableTaskStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableWalStatus {
    pub accepted_next_sequence: u64,
    pub materialized_next_sequence: u64,
    pub pending_batches: u64,
    pub pending_bytes: u64,
    pub oldest_pending_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableIcebergStatus {
    pub desired_revision: u64,
    pub reconciled_revision: u64,
    pub reconciled_snapshot_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableMaintenanceStatus {
    pub wal_gc: TableWalGcStatus,
    pub deletion: TableTaskStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableWalGcStatus {
    #[serde(flatten)]
    pub task: TableTaskStatus,
    pub pending_batches: u64,
    pub pending_bytes: u64,
    pub oldest_pending_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableTaskStatus {
    pub state: TableTaskState,
    pub eligible_at: Option<DateTime<Utc>>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub failure: Option<TableTaskFailure>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TableTaskState {
    Idle,
    Scheduled,
    Queued,
    Running,
    Retrying,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableTaskFailure {
    pub code: TaskFailureCode,
    pub message: &'static str,
    pub failed_at: DateTime<Utc>,
    pub consecutive_failures: u32,
}

impl Metadata {
    /// Reads a consistent summary of table ingestion and maintenance status without changing state.
    pub async fn table_status(
        &self,
        catalog_id: &CatalogId,
        table_uuid: Uuid,
    ) -> Result<TableStatus> {
        let snapshot = self.db.snapshot().await?;
        let now = self.clock.now();
        let table = TableKey {
            catalog_id,
            table_uuid,
        };
        let work: TableWork = decode(
            &snapshot
                .get(table.record(TableRecordType::TableWork))
                .await?
                .ok_or(Error::TableNotResolved(table_uuid))?,
        )?;
        let wal_state: WalStateValue = snapshot
            .get(table.record(TableRecordType::WalState))
            .await?
            .map(|bytes| decode(&bytes))
            .transpose()?
            .unwrap_or_default();

        let mut wal = TableWalStatus {
            accepted_next_sequence: wal_state.next_sequence,
            materialized_next_sequence: work.materialized_next,
            pending_batches: 0,
            pending_bytes: 0,
            oldest_pending_at: None,
        };
        let scan = table.wal_batches(work.materialized_next..);
        let mut batches = snapshot.scan_prefix(scan.prefix, scan.suffix).await?;

        while let Some(batch) = batches.next().await? {
            let batch: BatchMetadata = decode(&batch.value)?;
            wal.pending_batches += 1;
            wal.pending_bytes += batch.wal_size_bytes;
            wal.oldest_pending_at = Some(
                wal.oldest_pending_at
                    .map_or(batch.timestamp, |oldest| oldest.min(batch.timestamp)),
            );
        }

        let mut wal_gc_pending_batches = 0;
        let mut wal_gc_pending_bytes = 0;
        let mut wal_gc_oldest_pending_at: Option<DateTime<Utc>> = None;
        let scan = table.scan(TableRecordType::WalDeletion);
        let mut deletions = snapshot.scan_prefix(scan.prefix, scan.suffix).await?;

        while let Some(entry) = deletions.next().await? {
            let deletion: crate::WalDeletion = decode(&entry.value)?;
            wal_gc_pending_batches += 1;
            wal_gc_pending_bytes += deletion.metadata.wal_size_bytes;
            wal_gc_oldest_pending_at = Some(
                wal_gc_oldest_pending_at
                    .map_or(deletion.queued_at, |oldest| oldest.min(deletion.queued_at)),
            );
        }

        let materialization = TableTaskStatus::new(
            work.materialization.as_ref(),
            read_task_failure(&snapshot, &table, TaskKind::Materialize).await?,
            now,
        );
        let reconciliation = TableTaskStatus::new(
            work.reconciliation.as_ref(),
            read_task_failure(&snapshot, &table, TaskKind::Reconcile).await?,
            now,
        );
        let wal_gc_task = TableTaskStatus::new(
            work.wal_cleanup.as_ref(),
            read_task_failure(&snapshot, &table, TaskKind::GarbageCollectWal).await?,
            now,
        );
        let deletion = TableTaskStatus::new(
            work.deletion.as_ref(),
            read_task_failure(&snapshot, &table, TaskKind::DeleteTable).await?,
            now,
        );

        let lifecycle = if work.deletion.is_some() {
            TableLifecycle::Deleting
        } else {
            TableLifecycle::Active
        };
        let health = match lifecycle {
            TableLifecycle::Deleting => task_health(&deletion),
            TableLifecycle::Active => [&materialization, &reconciliation, &wal_gc_task]
                .into_iter()
                .map(task_health)
                .max()
                .unwrap_or(TableHealth::Healthy),
        };

        Ok(TableStatus {
            table_uuid,
            table_ident: work.table_ident,
            checkpoint_interval_sec: work.checkpoint_interval_sec,
            observed_at: now,
            lifecycle,
            health,
            ingestion: TableIngestionStatus {
                wal,
                iceberg: TableIcebergStatus {
                    desired_revision: work.desired_revision,
                    reconciled_revision: work.reconciled_revision,
                    reconciled_snapshot_id: work.reconciled_snapshot_id,
                },
                materialization,
                reconciliation,
            },
            maintenance: TableMaintenanceStatus {
                wal_gc: TableWalGcStatus {
                    task: wal_gc_task,
                    pending_batches: wal_gc_pending_batches,
                    pending_bytes: wal_gc_pending_bytes,
                    oldest_pending_at: wal_gc_oldest_pending_at,
                },
                deletion,
            },
        })
    }
}

impl TableTaskStatus {
    fn new(
        scheduled: Option<&Scheduled>,
        persisted_failure: Option<PersistedTaskFailure>,
        now: DateTime<Utc>,
    ) -> Self {
        let Some(scheduled) = scheduled else {
            return Self {
                state: TableTaskState::Idle,
                eligible_at: None,
                lease_expires_at: None,
                failure: persisted_failure.map(TableTaskFailure::from),
            };
        };

        if let Some(assignment) = &scheduled.assignment {
            if assignment.expires_at > now {
                return Self {
                    state: TableTaskState::Running,
                    eligible_at: None,
                    lease_expires_at: Some(assignment.expires_at),
                    failure: persisted_failure.map(TableTaskFailure::from),
                };
            }

            let consecutive_failures = persisted_failure
                .map_or(1, |failure| failure.consecutive_failures.saturating_add(1));

            return Self {
                state: TableTaskState::Queued,
                eligible_at: Some(scheduled.eligible_at),
                lease_expires_at: None,
                failure: Some(TableTaskFailure::from(PersistedTaskFailure {
                    code: TaskFailureCode::LeaseExpired,
                    failed_at: assignment.expires_at,
                    consecutive_failures,
                })),
            };
        }

        let failure = persisted_failure.map(TableTaskFailure::from);
        let state = if scheduled.eligible_at > now {
            if failure.is_some() {
                TableTaskState::Retrying
            } else {
                TableTaskState::Scheduled
            }
        } else {
            TableTaskState::Queued
        };

        Self {
            state,
            eligible_at: Some(scheduled.eligible_at),
            lease_expires_at: None,
            failure,
        }
    }
}

impl From<PersistedTaskFailure> for TableTaskFailure {
    fn from(failure: PersistedTaskFailure) -> Self {
        Self {
            code: failure.code,
            message: failure.code.message(),
            failed_at: failure.failed_at,
            consecutive_failures: failure.consecutive_failures,
        }
    }
}

async fn read_task_failure(
    snapshot: &DbSnapshot,
    table: &TableKey<'_>,
    kind: TaskKind,
) -> Result<Option<PersistedTaskFailure>> {
    snapshot
        .get(table.task_failure(kind))
        .await?
        .map(|bytes| decode(&bytes))
        .transpose()
}

fn task_health(status: &TableTaskStatus) -> TableHealth {
    let Some(failure) = &status.failure else {
        return TableHealth::Healthy;
    };

    if failure.code.is_non_transient()
        || failure.consecutive_failures >= TRANSIENT_FAILURE_THRESHOLD
    {
        TableHealth::Failing
    } else {
        TableHealth::Degraded
    }
}
