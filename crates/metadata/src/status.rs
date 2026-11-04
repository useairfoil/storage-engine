use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    BatchMetadata, CatalogId, Error, Metadata, Result, Scheduled, TableWork,
    encoding::{RecordType, TableKey, WalStateValue, decode},
};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableStatus {
    pub table_uuid: Uuid,
    pub observed_at: DateTime<Utc>,
    pub conditions: Vec<TableCondition>,
    pub wal: TableWalStatus,
    pub iceberg: TableIcebergStatus,
    pub work: TableWorkStatus,
}

#[derive(Serialize)]
pub struct TableCondition {
    pub r#type: &'static str,
    pub status: &'static str,
    pub reason: &'static str,
    pub message: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableWalStatus {
    pub accepted_next_sequence: u64,
    pub materialized_next_sequence: u64,
    pub pending_batches: u64,
    pub pending_bytes: u64,
    pub oldest_pending_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableIcebergStatus {
    pub desired_revision: u64,
    pub reconciled_revision: u64,
    pub reconciled_snapshot_id: Option<i64>,
}

#[derive(Serialize)]
pub struct TableWorkStatus {
    pub materialization: TableTaskStatus,
    pub reconciliation: TableTaskStatus,
    pub deletion: TableTaskStatus,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableTaskStatus {
    pub state: TableTaskState,
    pub eligible_at: Option<DateTime<Utc>>,
    pub lease_expires_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Serialize)]
pub enum TableTaskState {
    Idle,
    Scheduled,
    Queued,
    Assigned,
}

impl Metadata {
    /// Reads a consistent summary of table progress without changing its state.
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
                .get(table.record(RecordType::TableWork))
                .await?
                .ok_or(Error::TableNotResolved(table_uuid))?,
        )?;
        let wal_state: WalStateValue = snapshot
            .get(table.record(RecordType::WalState))
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

        let task_work = TableWorkStatus {
            materialization: TableTaskStatus::new(work.materialization.as_ref(), now),
            reconciliation: TableTaskStatus::new(work.reconciliation.as_ref(), now),
            deletion: TableTaskStatus::new(work.deletion.as_ref(), now),
        };
        let deleting = work.deletion.is_some();
        let mut conditions = vec![
            progress_condition(
                "Materialized",
                work.materialized_next == wal_state.next_sequence,
                deleting,
                task_work.materialization.state,
            ),
            progress_condition(
                "Reconciled",
                work.reconciled_revision == work.desired_revision,
                deleting,
                task_work.reconciliation.state,
            ),
        ];
        let (reason, message) = match task_work.deletion.state {
            TableTaskState::Idle => ("NotRequested", "Table deletion has not been requested."),
            TableTaskState::Scheduled => {
                ("Scheduled", "Table cleanup is scheduled for a later time.")
            }
            TableTaskState::Queued => ("DeletionRequested", "Table cleanup is queued."),
            TableTaskState::Assigned => {
                ("CleanupRunning", "Table cleanup is assigned to a worker.")
            }
        };
        conditions.push(TableCondition {
            r#type: "Deleting",
            status: if deleting { "True" } else { "False" },
            reason,
            message: message.into(),
        });

        Ok(TableStatus {
            table_uuid,
            observed_at: now,
            conditions,
            wal,
            iceberg: TableIcebergStatus {
                desired_revision: work.desired_revision,
                reconciled_revision: work.reconciled_revision,
                reconciled_snapshot_id: work.reconciled_snapshot_id,
            },
            work: task_work,
        })
    }
}

impl TableTaskStatus {
    fn new(scheduled: Option<&Scheduled>, now: DateTime<Utc>) -> Self {
        let Some(scheduled) = scheduled else {
            return Self {
                state: TableTaskState::Idle,
                eligible_at: None,
                lease_expires_at: None,
            };
        };
        let lease_expires_at = scheduled
            .assignment
            .as_ref()
            .map(|assignment| assignment.expires_at);
        let state = if lease_expires_at.is_some_and(|expires_at| expires_at > now) {
            TableTaskState::Assigned
        } else if scheduled.eligible_at > now {
            TableTaskState::Scheduled
        } else {
            TableTaskState::Queued
        };

        Self {
            state,
            eligible_at: Some(scheduled.eligible_at),
            lease_expires_at,
        }
    }
}

fn progress_condition(
    kind: &'static str,
    caught_up: bool,
    deleting: bool,
    state: TableTaskState,
) -> TableCondition {
    let (reason, message) = if caught_up {
        ("CaughtUp", "All pending changes have been processed.")
    } else if deleting {
        (
            "Deleting",
            "Pending changes will not be processed because the table is being deleted.",
        )
    } else {
        match state {
            TableTaskState::Scheduled => {
                ("Scheduled", "Pending changes are scheduled for processing.")
            }
            TableTaskState::Queued => ("Queued", "Pending changes are waiting for a worker."),
            TableTaskState::Assigned => ("Running", "Pending changes are assigned to a worker."),
            TableTaskState::Idle => ("Pending", "Changes have not yet been processed."),
        }
    };

    TableCondition {
        r#type: kind,
        status: if caught_up { "True" } else { "False" },
        reason,
        message: message.into(),
    }
}
