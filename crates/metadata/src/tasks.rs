use std::{
    collections::{BTreeMap, HashSet},
    ops::Bound,
    time::Duration,
};

use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use iceberg::{TableIdent, spec::DataFileFormat};
use se_common::id::IdGenerator;
use serde::{Deserialize, Serialize};
use slatedb::{DbTransaction, ErrorKind, IsolationLevel};
use tracing::info;
use uuid::Uuid;

use crate::{
    BatchMetadata, CatalogId, Error, Metadata, ReconciliationPlan, Result,
    encoding::{
        ReadyWorkKey, TableKey, TableRecordType, WalStateValue, WorkerAssignmentKey, decode, encode,
    },
};

pub const CHECKPOINT_INTERVAL_PROPERTY: &str = "airfoil.checkpoint.interval-sec";

pub(crate) const DEFAULT_CHECKPOINT_INTERVAL_SEC: u64 = 60;

const LEASE_DURATION: TimeDelta = TimeDelta::seconds(30);
const RETRY_DELAY: TimeDelta = TimeDelta::seconds(5);
const RECONCILIATION_DELAY: TimeDelta = TimeDelta::seconds(5);
const WAL_CLEANUP_DELAY: TimeDelta = TimeDelta::seconds(60);
const WAL_CLEANUP_BATCH_SIZE: usize = 128;
const RECEIPT_RETENTION: TimeDelta = TimeDelta::hours(24);
const RECEIPT_PRUNE_BATCH_SIZE: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalRange {
    pub start: u64,
    /// Exclusive upper bound.
    pub end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalDeletion {
    pub sequence: u64,
    pub metadata: BatchMetadata,
    pub queued_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataFileMetadata {
    pub path: String,
    pub format: DataFileFormat,
    pub size: u64,
    pub num_rows: u64,
    pub partition_spec_id: i32,
    /// Iceberg-encoded values keyed by partition field ID; None represents null.
    #[serde(deserialize_with = "deserialize_field_map")]
    pub partition: BTreeMap<i32, Option<Vec<u8>>>,
    pub sort_order_id: Option<i64>,
    #[serde(deserialize_with = "deserialize_field_map")]
    pub column_sizes: BTreeMap<i32, u64>,
    #[serde(deserialize_with = "deserialize_field_map")]
    pub value_counts: BTreeMap<i32, u64>,
    #[serde(deserialize_with = "deserialize_field_map")]
    pub null_value_counts: BTreeMap<i32, u64>,
    #[serde(deserialize_with = "deserialize_field_map")]
    pub nan_value_counts: BTreeMap<i32, u64>,
    /// Iceberg single-value binary encodings keyed by schema field ID.
    #[serde(deserialize_with = "deserialize_field_map")]
    pub lower_bounds: BTreeMap<i32, Vec<u8>>,
    #[serde(deserialize_with = "deserialize_field_map")]
    pub upper_bounds: BTreeMap<i32, Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletionVectorMetadata {
    pub puffin_path: String,
    pub target_file: String,
    pub cardinality: u64,
    pub file_size: u64,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletionVectorReplacement {
    pub previous: Option<DeletionVectorMetadata>,
    pub current: DeletionVectorMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializationResult {
    pub range: WalRange,
    pub schema_id: i32,
    #[serde(default)]
    pub index_delta: Option<Uuid>,
    pub data_files: Vec<DataFileMetadata>,
    /// Each current vector includes all earlier deletions for its target file.
    pub deletion_vectors: Vec<DeletionVectorReplacement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDelta {
    pub id: Uuid,
    pub range: WalRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializationInput {
    pub deltas: Vec<IndexDelta>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskToken {
    /// A unique worker process/session, not a reusable host name.
    pub worker_id: Uuid,
    pub catalog_id: CatalogId,
    pub table_uuid: Uuid,
    pub table_ident: TableIdent,
    pub attempt_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Task {
    DeleteTable,
    GarbageCollectWal {
        batches: Vec<WalDeletion>,
    },
    Materialize {
        epoch: u64,
        range: WalRange,
        schema_id: i32,
    },
    Reconcile {
        plan: ReconciliationPlan,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskAssignment {
    pub token: TaskToken,
    #[serde(default)]
    pub eligible_at: Option<DateTime<Utc>>,
    pub expires_at: DateTime<Utc>,
    pub task: Task,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAssignmentResponse {
    #[serde(flatten)]
    pub assignment: TaskAssignment,
    pub timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum TaskRequest {
    CompleteWalCleanup {
        token: TaskToken,
    },
    CompleteDeleteTable {
        token: TaskToken,
    },
    Claim {
        worker_id: Uuid,
    },
    Heartbeat {
        token: TaskToken,
    },
    CompleteMaterialization {
        token: TaskToken,
        result: MaterializationResult,
    },
    MaterializationInput {
        token: TaskToken,
        applied_through: u64,
    },
    ReconciliationInput {
        token: TaskToken,
    },
    CompleteReconciliation {
        token: TaskToken,
        commit: crate::PreparedCommit,
    },
    Fail {
        token: TaskToken,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TableWork {
    pub table_ident: TableIdent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletion: Option<Scheduled>,
    #[serde(default = "default_checkpoint_interval_sec")]
    pub checkpoint_interval_sec: u64,
    pub materialized_next: u64,
    #[serde(default)]
    pub desired_revision: u64,
    #[serde(default)]
    pub reconciled_revision: u64,
    #[serde(default)]
    pub reconciled_snapshot_id: Option<i64>,
    pub materialization: Option<Scheduled>,
    #[serde(default)]
    pub reconciliation: Option<Scheduled>,
    #[serde(default)]
    pub reconciliation_plan: Option<ReconciliationPlan>,
    pub wal_cleanup: Option<Scheduled>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Scheduled {
    pub eligible_at: DateTime<Utc>,
    pub assignment: Option<TaskAssignment>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Target {
    pub(crate) catalog_id: CatalogId,
    pub(crate) table_uuid: Uuid,
    #[serde(default)]
    pub(crate) kind: TaskKind,
}

#[derive(Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum TaskKind {
    #[default]
    Materialize,
    Reconcile,
    DeleteTable,
    GarbageCollectWal,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct MaterializationReceipt {
    pub(crate) token: TaskToken,
    pub(crate) result: MaterializationResult,
    pub(crate) completed_at: DateTime<Utc>,
}

pub(crate) struct WalCleanupReceipt {
    pub(crate) token: TaskToken,
    pub(crate) completed_at: DateTime<Utc>,
}

impl From<TaskAssignment> for TaskAssignmentResponse {
    fn from(assignment: TaskAssignment) -> Self {
        Self {
            assignment,
            // PANIC: The constant lease duration is positive and fits in Duration.
            timeout: LEASE_DURATION.to_std().expect("positive lease duration"),
        }
    }
}

impl Metadata {
    /// Claims eligible work, returning the same live assignment on repeated worker requests.
    pub async fn claim_task(&self, worker_id: Uuid) -> Result<Option<TaskAssignment>> {
        info!(worker.id = %worker_id, "claiming metadata task");

        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let now = self.clock.now();
            let worker_key = WorkerAssignmentKey { worker_id }.encode();
            if let Some(bytes) = tx.get(&worker_key).await? {
                let assignment: TaskAssignment = decode(&bytes)?;
                if assignment.expires_at > now {
                    // Assignment visibility alone does not make its deletion authority durable.
                    if matches!(assignment.task, Task::GarbageCollectWal { .. }) {
                        self.db.flush().await?;
                    }
                    return Ok(Some(assignment));
                }
            }

            let scan = ReadyWorkKey::scan();
            let mut ready = tx.scan_prefix(scan.prefix, scan.suffix).await?;
            let Some(entry) = ready.next().await? else {
                return Ok(None);
            };
            let target: Target = decode(&entry.value)?;
            let table = TableKey {
                catalog_id: &target.catalog_id,
                table_uuid: target.table_uuid,
            };
            let key = table.record(TableRecordType::TableWork);
            let mut work: TableWork = decode(&tx.get(&key).await?.ok_or(Error::InvalidTaskState)?)?;
            let scheduled = work.scheduled(target.kind).ok_or(Error::InvalidTaskState)?;
            if scheduled.eligible_at > now {
                return Ok(None);
            }
            let eligible_at = scheduled.eligible_at;

            if let Some(old) = &scheduled.assignment {
                let old_key = WorkerAssignmentKey::for_token(&old.token).encode();
                if let Some(bytes) = tx.get(&old_key).await? {
                    let worker: TaskAssignment = decode(&bytes)?;
                    if worker.token == old.token {
                        tx.delete(old_key)?;
                    }
                }
            }

            let task = match target.kind {
                TaskKind::DeleteTable => Task::DeleteTable,
                TaskKind::GarbageCollectWal => {
                    let scan = table.scan(TableRecordType::WalDeletion);
                    let mut scan = tx.scan_prefix(scan.prefix, scan.suffix).await?;
                    let mut batches = Vec::new();
                    while batches.len() < WAL_CLEANUP_BATCH_SIZE {
                        let Some(entry) = scan.next().await? else {
                            break;
                        };
                        batches.push(decode(&entry.value)?);
                    }
                    if batches.is_empty() {
                        return Err(Error::InvalidTaskState);
                    }
                    Task::GarbageCollectWal { batches }
                }
                TaskKind::Reconcile => {
                    let plan = work
                        .reconciliation_plan
                        .get_or_insert_with(|| ReconciliationPlan {
                            id: rand::rng().gen_uuid(),
                            base_revision: work.reconciled_revision,
                            target_revision: work.desired_revision,
                            base_snapshot_id: work.reconciled_snapshot_id,
                        });
                    Task::Reconcile { plan: plan.clone() }
                }
                TaskKind::Materialize => {
                    let state_key = table.record(TableRecordType::WalState);
                    let mut state: WalStateValue =
                        decode(&tx.get(&state_key).await?.ok_or(Error::InvalidTaskState)?)?;
                    state.materializer_epoch = state
                        .materializer_epoch
                        .checked_add(1)
                        .ok_or(Error::WalEpochExhausted)?;
                    tx.put(state_key, encode(&state)?)?;
                    let scan = table.wal_batches(work.materialized_next..);
                    let mut batches = tx.scan_prefix(scan.prefix, scan.suffix).await?;
                    let mut schema_id = None;
                    let mut end = work.materialized_next;
                    while let Some(batch) = batches.next().await? {
                        let descriptor: BatchMetadata = decode(&batch.value)?;
                        if schema_id.is_some_and(|id| id != descriptor.schema_id) {
                            break;
                        }
                        schema_id = Some(descriptor.schema_id);
                        end = end.checked_add(1).ok_or(Error::WalSequenceExhausted)?;
                    }
                    Task::Materialize {
                        epoch: state.materializer_epoch,
                        range: WalRange {
                            start: work.materialized_next,
                            end,
                        },
                        schema_id: schema_id.ok_or(Error::InvalidTaskState)?,
                    }
                }
            };
            let assignment = TaskAssignment {
                token: TaskToken {
                    worker_id,
                    catalog_id: target.catalog_id.clone(),
                    table_uuid: target.table_uuid,
                    table_ident: work.table_ident.clone(),
                    attempt_id: rand::rng().gen_uuid(),
                },
                eligible_at: Some(eligible_at),
                expires_at: now + LEASE_DURATION,
                task,
            };
            tx.delete(entry.key)?;
            work.set_scheduled(
                target.kind,
                Some(Scheduled {
                    eligible_at: assignment.expires_at,
                    assignment: Some(assignment.clone()),
                }),
            );
            tx.put(
                ReadyWorkKey {
                    target: &target,
                    at: assignment.expires_at,
                }
                .encode(),
                encode(&target)?,
            )?;
            tx.put(key, encode(&work)?)?;
            tx.put(worker_key, encode(&assignment)?)?;

            match tx.commit().await {
                Ok(handle) => {
                    if let Some(handle) = handle {
                        handle.await_durable().await?;
                    }
                    return Ok(Some(assignment));
                }
                Err(error) if error.kind() == ErrorKind::Transaction => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Removes deleted WAL candidates after the assigned batch succeeds, allowing exact retries.
    pub async fn complete_wal_cleanup(&self, token: &TaskToken) -> Result<()> {
        let table = TableKey::for_token(token);
        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let now = self.clock.now();
            if self.ignore_task_result(&tx, token).await? {
                return Ok(());
            }
            let receipt_key = table.wal_cleanup_receipt(token.attempt_id);
            if let Some(bytes) = tx.get(&receipt_key).await? {
                let receipt: WalCleanupReceipt = decode(&bytes)?;
                if receipt.token != *token {
                    return Err(Error::InvalidTaskResult);
                }
                if receipt_expired(receipt.completed_at, now) {
                    return Err(Error::TaskFenced);
                }
                self.db.flush().await?;
                return Ok(());
            }
            let (mut work, assignment) = self.active_assignment(&tx, token).await?;
            let Task::GarbageCollectWal { batches } = assignment.task else {
                return Err(Error::InvalidTaskResult);
            };
            self.prune_completion_receipts(&tx, token, now).await?;
            let target = Target {
                catalog_id: token.catalog_id.clone(),
                table_uuid: token.table_uuid,
                kind: TaskKind::GarbageCollectWal,
            };
            let scheduled = work.wal_cleanup.take().ok_or(Error::InvalidTaskState)?;
            tx.delete(
                ReadyWorkKey {
                    target: &target,
                    at: scheduled.eligible_at,
                }
                .encode(),
            )?;
            for batch in batches {
                tx.delete(table.wal_deletion(batch.sequence))?;
            }
            let scan = table.scan(TableRecordType::WalDeletion);
            let mut remaining = tx.scan_prefix(scan.prefix, scan.suffix).await?;
            if remaining.next().await?.is_some() {
                let eligible_at = self.clock.now();
                work.wal_cleanup = Some(Scheduled {
                    eligible_at,
                    assignment: None,
                });
                tx.put(
                    ReadyWorkKey {
                        target: &target,
                        at: eligible_at,
                    }
                    .encode(),
                    encode(&target)?,
                )?;
            }
            tx.put(table.record(TableRecordType::TableWork), encode(&work)?)?;
            tx.put(
                receipt_key,
                encode(&WalCleanupReceipt {
                    token: token.clone(),
                    completed_at: now,
                })?,
            )?;
            tx.delete(WorkerAssignmentKey::for_token(token).encode())?;
            match tx.commit().await {
                Ok(handle) => {
                    if let Some(handle) = handle {
                        handle.await_durable().await?;
                    }
                    return Ok(());
                }
                Err(error) if error.kind() == ErrorKind::Transaction => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Renews the current attempt without allowing an expired worker to regain ownership.
    pub async fn heartbeat_task(&self, token: &TaskToken) -> Result<TaskAssignment> {
        info!(catalog.id = %token.catalog_id, table.uuid = %token.table_uuid, "heartbeating metadata task");

        self.update_task(token, None, false)
            .await?
            .ok_or(Error::TaskFenced)
    }

    /// Atomically accepts files and index deltas with WAL acknowledgment, allowing exact retries.
    pub async fn complete_materialization(
        &self,
        token: &TaskToken,
        result: MaterializationResult,
    ) -> Result<()> {
        info!(catalog.id = %token.catalog_id, table.uuid = %token.table_uuid, "completing materialization");

        self.update_task(token, Some(result), false).await?;
        Ok(())
    }

    /// Returns accepted index deltas needed before the active materialization range.
    pub async fn materialization_input(
        &self,
        token: &TaskToken,
        applied_through: u64,
    ) -> Result<MaterializationInput> {
        info!(catalog.id = %token.catalog_id, table.uuid = %token.table_uuid, args.applied_through = applied_through, "reading materialization input");

        let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let (_, assignment) = self.active_assignment(&tx, token).await?;
        let Task::Materialize { range, .. } = assignment.task else {
            return Err(Error::InvalidTaskResult);
        };
        if applied_through > range.start {
            return Err(Error::InvalidTaskResult);
        }
        let scan = TableKey::for_token(token).index_deltas_before(range.start);
        let mut scan = tx.scan_prefix(scan.prefix, scan.suffix).await?;
        let mut deltas = Vec::new();
        while let Some(entry) = scan.next().await? {
            let delta: IndexDelta = decode(&entry.value)?;
            if delta.range.start < applied_through && applied_through < delta.range.end {
                return Err(Error::InvalidTaskResult);
            }
            if delta.range.end > range.start {
                return Err(Error::InvalidTaskState);
            }
            if delta.range.end > applied_through {
                deltas.push(delta);
            }
        }
        // A concurrent completion may be visible before its durability wait finishes.
        self.db.flush().await?;
        Ok(MaterializationInput { deltas })
    }

    /// Releases a failed attempt and makes its work eligible after a short retry delay.
    pub async fn fail_task(&self, token: &TaskToken) -> Result<()> {
        info!(catalog.id = %token.catalog_id, table.uuid = %token.table_uuid, "failing metadata task");

        self.update_task(token, None, true).await?;
        Ok(())
    }

    async fn update_task(
        &self,
        token: &TaskToken,
        result: Option<MaterializationResult>,
        failed: bool,
    ) -> Result<Option<TaskAssignment>> {
        let table = TableKey::for_token(token);
        let key = table.record(TableRecordType::TableWork);
        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let now = self.clock.now();
            if (result.is_some() || failed) && self.ignore_task_result(&tx, token).await? {
                return Ok(None);
            }
            if let Some(result) = &result {
                validate_result(result)?;
                if result.index_delta.is_some_and(|id| id != token.attempt_id) {
                    return Err(Error::InvalidTaskResult);
                }
            }
            let receipt_key = table.materialization_receipt(token.attempt_id);
            if let Some(result) = &result
                && let Some(bytes) = tx.get(&receipt_key).await?
            {
                let receipt: MaterializationReceipt = decode(&bytes)?;
                if receipt.token != *token || receipt.result != *result {
                    return Err(Error::InvalidTaskResult);
                }
                if receipt_expired(receipt.completed_at, now) {
                    return Err(Error::TaskFenced);
                }
                // Reading an in-memory receipt must not authorize index application early.
                self.db.flush().await?;
                return Ok(None);
            }
            let (mut work, mut assignment) = self.active_assignment(&tx, token).await?;
            let kind = assignment.task.kind();
            let target = Target {
                catalog_id: token.catalog_id.clone(),
                table_uuid: token.table_uuid,
                kind,
            };
            let scheduled = work.scheduled(kind).ok_or(Error::TaskFenced)?;
            tx.delete(
                ReadyWorkKey {
                    target: &target,
                    at: scheduled.eligible_at,
                }
                .encode(),
            )?;

            if failed {
                work.set_scheduled(
                    kind,
                    Some(Scheduled {
                        eligible_at: now + RETRY_DELAY,
                        assignment: None,
                    }),
                );
                tx.delete(WorkerAssignmentKey::for_token(token).encode())?;
            } else if let Some(result) = &result {
                let Task::Materialize {
                    range, schema_id, ..
                } = assignment.task
                else {
                    return Err(Error::InvalidTaskResult);
                };
                if result.range != range || result.schema_id != schema_id {
                    return Err(Error::InvalidTaskResult);
                }
                self.prune_completion_receipts(&tx, token, now).await?;
                let revision = work
                    .desired_revision
                    .checked_add(1)
                    .ok_or(Error::TableRevisionExhausted)?;
                crate::files::apply_materialization(&tx, token, result, revision).await?;
                tx.put(
                    receipt_key,
                    encode(&MaterializationReceipt {
                        token: token.clone(),
                        result: result.clone(),
                        completed_at: now,
                    })?,
                )?;
                if let Some(id) = result.index_delta {
                    let delta_key = table.index_delta(range.start);
                    // TODO: prune accepted references after durable application acknowledgment.
                    tx.put(delta_key, encode(&IndexDelta { id, range })?)?;
                }
                work.desired_revision = revision;
                work.enqueue_reconciliation(&tx, token, now)?;
                let scan = table.wal_batches(range.start..range.end);
                let mut batches = tx.scan_prefix(scan.prefix, scan.suffix).await?;
                while let Some(batch) = batches.next().await? {
                    let sequence = table.wal_sequence(&batch.key)?;
                    tx.put(
                        table.wal_deletion(sequence),
                        encode(&WalDeletion {
                            sequence,
                            metadata: decode(&batch.value)?,
                            queued_at: now,
                        })?,
                    )?;
                    tx.delete(batch.key)?;
                }
                if work.wal_cleanup.is_none() {
                    let eligible_at = now + WAL_CLEANUP_DELAY;
                    let target = Target {
                        catalog_id: token.catalog_id.clone(),
                        table_uuid: token.table_uuid,
                        kind: TaskKind::GarbageCollectWal,
                    };
                    work.wal_cleanup = Some(Scheduled {
                        eligible_at,
                        assignment: None,
                    });
                    tx.put(
                        ReadyWorkKey {
                            target: &target,
                            at: eligible_at,
                        }
                        .encode(),
                        encode(&target)?,
                    )?;
                }
                work.materialized_next = range.end;
                let state: WalStateValue = decode(
                    &tx.get(table.record(TableRecordType::WalState))
                        .await?
                        .ok_or(Error::InvalidTaskState)?,
                )?;
                work.materialization = if range.end < state.next_sequence {
                    Some(Scheduled {
                        eligible_at: work.checkpoint_eligible_at(now)?,
                        assignment: None,
                    })
                } else {
                    None
                };
                tx.delete(WorkerAssignmentKey::for_token(token).encode())?;
            } else {
                assignment.expires_at = now + LEASE_DURATION;
                work.set_scheduled(
                    kind,
                    Some(Scheduled {
                        eligible_at: assignment.expires_at,
                        assignment: Some(assignment.clone()),
                    }),
                );
                tx.put(
                    WorkerAssignmentKey::for_token(token).encode(),
                    encode(&assignment)?,
                )?;
            }
            if let Some(scheduled) = work.scheduled(kind) {
                tx.put(
                    ReadyWorkKey {
                        target: &target,
                        at: scheduled.eligible_at,
                    }
                    .encode(),
                    encode(&target)?,
                )?;
            }
            tx.put(key.clone(), encode(&work)?)?;
            match tx.commit().await {
                Ok(handle) => {
                    if let Some(handle) = handle {
                        handle.await_durable().await?;
                    }
                    return Ok((result.is_none() && !failed).then_some(assignment));
                }
                Err(error) if error.kind() == ErrorKind::Transaction => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Prunes a bounded page of expired receipts from each family atomically.
    pub(crate) async fn prune_completion_receipts(
        &self,
        tx: &DbTransaction,
        token: &TaskToken,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let table = TableKey::for_token(token);
        for kind in [
            TableRecordType::MaterializationReceipt,
            TableRecordType::WalCleanupReceipt,
        ] {
            let cursor_key = table.receipt_prune_cursor(kind);
            let mut scan = table.scan(kind);
            if let Some(cursor) = tx.get(&cursor_key).await? {
                scan.suffix.0 = Bound::Excluded(cursor);
            }
            let prefix_len = scan.prefix.len();
            let mut entries = tx.scan_prefix(scan.prefix, scan.suffix).await?;
            let mut cursor = None;
            for _ in 0..RECEIPT_PRUNE_BATCH_SIZE {
                let Some(entry) = entries.next().await? else {
                    cursor = None;
                    break;
                };
                cursor = Some(entry.key.slice(prefix_len..));
                let completed_at = if kind == TableRecordType::MaterializationReceipt {
                    decode::<MaterializationReceipt>(&entry.value)?.completed_at
                } else {
                    decode::<WalCleanupReceipt>(&entry.value)?.completed_at
                };
                if receipt_expired(completed_at, now) {
                    tx.delete(entry.key)?;
                }
            }
            if let Some(cursor) = cursor {
                tx.put(cursor_key, cursor)?;
            } else {
                tx.delete(cursor_key)?;
            }
        }
        Ok(())
    }

    pub(crate) async fn active_assignment(
        &self,
        tx: &DbTransaction,
        token: &TaskToken,
    ) -> Result<(TableWork, TaskAssignment)> {
        let work: TableWork = decode(
            &tx.get(TableKey::for_token(token).record(TableRecordType::TableWork))
                .await?
                .ok_or(Error::TaskFenced)?,
        )?;
        let worker: TaskAssignment = decode(
            &tx.get(WorkerAssignmentKey::for_token(token).encode())
                .await?
                .ok_or(Error::TaskFenced)?,
        )?;
        let assignment = work
            .scheduled(worker.task.kind())
            .and_then(|scheduled| scheduled.assignment.as_ref())
            .ok_or(Error::TaskFenced)?;
        if assignment != &worker || worker.token != *token || worker.expires_at <= self.clock.now()
        {
            return Err(Error::TaskFenced);
        }
        Ok((work, worker))
    }

    pub(crate) async fn ignore_task_result(
        &self,
        tx: &DbTransaction,
        token: &TaskToken,
    ) -> Result<bool> {
        let Some(bytes) = tx
            .get(TableKey::for_token(token).record(TableRecordType::TableWork))
            .await?
        else {
            self.db.flush().await?;
            return Ok(true);
        };
        let work: TableWork = decode(&bytes)?;
        let ignored = work.deletion.as_ref().is_some_and(|scheduled| {
            scheduled
                .assignment
                .as_ref()
                .map(|assignment| &assignment.token)
                != Some(token)
        });
        if ignored {
            self.db.flush().await?;
        }
        Ok(ignored)
    }
}

impl Task {
    /// Returns the task kind used in tracing fields.
    pub fn task_kind(&self) -> &'static str {
        match self {
            Self::DeleteTable => "delete_table",
            Self::GarbageCollectWal { .. } => "garbage_collect_wal",
            Self::Materialize { .. } => "materialize",
            Self::Reconcile { .. } => "reconcile",
        }
    }

    pub(crate) fn kind(&self) -> TaskKind {
        match self {
            Self::DeleteTable => TaskKind::DeleteTable,
            Self::GarbageCollectWal { .. } => TaskKind::GarbageCollectWal,
            Self::Materialize { .. } => TaskKind::Materialize,
            Self::Reconcile { .. } => TaskKind::Reconcile,
        }
    }
}

impl TableWork {
    pub(crate) fn new(table_ident: TableIdent) -> Self {
        Self {
            table_ident,
            deletion: None,
            checkpoint_interval_sec: default_checkpoint_interval_sec(),
            materialized_next: 0,
            desired_revision: 0,
            reconciled_revision: 0,
            reconciled_snapshot_id: None,
            materialization: None,
            reconciliation: None,
            reconciliation_plan: None,
            wal_cleanup: None,
        }
    }

    pub(crate) fn enqueue(
        &mut self,
        catalog_id: &CatalogId,
        table_uuid: Uuid,
        now: DateTime<Utc>,
    ) -> Result<Option<(Bytes, Bytes)>> {
        if self.materialization.is_some() {
            return Ok(None);
        }
        let eligible_at = self.checkpoint_eligible_at(now)?;
        self.materialization = Some(Scheduled {
            eligible_at,
            assignment: None,
        });
        let target = Target {
            catalog_id: catalog_id.clone(),
            table_uuid,
            kind: TaskKind::Materialize,
        };
        Ok(Some((
            ReadyWorkKey {
                target: &target,
                at: eligible_at,
            }
            .encode(),
            encode(&target)?,
        )))
    }

    pub(crate) fn checkpoint_eligible_at(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
        i64::try_from(self.checkpoint_interval_sec)
            .ok()
            .and_then(TimeDelta::try_seconds)
            .and_then(|interval| now.checked_add_signed(interval))
            .ok_or_else(|| {
                Error::InvalidCheckpointInterval(self.checkpoint_interval_sec.to_string())
            })
    }

    pub(crate) fn scheduled(&self, kind: TaskKind) -> Option<&Scheduled> {
        match kind {
            TaskKind::DeleteTable => self.deletion.as_ref(),
            TaskKind::GarbageCollectWal => self.wal_cleanup.as_ref(),
            TaskKind::Materialize => self.materialization.as_ref(),
            TaskKind::Reconcile => self.reconciliation.as_ref(),
        }
    }

    fn set_scheduled(&mut self, kind: TaskKind, scheduled: Option<Scheduled>) {
        match kind {
            TaskKind::DeleteTable => self.deletion = scheduled,
            TaskKind::GarbageCollectWal => self.wal_cleanup = scheduled,
            TaskKind::Materialize => self.materialization = scheduled,
            TaskKind::Reconcile => self.reconciliation = scheduled,
        }
    }

    pub(crate) fn enqueue_reconciliation(
        &mut self,
        tx: &DbTransaction,
        token: &TaskToken,
        now: DateTime<Utc>,
    ) -> Result<()> {
        if self.reconciliation.is_none() && self.desired_revision > self.reconciled_revision {
            let eligible_at = now + RECONCILIATION_DELAY;
            let target = Target {
                catalog_id: token.catalog_id.clone(),
                table_uuid: token.table_uuid,
                kind: TaskKind::Reconcile,
            };
            self.reconciliation = Some(Scheduled {
                eligible_at,
                assignment: None,
            });
            tx.put(
                ReadyWorkKey {
                    target: &target,
                    at: eligible_at,
                }
                .encode(),
                encode(&target)?,
            )?;
        }
        Ok(())
    }
}

fn receipt_expired(completed_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    now - completed_at >= RECEIPT_RETENTION
}

fn default_checkpoint_interval_sec() -> u64 {
    DEFAULT_CHECKPOINT_INTERVAL_SEC
}

// Tagged enums buffer JSON objects, losing serde_json's numeric map-key conversion.
fn deserialize_field_map<'de, D, T>(
    deserializer: D,
) -> std::result::Result<BTreeMap<i32, T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    BTreeMap::<String, T>::deserialize(deserializer)?
        .into_iter()
        .map(|(field, value)| Ok((field.parse().map_err(serde::de::Error::custom)?, value)))
        .collect()
}

fn validate_result(result: &MaterializationResult) -> Result<()> {
    let mut paths = HashSet::new();
    let mut targets = HashSet::new();
    if result.range.start >= result.range.end
        || result
            .data_files
            .iter()
            .any(|file| file.path.is_empty() || !paths.insert(&file.path))
        || result.deletion_vectors.iter().any(|replacement| {
            let vector = &replacement.current;
            vector.puffin_path.is_empty()
                || vector.target_file.is_empty()
                || !targets.insert(&vector.target_file)
                || vector.length == 0
                || vector
                    .offset
                    .checked_add(vector.length)
                    .is_none_or(|end| end > vector.file_size)
                || replacement
                    .previous
                    .as_ref()
                    .is_some_and(|previous| previous.target_file != vector.target_file)
        })
    {
        return Err(Error::InvalidTaskResult);
    }
    Ok(())
}

