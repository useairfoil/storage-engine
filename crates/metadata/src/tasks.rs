use std::collections::{BTreeMap, HashSet};

use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use iceberg::{TableIdent, spec::DataFileFormat};
use se_common::id::IdGenerator;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use slatedb::{DbTransaction, ErrorKind, IsolationLevel};
use uuid::Uuid;

use crate::{
    CatalogId, Error, Metadata, ReconciliationPlan, Result,
    encoding::{RecordType, WalBatchKey, WalStateKey, WalStateValue},
};

const LEASE_DURATION: TimeDelta = TimeDelta::seconds(30);
const RETRY_DELAY: TimeDelta = TimeDelta::seconds(5);
const RECONCILIATION_DELAY: TimeDelta = TimeDelta::seconds(5);
const READY_PREFIX: &[u8] = b"\x01\x05";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalRange {
    pub start: u64,
    /// Exclusive upper bound.
    pub end: u64,
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
    pub data_files: Vec<DataFileMetadata>,
    /// Each current vector includes all earlier deletions for its target file.
    pub deletion_vectors: Vec<DeletionVectorReplacement>,
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
    pub expires_at: DateTime<Utc>,
    pub task: Task,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum TaskRequest {
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
pub(crate) struct TableWork {
    pub(crate) table_ident: TableIdent,
    materialized_next: u64,
    #[serde(default)]
    pub(crate) desired_revision: u64,
    #[serde(default)]
    pub(crate) reconciled_revision: u64,
    #[serde(default)]
    pub(crate) reconciled_snapshot_id: Option<i64>,
    materialization: Option<Scheduled>,
    #[serde(default)]
    pub(crate) reconciliation: Option<Scheduled>,
    #[serde(default)]
    pub(crate) reconciliation_plan: Option<ReconciliationPlan>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Scheduled {
    pub(crate) eligible_at: DateTime<Utc>,
    pub(crate) assignment: Option<TaskAssignment>,
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
}

impl Metadata {
    /// Claims eligible work, returning the same live assignment on repeated worker requests.
    pub async fn claim_task(&self, worker_id: Uuid) -> Result<Option<TaskAssignment>> {
        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let now = self.clock.now();
            let worker_key = worker_key(worker_id);
            if let Some(bytes) = tx.get(&worker_key).await? {
                let assignment: TaskAssignment = decode(&bytes)?;
                if assignment.expires_at > now {
                    return Ok(Some(assignment));
                }
            }

            let mut ready = tx.scan_prefix(READY_PREFIX, ..).await?;
            let Some(entry) = ready.next().await? else {
                return Ok(None);
            };
            let target: Target = decode(&entry.value)?;
            let key = RecordType::TableWork
                .table_key(&target.catalog_id, target.table_uuid)
                .freeze();
            let mut work: TableWork = decode(&tx.get(&key).await?.ok_or(Error::InvalidTaskState)?)?;
            let scheduled = work.scheduled(target.kind).ok_or(Error::InvalidTaskState)?;
            if scheduled.eligible_at > now {
                return Ok(None);
            }

            if let Some(old) = &scheduled.assignment {
                let old_key = worker_key_for(&old.token);
                if let Some(bytes) = tx.get(&old_key).await? {
                    let worker: TaskAssignment = decode(&bytes)?;
                    if worker.token == old.token {
                        tx.delete(old_key)?;
                    }
                }
            }

            let task = match target.kind {
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
                    let state_key = WalStateKey {
                        catalog_id: &target.catalog_id,
                        table_uuid: target.table_uuid,
                    }
                    .encode();
                    let mut state = WalStateValue::decode(
                        &tx.get(&state_key).await?.ok_or(Error::InvalidTaskState)?,
                    )?;
                    state.materializer_epoch = state
                        .materializer_epoch
                        .checked_add(1)
                        .ok_or(Error::WalEpochExhausted)?;
                    tx.put(state_key, state.encode())?;
                    let prefix = WalBatchKey {
                        catalog_id: &target.catalog_id,
                        table_uuid: target.table_uuid,
                        sequence: 0,
                    }
                    .prefix();
                    let mut batches = tx
                        .scan_prefix(&prefix, work.materialized_next.to_be_bytes()..)
                        .await?;
                    let mut schema_id = None;
                    let mut end = work.materialized_next;
                    while let Some(batch) = batches.next().await? {
                        let descriptor = crate::encoding::WalBatchValue::decode(&batch.value)?;
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
            tx.put(ready_key(&target, assignment.expires_at), encode(&target)?)?;
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

    /// Renews the current attempt without allowing an expired worker to regain ownership.
    pub async fn heartbeat_task(&self, token: &TaskToken) -> Result<TaskAssignment> {
        self.update_task(token, None, false)
            .await?
            .ok_or(Error::TaskFenced)
    }

    /// Atomically persists desired file changes and acknowledges the assigned WAL range.
    pub async fn complete_materialization(
        &self,
        token: &TaskToken,
        result: MaterializationResult,
    ) -> Result<()> {
        validate_result(&result)?;
        self.update_task(token, Some(result), false).await?;
        Ok(())
    }

    /// Releases a failed attempt and makes its work eligible after a short retry delay.
    pub async fn fail_task(&self, token: &TaskToken) -> Result<()> {
        self.update_task(token, None, true).await?;
        Ok(())
    }

    async fn update_task(
        &self,
        token: &TaskToken,
        result: Option<MaterializationResult>,
        failed: bool,
    ) -> Result<Option<TaskAssignment>> {
        let key = RecordType::TableWork
            .table_key(&token.catalog_id, token.table_uuid)
            .freeze();
        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let now = self.clock.now();
            let (mut work, mut assignment) = self.active_assignment(&tx, token).await?;
            let kind = assignment.task.kind();
            let target = Target {
                catalog_id: token.catalog_id.clone(),
                table_uuid: token.table_uuid,
                kind,
            };
            let scheduled = work.scheduled(kind).ok_or(Error::TaskFenced)?;
            tx.delete(ready_key(&target, scheduled.eligible_at))?;

            if failed {
                work.set_scheduled(
                    kind,
                    Some(Scheduled {
                        eligible_at: now + RETRY_DELAY,
                        assignment: None,
                    }),
                );
                tx.delete(worker_key_for(token))?;
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
                let revision = work
                    .desired_revision
                    .checked_add(1)
                    .ok_or(Error::TableRevisionExhausted)?;
                crate::files::apply_materialization(&tx, token, result, revision).await?;
                work.desired_revision = revision;
                work.enqueue_reconciliation(&tx, token, now)?;
                let prefix = WalBatchKey {
                    catalog_id: &token.catalog_id,
                    table_uuid: token.table_uuid,
                    sequence: 0,
                }
                .prefix();
                let mut batches = tx
                    .scan_prefix(prefix, range.start.to_be_bytes()..range.end.to_be_bytes())
                    .await?;
                while let Some(batch) = batches.next().await? {
                    tx.delete(batch.key)?;
                }
                work.materialized_next = range.end;
                let state = WalStateValue::decode(
                    &tx.get(
                        WalStateKey {
                            catalog_id: &token.catalog_id,
                            table_uuid: token.table_uuid,
                        }
                        .encode(),
                    )
                    .await?
                    .ok_or(Error::InvalidTaskState)?,
                )?;
                work.materialization = (range.end < state.next_sequence).then_some(Scheduled {
                    eligible_at: now,
                    assignment: None,
                });
                tx.delete(worker_key_for(token))?;
            } else {
                assignment.expires_at = now + LEASE_DURATION;
                work.set_scheduled(
                    kind,
                    Some(Scheduled {
                        eligible_at: assignment.expires_at,
                        assignment: Some(assignment.clone()),
                    }),
                );
                tx.put(worker_key_for(token), encode(&assignment)?)?;
            }
            if let Some(scheduled) = work.scheduled(kind) {
                tx.put(ready_key(&target, scheduled.eligible_at), encode(&target)?)?;
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

    pub(crate) async fn active_assignment(
        &self,
        tx: &DbTransaction,
        token: &TaskToken,
    ) -> Result<(TableWork, TaskAssignment)> {
        let work: TableWork = decode(
            &tx.get(RecordType::TableWork.table_key(&token.catalog_id, token.table_uuid))
                .await?
                .ok_or(Error::TaskFenced)?,
        )?;
        let worker: TaskAssignment = decode(
            &tx.get(worker_key_for(token))
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
}

impl Task {
    pub(crate) fn kind(&self) -> TaskKind {
        match self {
            Self::Materialize { .. } => TaskKind::Materialize,
            Self::Reconcile { .. } => TaskKind::Reconcile,
        }
    }
}

impl TableWork {
    pub(crate) fn new(table_ident: TableIdent) -> Self {
        Self {
            table_ident,
            materialized_next: 0,
            desired_revision: 0,
            reconciled_revision: 0,
            reconciled_snapshot_id: None,
            materialization: None,
            reconciliation: None,
            reconciliation_plan: None,
        }
    }

    pub(crate) fn enqueue(
        &mut self,
        catalog_id: &CatalogId,
        table_uuid: Uuid,
        now: DateTime<Utc>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.materialization.is_some() {
            return Ok(None);
        }
        self.materialization = Some(Scheduled {
            eligible_at: now,
            assignment: None,
        });
        let target = Target {
            catalog_id: catalog_id.clone(),
            table_uuid,
            kind: TaskKind::Materialize,
        };
        Ok(Some((ready_key(&target, now), encode(&target)?)))
    }

    pub(crate) fn scheduled(&self, kind: TaskKind) -> Option<&Scheduled> {
        match kind {
            TaskKind::Materialize => self.materialization.as_ref(),
            TaskKind::Reconcile => self.reconciliation.as_ref(),
        }
    }

    fn set_scheduled(&mut self, kind: TaskKind, scheduled: Option<Scheduled>) {
        match kind {
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
            tx.put(ready_key(&target, eligible_at), encode(&target)?)?;
        }
        Ok(())
    }
}

pub(crate) fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(Error::TaskEncoding)
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(Error::TaskEncoding)
}

fn worker_key(worker_id: Uuid) -> Bytes {
    Bytes::from([b"\x01\x07".as_slice(), worker_id.as_bytes()].concat())
}

pub(crate) fn worker_key_for(token: &TaskToken) -> Bytes {
    worker_key(token.worker_id)
}

pub(crate) fn ready_key(target: &Target, at: DateTime<Utc>) -> Vec<u8> {
    let time = (at.timestamp() as u64) ^ (1 << 63);
    let mut key = [
        READY_PREFIX,
        &time.to_be_bytes(),
        &at.timestamp_subsec_nanos().to_be_bytes(),
        &RecordType::TableWork.table_key(&target.catalog_id, target.table_uuid),
    ]
    .concat();
    if target.kind == TaskKind::Reconcile {
        key.push(1);
    }
    key
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
