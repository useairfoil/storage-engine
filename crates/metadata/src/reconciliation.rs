use std::sync::Arc;

use iceberg::{
    TableCommit, TableRequirement, TableUpdate,
    spec::{MAIN_BRANCH, Operation, Snapshot},
};
use se_iceberg::storage::ObjectStoreStorageFactory;
use serde::{Deserialize, Serialize};
use slatedb::{ErrorKind, IsolationLevel};
use uuid::Uuid;

use crate::{
    Error, Metadata, Result, Task, TaskToken, VersionedDataFile, VersionedDeletionVector,
    encoding::RecordType,
    tasks::{TableWork, Target, TaskKind, decode, encode, ready_key, worker_key_for},
};

pub const RECONCILIATION_ID_PROPERTY: &str = "se.reconciliation-id";
pub const REVISION_PROPERTY: &str = "se.revision";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationPlan {
    pub id: Uuid,
    pub base_revision: u64,
    pub target_revision: u64,
    pub base_snapshot_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedCommit {
    pub updates: Vec<TableUpdate>,
    pub requirements: Vec<TableRequirement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationInput {
    pub plan: ReconciliationPlan,
    pub files: Vec<VersionedDataFile>,
    pub prepared: Option<PreparedCommit>,
}

impl Metadata {
    /// Reads the fixed append-only reconciliation input and any durable prepared commit.
    pub async fn reconciliation_input(&self, token: &TaskToken) -> Result<ReconciliationInput> {
        let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let (work, assignment) = self.active_assignment(&tx, token).await?;
        let Task::Reconcile { plan } = assignment.task else {
            return Err(Error::InvalidTaskResult);
        };
        if work.reconciliation_plan.as_ref() != Some(&plan) {
            return Err(Error::InvalidTaskState);
        }
        let mut files = Vec::new();
        let mut scan = tx
            .scan_prefix(
                RecordType::DataFile
                    .table_key(&token.catalog_id, token.table_uuid)
                    .freeze(),
                ..,
            )
            .await?;
        while let Some(entry) = scan.next().await? {
            let file: VersionedDataFile = decode(&entry.value)?;
            if file.added_revision > plan.target_revision {
                continue;
            }
            if file.removed_revision.is_some_and(|revision| {
                revision > plan.base_revision && revision <= plan.target_revision
            }) {
                return Err(Error::UnsupportedReconciliation);
            }
            if file.added_revision > plan.base_revision
                && file
                    .removed_revision
                    .is_none_or(|revision| revision > plan.target_revision)
            {
                files.push(file);
            }
        }
        let mut scan = tx
            .scan_prefix(
                RecordType::DeletionVector
                    .table_key(&token.catalog_id, token.table_uuid)
                    .freeze(),
                ..,
            )
            .await?;
        while let Some(entry) = scan.next().await? {
            let vector: VersionedDeletionVector = decode(&entry.value)?;
            if vector.added_revision <= plan.target_revision
                && vector
                    .removed_revision
                    .is_none_or(|revision| revision > plan.base_revision)
            {
                return Err(Error::UnsupportedReconciliation);
            }
        }
        let prepared = tx
            .get(RecordType::PreparedCommit.table_key(&token.catalog_id, token.table_uuid))
            .await?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        Ok(ReconciliationInput {
            plan,
            files,
            prepared,
        })
    }

    /// Durably fixes a prepared append, submits it without rebasing, and confirms its revision.
    pub async fn complete_reconciliation(
        &self,
        token: &TaskToken,
        commit: PreparedCommit,
    ) -> Result<()> {
        let input = self.reconciliation_input(token).await?;
        let snapshot = validate_commit(token, &input.plan, &commit)?;
        let snapshot_id = snapshot.snapshot_id();
        let prepared_key = RecordType::PreparedCommit
            .table_key(&token.catalog_id, token.table_uuid)
            .freeze();
        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let (work, _) = self.active_assignment(&tx, token).await?;
            if work.reconciliation_plan.as_ref() != Some(&input.plan) {
                return Err(Error::TaskFenced);
            }
            if let Some(bytes) = tx.get(&prepared_key).await? {
                let prepared: PreparedCommit = decode(&bytes)?;
                if prepared != commit {
                    return Err(Error::InvalidPreparedCommit);
                }
                break;
            }
            tx.put(prepared_key.clone(), encode(&commit)?)?;
            match tx.commit().await {
                Ok(handle) => {
                    if let Some(handle) = handle {
                        handle.await_durable().await?;
                    }
                    break;
                }
                Err(error) if error.kind() == ErrorKind::Transaction => continue,
                Err(error) => return Err(error.into()),
            }
        }

        // A timeout may leave this request running. Every retry must use this exact payload/base.
        let config = self
            .get_catalog(&token.catalog_id)
            .await?
            .ok_or_else(|| Error::CatalogNotFound(token.catalog_id.clone()))?;
        let catalog = config
            .to_catalog(&token.catalog_id, Arc::new(ObjectStoreStorageFactory))
            .await?;
        let mut table = catalog.load_table(&token.table_ident).await?;
        if table.metadata().uuid() != token.table_uuid {
            return Err(Error::ReconciliationConflict);
        }
        let already_committed = table.metadata().current_snapshot().is_some_and(|current| {
            current.snapshot_id() == snapshot_id
                && current
                    .summary()
                    .additional_properties
                    .get(RECONCILIATION_ID_PROPERTY)
                    == Some(&input.plan.id.to_string())
                && current
                    .summary()
                    .additional_properties
                    .get(REVISION_PROPERTY)
                    == Some(&input.plan.target_revision.to_string())
        });
        if !already_committed {
            if table.metadata().current_snapshot_id() != input.plan.base_snapshot_id {
                return Err(Error::ReconciliationConflict);
            }
            for requirement in &commit.requirements {
                requirement.check(Some(table.metadata()))?;
            }
            table = catalog
                .update_table(
                    TableCommit::builder()
                        .ident(token.table_ident.clone())
                        .updates(commit.updates)
                        .requirements(commit.requirements)
                        .build(),
                )
                .await?;
            if table.metadata().uuid() != token.table_uuid
                || table.metadata().current_snapshot_id() != Some(snapshot_id)
            {
                return Err(Error::ReconciliationConflict);
            }
        }

        // Confirmation follows an observed catalog effect, even if the submitting lease expired.
        let key = RecordType::TableWork
            .table_key(&token.catalog_id, token.table_uuid)
            .freeze();
        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let mut work: TableWork = decode(&tx.get(&key).await?.ok_or(Error::TaskFenced)?)?;
            if work.reconciliation_plan.as_ref() != Some(&input.plan) {
                return Err(Error::TaskFenced);
            }
            let target = Target {
                catalog_id: token.catalog_id.clone(),
                table_uuid: token.table_uuid,
                kind: TaskKind::Reconcile,
            };
            if let Some(scheduled) = work.reconciliation.take() {
                tx.delete(ready_key(&target, scheduled.eligible_at))?;
                if let Some(assignment) = scheduled.assignment {
                    let worker_key = worker_key_for(&assignment.token);
                    if tx
                        .get(&worker_key)
                        .await?
                        .map(|bytes| decode::<crate::TaskAssignment>(&bytes))
                        .transpose()?
                        .as_ref()
                        == Some(&assignment)
                    {
                        tx.delete(worker_key)?;
                    }
                }
            }
            work.reconciled_revision = input.plan.target_revision;
            work.reconciled_snapshot_id = Some(snapshot_id);
            work.reconciliation_plan = None;
            work.enqueue_reconciliation(&tx, token, self.clock.now())?;
            tx.put(key.clone(), encode(&work)?)?;
            tx.delete(prepared_key.clone())?;
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
}

fn validate_commit<'a>(
    token: &TaskToken,
    plan: &ReconciliationPlan,
    commit: &'a PreparedCommit,
) -> Result<&'a Snapshot> {
    let [
        TableUpdate::AddSnapshot { snapshot },
        TableUpdate::SetSnapshotRef {
            ref_name,
            reference,
        },
    ] = commit.updates.as_slice()
    else {
        return Err(Error::InvalidPreparedCommit);
    };
    if ref_name != MAIN_BRANCH
        || reference.snapshot_id != snapshot.snapshot_id()
        || snapshot.parent_snapshot_id() != plan.base_snapshot_id
        || snapshot.summary().operation != Operation::Append
        || snapshot
            .summary()
            .additional_properties
            .get(RECONCILIATION_ID_PROPERTY)
            != Some(&plan.id.to_string())
        || snapshot
            .summary()
            .additional_properties
            .get(REVISION_PROPERTY)
            != Some(&plan.target_revision.to_string())
        || !commit.requirements.contains(&TableRequirement::UuidMatch {
            uuid: token.table_uuid,
        })
        || !commit
            .requirements
            .contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.into(),
                snapshot_id: plan.base_snapshot_id,
            })
        || !commit
            .requirements
            .iter()
            .any(|requirement| matches!(requirement, TableRequirement::CurrentSchemaIdMatch { .. }))
    {
        return Err(Error::InvalidPreparedCommit);
    }
    Ok(snapshot)
}
