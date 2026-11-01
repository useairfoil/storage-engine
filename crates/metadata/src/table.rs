use std::sync::Arc;

use iceberg::{TableIdent, spec::TableMetadata};
use se_iceberg::storage::ObjectStoreStorageFactory;
use serde::Serialize;
use slatedb::{ErrorKind, IsolationLevel};
use uuid::Uuid;

use crate::{
    BatchDescriptor, CatalogId, Error, IndexDelta, Metadata, PreparedCommit, Result,
    VersionedDataFile, VersionedDeletionVector,
    encoding::{RecordType, WalBatchKey, WalBatchValue, WalStateKey, WalStateValue},
    tasks::{self, Scheduled, TableWork, Target, TaskKind, ready_key, worker_key_for},
};

#[derive(Serialize)]
pub struct TableDebugState {
    pub catalog_id: CatalogId,
    pub table_uuid: Uuid,
    #[serde(flatten)]
    pub state: TableWork,
    pub next_sequence: u64,
    pub materializer_epoch: u64,
    pub wal: Vec<BatchDescriptor>,
    pub files: Vec<VersionedDataFile>,
    pub deletion_vectors: Vec<VersionedDeletionVector>,
    pub index_deltas: Vec<IndexDelta>,
    pub prepared_commit: Option<PreparedCommit>,
}

impl Metadata {
    /// Loads catalog metadata and durably records the table binding and checkpoint interval.
    #[tracing::instrument(name = "metadata.resolve_table", skip_all, fields(catalog.id = %catalog_id, table.ident = %table))]
    pub async fn resolve_table(
        &self,
        catalog_id: &CatalogId,
        table: &TableIdent,
    ) -> Result<TableMetadata> {
        let config = self
            .get_catalog(catalog_id)
            .await?
            .ok_or_else(|| Error::CatalogNotFound(catalog_id.clone()))?;
        let catalog = config
            .to_catalog(catalog_id, Arc::new(ObjectStoreStorageFactory))
            .await?;
        let metadata = catalog.load_table(table).await?.metadata().clone();
        let checkpoint_interval_sec = metadata
            .properties()
            .get(tasks::CHECKPOINT_INTERVAL_PROPERTY)
            .map(|value| {
                value
                    .parse::<u64>()
                    .ok()
                    .filter(|seconds| *seconds <= 900)
                    .ok_or_else(|| Error::InvalidCheckpointInterval(value.clone()))
            })
            .transpose()?
            .unwrap_or(tasks::DEFAULT_CHECKPOINT_INTERVAL_SEC);
        let key = RecordType::TableWork
            .table_key(catalog_id, metadata.uuid())
            .freeze();

        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let mut work: TableWork = tx
                .get(&key)
                .await?
                .map(|bytes| tasks::decode(&bytes))
                .transpose()?
                .unwrap_or_else(|| TableWork::new(table.clone()));
            if work.deletion.is_some() {
                return Err(Error::TableDeleting(metadata.uuid()));
            }
            work.table_ident = table.clone();
            work.checkpoint_interval_sec = checkpoint_interval_sec;
            work.checkpoint_eligible_at(self.clock.now())?;
            tx.put(key.clone(), tasks::encode(&work)?)?;

            match tx.commit().await {
                Ok(handle) => {
                    if let Some(handle) = handle {
                        handle.await_durable().await?;
                    }

                    return Ok(metadata);
                }
                Err(error) if error.kind() == ErrorKind::Transaction => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Marks a table for deletion and replaces its scheduled work with a deletion job.
    pub async fn delete_table(&self, catalog_id: &CatalogId, table_uuid: Uuid) -> Result<()> {
        let key = RecordType::TableWork
            .table_key(catalog_id, table_uuid)
            .freeze();

        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

            let mut work: TableWork = tasks::decode(
                &tx.get(&key)
                    .await?
                    .ok_or(Error::TableNotResolved(table_uuid))?,
            )?;

            if work.deletion.is_some() {
                self.db.flush().await?;
                return Ok(());
            }

            let now = self.clock.now();
            let mut eligible_at = now;

            for (kind, scheduled) in [
                (TaskKind::Materialize, work.materialization.take()),
                (TaskKind::Reconcile, work.reconciliation.take()),
            ] {
                if let Some(scheduled) = scheduled {
                    let target = Target {
                        catalog_id: catalog_id.clone(),
                        table_uuid,
                        kind,
                    };

                    tx.delete(ready_key(&target, scheduled.eligible_at))?;

                    if let Some(assignment) = scheduled.assignment {
                        // Allow existing workers to observe fencing before removing their files.
                        eligible_at = eligible_at.max(assignment.expires_at);
                        let worker_key = worker_key_for(&assignment.token);

                        if tx
                            .get(&worker_key)
                            .await?
                            .map(|bytes| tasks::decode::<crate::TaskAssignment>(&bytes))
                            .transpose()?
                            .as_ref()
                            == Some(&assignment)
                        {
                            tx.delete(worker_key)?;
                        }
                    }
                }
            }

            work.reconciliation_plan = None;
            work.deletion = Some(Scheduled {
                eligible_at,
                assignment: None,
            });

            let target = Target {
                catalog_id: catalog_id.clone(),
                table_uuid,
                kind: TaskKind::DeleteTable,
            };

            tx.put(ready_key(&target, eligible_at), tasks::encode(&target)?)?;
            tx.put(key.clone(), tasks::encode(&work)?)?;

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

    /// Removes all persisted table records after its deletion worker finishes.
    pub async fn complete_delete_table(&self, token: &crate::TaskToken) -> Result<()> {
        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

            let key = RecordType::TableWork.table_key(&token.catalog_id, token.table_uuid);

            if tx.get(&key).await?.is_none() {
                self.db.flush().await?;
                return Ok(());
            }

            let (work, assignment) = self.active_assignment(&tx, token).await?;
            if assignment.task != crate::Task::DeleteTable {
                return Err(Error::InvalidTaskResult);
            }

            let target = Target {
                catalog_id: token.catalog_id.clone(),
                table_uuid: token.table_uuid,
                kind: TaskKind::DeleteTable,
            };

            tx.delete(ready_key(
                &target,
                work.deletion.ok_or(Error::InvalidTaskState)?.eligible_at,
            ))?;

            tx.delete(worker_key_for(token))?;

            for record in [
                RecordType::TableWork,
                RecordType::WalState,
                RecordType::WalBatch,
                RecordType::DataFile,
                RecordType::DeletionVector,
                RecordType::CurrentDeletionVector,
                RecordType::PreparedCommit,
                RecordType::IndexDelta,
                RecordType::MaterializationReceipt,
            ] {
                let mut scan = tx
                    .scan_prefix(
                        record
                            .table_key(&token.catalog_id, token.table_uuid)
                            .freeze(),
                        ..,
                    )
                    .await?;

                while let Some(entry) = scan.next().await? {
                    tx.delete(entry.key)?;
                }
            }

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

    /// Dumps a consistent snapshot of a resolved table's persisted metadata without changing it.
    pub async fn debug_table(
        &self,
        catalog_id: &CatalogId,
        table_uuid: Uuid,
    ) -> Result<TableDebugState> {
        let snapshot = self.db.snapshot().await?;
        let state = tasks::decode(
            &snapshot
                .get(RecordType::TableWork.table_key(catalog_id, table_uuid))
                .await?
                .ok_or(Error::TableNotResolved(table_uuid))?,
        )?;
        let wal_state = snapshot
            .get(
                WalStateKey {
                    catalog_id,
                    table_uuid,
                }
                .encode(),
            )
            .await?
            .map(|bytes| WalStateValue::decode(&bytes))
            .transpose()?
            .unwrap_or_default();
        let prefix = WalBatchKey {
            catalog_id,
            table_uuid,
            sequence: 0,
        }
        .prefix();
        let mut scan = snapshot.scan_prefix(&prefix, ..).await?;
        let mut wal = Vec::new();

        while let Some(entry) = scan.next().await? {
            wal.push(BatchDescriptor {
                sequence: u64::from_be_bytes(
                    entry.key[prefix.len()..]
                        .try_into()
                        .map_err(|_| Error::InvalidWalBatch)?,
                ),
                metadata: WalBatchValue::decode(&entry.value)?,
            });
        }

        let mut scan = snapshot
            .scan_prefix(
                RecordType::DataFile
                    .table_key(catalog_id, table_uuid)
                    .freeze(),
                ..,
            )
            .await?;
        let mut files = Vec::new();
        while let Some(entry) = scan.next().await? {
            files.push(tasks::decode(&entry.value)?);
        }

        let mut scan = snapshot
            .scan_prefix(
                RecordType::DeletionVector
                    .table_key(catalog_id, table_uuid)
                    .freeze(),
                ..,
            )
            .await?;
        let mut deletion_vectors = Vec::new();
        while let Some(entry) = scan.next().await? {
            deletion_vectors.push(tasks::decode(&entry.value)?);
        }

        let mut scan = snapshot
            .scan_prefix(
                RecordType::IndexDelta
                    .table_key(catalog_id, table_uuid)
                    .freeze(),
                ..,
            )
            .await?;
        let mut index_deltas = Vec::new();
        while let Some(entry) = scan.next().await? {
            index_deltas.push(tasks::decode(&entry.value)?);
        }

        Ok(TableDebugState {
            catalog_id: catalog_id.clone(),
            table_uuid,
            state,
            next_sequence: wal_state.next_sequence,
            materializer_epoch: wal_state.materializer_epoch,
            wal,
            files,
            deletion_vectors,
            index_deltas,
            prepared_commit: snapshot
                .get(RecordType::PreparedCommit.table_key(catalog_id, table_uuid))
                .await?
                .map(|bytes| tasks::decode(&bytes))
                .transpose()?,
        })
    }
}
