use std::sync::Arc;

use iceberg::{TableIdent, spec::TableMetadata};
use se_iceberg::storage::ObjectStoreStorageFactory;
use serde::Serialize;
use slatedb::{ErrorKind, IsolationLevel};
use tracing::{info, instrument};
use uuid::Uuid;

use crate::{
    BatchDescriptor, CatalogId, Error, IndexDelta, Metadata, PreparedCommit, Result,
    VersionedDataFile, VersionedDeletionVector,
    encoding::{
        ReadyWorkKey, TableKey, TableRecordType, WalStateValue, WorkerAssignmentKey, decode, encode,
    },
    tasks::{self, Scheduled, TableWork, Target, TaskKind},
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
    pub wal_deletions: Vec<crate::WalDeletion>,
    pub files: Vec<VersionedDataFile>,
    pub deletion_vectors: Vec<VersionedDeletionVector>,
    pub index_deltas: Vec<IndexDelta>,
    pub prepared_commit: Option<PreparedCommit>,
}

impl Metadata {
    /// Loads catalog metadata and durably records the table binding and checkpoint interval.
    #[instrument(name = "metadata.resolve_table", skip_all, fields(catalog.id = %catalog_id, table.ident = %table))]
    pub async fn resolve_table(
        &self,
        catalog_id: &CatalogId,
        table: &TableIdent,
    ) -> Result<TableMetadata> {
        info!(catalog.id = %catalog_id, table.ident = %table, "resolving table");

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
        let key = TableKey {
            catalog_id,
            table_uuid: metadata.uuid(),
        }
        .record(TableRecordType::TableWork);

        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let mut work: TableWork = tx
                .get(&key)
                .await?
                .map(|bytes| decode(&bytes))
                .transpose()?
                .unwrap_or_else(|| TableWork::new(table.clone()));
            if work.deletion.is_some() {
                return Err(Error::TableDeleting(metadata.uuid()));
            }
            work.table_ident = table.clone();
            work.checkpoint_interval_sec = checkpoint_interval_sec;
            work.checkpoint_eligible_at(self.clock.now())?;
            tx.put(key.clone(), encode(&work)?)?;

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
        info!(catalog.id = %catalog_id, table.uuid = %table_uuid, "deleting table");

        let key = TableKey {
            catalog_id,
            table_uuid,
        }
        .record(TableRecordType::TableWork);

        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

            let mut work: TableWork = decode(
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
                (TaskKind::GarbageCollectWal, work.wal_cleanup.take()),
            ] {
                if let Some(scheduled) = scheduled {
                    let target = Target {
                        catalog_id: catalog_id.clone(),
                        table_uuid,
                        kind,
                    };

                    tx.delete(
                        ReadyWorkKey {
                            target: &target,
                            at: scheduled.eligible_at,
                        }
                        .encode(),
                    )?;

                    if let Some(assignment) = scheduled.assignment {
                        // Allow existing workers to observe fencing before removing their files.
                        eligible_at = eligible_at.max(assignment.expires_at);
                        let worker_key = WorkerAssignmentKey::for_token(&assignment.token).encode();

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

            tx.put(
                ReadyWorkKey {
                    target: &target,
                    at: eligible_at,
                }
                .encode(),
                encode(&target)?,
            )?;
            tx.put(key.clone(), encode(&work)?)?;

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
        info!(catalog.id = %token.catalog_id, table.uuid = %token.table_uuid, "completing table deletion");

        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

            let table = TableKey::for_token(token);
            let key = table.record(TableRecordType::TableWork);

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

            tx.delete(
                ReadyWorkKey {
                    target: &target,
                    at: work.deletion.ok_or(Error::InvalidTaskState)?.eligible_at,
                }
                .encode(),
            )?;

            tx.delete(WorkerAssignmentKey::for_token(token).encode())?;

            let scan = table.records();
            let mut scan = tx.scan_prefix(scan.prefix, scan.suffix).await?;

            while let Some(entry) = scan.next().await? {
                tx.delete(entry.key)?;
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
        info!(catalog.id = %catalog_id, table.uuid = %table_uuid, "reading table debug state");

        let snapshot = self.db.snapshot().await?;
        let table = TableKey {
            catalog_id,
            table_uuid,
        };
        let state = decode(
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
        let scan = table.wal_batches(..);
        let mut scan = snapshot.scan_prefix(scan.prefix, scan.suffix).await?;
        let mut wal = Vec::new();

        while let Some(entry) = scan.next().await? {
            wal.push(BatchDescriptor {
                sequence: table.wal_sequence(&entry.key)?,
                metadata: decode(&entry.value)?,
            });
        }

        let scan = table.scan(TableRecordType::WalDeletion);
        let mut scan = snapshot.scan_prefix(scan.prefix, scan.suffix).await?;
        let mut wal_deletions = Vec::new();
        while let Some(entry) = scan.next().await? {
            wal_deletions.push(decode(&entry.value)?);
        }

        let scan = table.scan(TableRecordType::DataFile);
        let mut scan = snapshot.scan_prefix(scan.prefix, scan.suffix).await?;
        let mut files = Vec::new();
        while let Some(entry) = scan.next().await? {
            files.push(decode(&entry.value)?);
        }

        let scan = table.scan(TableRecordType::DeletionVector);
        let mut scan = snapshot.scan_prefix(scan.prefix, scan.suffix).await?;
        let mut deletion_vectors = Vec::new();
        while let Some(entry) = scan.next().await? {
            deletion_vectors.push(decode(&entry.value)?);
        }

        let scan = table.scan(TableRecordType::IndexDelta);
        let mut scan = snapshot.scan_prefix(scan.prefix, scan.suffix).await?;
        let mut index_deltas = Vec::new();
        while let Some(entry) = scan.next().await? {
            index_deltas.push(decode(&entry.value)?);
        }

        Ok(TableDebugState {
            catalog_id: catalog_id.clone(),
            table_uuid,
            state,
            next_sequence: wal_state.next_sequence,
            materializer_epoch: wal_state.materializer_epoch,
            wal,
            wal_deletions,
            files,
            deletion_vectors,
            index_deltas,
            prepared_commit: snapshot
                .get(table.record(TableRecordType::PreparedCommit))
                .await?
                .map(|bytes| decode(&bytes))
                .transpose()?,
        })
    }
}
