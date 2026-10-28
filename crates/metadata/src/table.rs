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
    tasks::{self, TableWork},
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

    /// Loads table metadata from its catalog and durably records its UUID/name binding.
    #[tracing::instrument(name = "metadata.resolve_table", skip_all, fields(catalog_id = %catalog_id, %table))]
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
            work.table_ident = table.clone();
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
}
