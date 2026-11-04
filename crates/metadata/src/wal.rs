use bytesize::ByteSize;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use slatedb::{ErrorKind, IsolationLevel};
use tracing::info;
use uuid::Uuid;

use crate::{
    CatalogId, Error, Metadata, Result,
    encoding::{RecordType, TableKey, WalStateValue, decode, encode},
    tasks::TableWork,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchMetadata {
    pub schema_id: i32,
    pub location: String,
    pub timestamp: DateTime<Utc>,
    #[serde(default)]
    pub wal_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchDescriptor {
    pub sequence: u64,
    pub metadata: BatchMetadata,
}

impl Metadata {
    /// Returns retained WAL descriptors in sequence order without claiming or acknowledging them.
    #[tracing::instrument(name = "metadata.wal_read_after", skip_all, fields(catalog.id = %catalog_id, table.uuid = %table_uuid, args.sequence = ?sequence, args.max = max))]
    pub async fn wal_read_after(
        &self,
        catalog_id: &CatalogId,
        table_uuid: Uuid,
        sequence: Option<u64>,
        max: usize,
    ) -> Result<Vec<BatchDescriptor>> {
        info!(catalog.id = %catalog_id, table.uuid = %table_uuid, args.sequence = ?sequence, args.max = max, "reading wal batches");

        if max == 0 || sequence == Some(u64::MAX) {
            return Ok(Vec::new());
        }

        let table = TableKey {
            catalog_id,
            table_uuid,
        };
        let scan = table.wal_batches(sequence.map_or(0, |sequence| sequence + 1)..);
        let mut scan = self.db.scan_prefix(scan.prefix, scan.suffix).await?;
        let mut batches = Vec::new();

        while batches.len() < max {
            let Some(entry) = scan.next().await? else {
                break;
            };
            batches.push(BatchDescriptor {
                sequence: table.wal_sequence(&entry.key)?,
                metadata: decode(&entry.value)?,
            });
        }

        Ok(batches)
    }

    /// Atomically appends an uploaded WAL file and queues materialization after the checkpoint interval.
    #[tracing::instrument(
        name = "metadata.wal_append",
        skip_all,
        fields(
            catalog.id = %catalog_id,
            table.uuid = %table_uuid,
            args.schema_id = schema_id,
            args.wal_size_bytes = wal_size_bytes,
        )
    )]
    pub async fn wal_append(
        &self,
        catalog_id: &CatalogId,
        table_uuid: Uuid,
        schema_id: i32,
        location: &str,
        timestamp: DateTime<Utc>,
        wal_size_bytes: u64,
    ) -> Result<()> {
        info!(
            catalog.id = %catalog_id,
            table.uuid = %table_uuid,
            args.schema_id = schema_id,
            args.wal_size = %ByteSize::b(wal_size_bytes),
            "appending wal batch"
        );

        let table = TableKey {
            catalog_id,
            table_uuid,
        };
        let state_key = table.record(RecordType::WalState);
        let batch = encode(&BatchMetadata {
            schema_id,
            location: location.into(),
            timestamp,
            wal_size_bytes,
        })?;

        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let mut state: WalStateValue = tx
                .get(&state_key)
                .await?
                .map(|bytes| decode(&bytes))
                .transpose()?
                .unwrap_or_default();
            let sequence = state.next_sequence;
            state.next_sequence = sequence.checked_add(1).ok_or(Error::WalSequenceExhausted)?;

            tx.put(table.wal_batch(sequence), batch.clone())?;
            tx.put(state_key.clone(), encode(&state)?)?;

            let work_key = table.record(RecordType::TableWork);
            let mut work: TableWork = tx
                .get(&work_key)
                .await?
                .map(|bytes| decode(&bytes))
                .transpose()?
                .ok_or(Error::TableNotResolved(table_uuid))?;
            if work.deletion.is_some() {
                return Err(Error::TableDeleting(table_uuid));
            }
            if let Some((key, value)) = work.enqueue(catalog_id, table_uuid, self.clock.now())? {
                tx.put(key, value)?;
            }
            tx.put(work_key, encode(&work)?)?;

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
