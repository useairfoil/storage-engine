use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use slatedb::{ErrorKind, IsolationLevel};
use uuid::Uuid;

use crate::{
    CatalogId, Error, Metadata, Result,
    encoding::{RecordType, WalBatchKey, WalBatchValue, WalStateKey, WalStateValue},
    tasks::{self, TableWork},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchMetadata {
    pub schema_id: i32,
    pub location: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchDescriptor {
    pub sequence: u64,
    pub metadata: BatchMetadata,
}

impl Metadata {
    /// Returns retained WAL descriptors in sequence order without claiming or acknowledging them.
    #[tracing::instrument(name = "metadata.wal_read_after", skip_all, fields(catalog_id = %catalog_id, %table_uuid, ?sequence, max))]
    pub async fn wal_read_after(
        &self,
        catalog_id: &CatalogId,
        table_uuid: Uuid,
        sequence: Option<u64>,
        max: usize,
    ) -> Result<Vec<BatchDescriptor>> {
        if max == 0 || sequence == Some(u64::MAX) {
            return Ok(Vec::new());
        }

        let key = WalBatchKey {
            catalog_id,
            table_uuid,
            sequence: sequence.map_or(0, |sequence| sequence + 1),
        };
        let prefix = key.prefix();
        let mut scan = self
            .db
            .scan_prefix(&prefix, key.sequence.to_be_bytes()..)
            .await?;
        let mut batches = Vec::new();

        while batches.len() < max {
            let Some(entry) = scan.next().await? else {
                break;
            };
            let sequence = u64::from_be_bytes(
                entry.key[prefix.len()..]
                    .try_into()
                    .map_err(|_| Error::InvalidWalBatch)?,
            );

            batches.push(BatchDescriptor {
                sequence,
                metadata: WalBatchValue::decode(&entry.value)?,
            });
        }

        Ok(batches)
    }

    /// Atomically appends an uploaded WAL file and queues materialization after the checkpoint interval.
    #[tracing::instrument(name = "metadata.wal_append", skip_all, fields(catalog_id = %catalog_id, %table_uuid, schema_id))]
    pub async fn wal_append(
        &self,
        catalog_id: &CatalogId,
        table_uuid: Uuid,
        schema_id: i32,
        location: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        let state_key = WalStateKey {
            catalog_id,
            table_uuid,
        }
        .encode();
        let batch = WalBatchValue {
            schema_id,
            location,
            timestamp,
        }
        .encode();

        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let mut state = tx
                .get(&state_key)
                .await?
                .map(|bytes| WalStateValue::decode(&bytes))
                .transpose()?
                .unwrap_or_default();
            let sequence = state.next_sequence;
            state.next_sequence = sequence.checked_add(1).ok_or(Error::WalSequenceExhausted)?;

            tx.put(
                WalBatchKey {
                    catalog_id,
                    table_uuid,
                    sequence,
                }
                .encode(),
                batch.clone(),
            )?;
            tx.put(state_key.clone(), state.encode())?;

            let work_key = RecordType::TableWork
                .table_key(catalog_id, table_uuid)
                .freeze();
            let mut work: TableWork = tx
                .get(&work_key)
                .await?
                .map(|bytes| tasks::decode(&bytes))
                .transpose()?
                .ok_or(Error::TableNotResolved(table_uuid))?;
            if let Some((key, value)) = work.enqueue(catalog_id, table_uuid, self.clock.now())? {
                tx.put(key, value)?;
            }
            tx.put(work_key, tasks::encode(&work)?)?;

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
