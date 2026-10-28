use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use slatedb::{ErrorKind, IsolationLevel};
use uuid::Uuid;

use crate::{
    CatalogId, Error, Metadata, Result,
    encoding::{WalBatchKey, WalBatchValue, WalStateKey, WalStateValue},
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
    /// Claims a new reader epoch and durably fences acknowledgments from previous readers.
    #[tracing::instrument(name = "metadata.wal_claim_reader", skip_all, fields(catalog_id = %catalog_id, %table_uuid))]
    pub async fn wal_claim_reader(&self, catalog_id: &CatalogId, table_uuid: Uuid) -> Result<u64> {
        let key = WalStateKey {
            catalog_id,
            table_uuid,
        }
        .encode();

        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let mut state = tx
                .get(&key)
                .await?
                .map(|bytes| WalStateValue::decode(&bytes))
                .transpose()?
                .unwrap_or_default();
            state.reader_epoch = state
                .reader_epoch
                .checked_add(1)
                .ok_or(Error::WalEpochExhausted)?;
            tx.put(key.clone(), state.encode())?;

            match tx.commit().await {
                Ok(handle) => {
                    if let Some(handle) = handle {
                        handle.await_durable().await?;
                    }

                    return Ok(state.reader_epoch);
                }
                Err(error) if error.kind() == ErrorKind::Transaction => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Durably removes descriptors through a committed sequence if the reader epoch is current.
    #[tracing::instrument(name = "metadata.wal_ack_through", skip_all, fields(catalog_id = %catalog_id, %table_uuid, epoch, sequence))]
    pub async fn wal_ack_through(
        &self,
        catalog_id: &CatalogId,
        table_uuid: Uuid,
        epoch: u64,
        sequence: u64,
    ) -> Result<()> {
        let state_key = WalStateKey {
            catalog_id,
            table_uuid,
        }
        .encode();
        let prefix = WalBatchKey {
            catalog_id,
            table_uuid,
            sequence: 0,
        }
        .prefix();

        loop {
            let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let state = tx
                .get(&state_key)
                .await?
                .map(|bytes| WalStateValue::decode(&bytes))
                .transpose()?
                .unwrap_or_default();

            if epoch == 0 || epoch != state.reader_epoch {
                return Err(Error::WalFenced);
            }

            if sequence >= state.next_sequence {
                return Err(Error::WalSequenceTooHigh {
                    sequence,
                    next_sequence: state.next_sequence,
                });
            }

            let mut scan = tx.scan_prefix(&prefix, ..=sequence.to_be_bytes()).await?;

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

    /// Atomically allocates a per-table sequence and durably appends an uploaded WAL file.
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
