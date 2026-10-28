use std::{io::Cursor, sync::Arc};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use async_trait::async_trait;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};

use crate::{BatchDescriptor, BatchMetadata, Error, Result};

#[async_trait]
pub trait BatchRead: Send + Sync {
    /// Claims a reader epoch, fencing acknowledgments from previous readers of this WAL.
    async fn claim_reader(&self) -> Result<u64>;

    /// Returns up to `max` committed descriptors in sequence order after the supplied sequence.
    async fn read_after(&self, sequence: Option<u64>, max: usize) -> Result<Vec<BatchDescriptor>>;

    /// Idempotently removes batches through a committed sequence if the epoch is current.
    async fn ack_through(&self, epoch: u64, sequence: u64) -> Result<()>;
}

#[derive(Debug)]
pub struct ConsumedBatch {
    pub batches: Vec<RecordBatch>,
    pub sequence: u64,
    pub metadata: BatchMetadata,
}

pub struct Consumer {
    wal: Arc<dyn BatchRead>,
    epoch: Option<u64>,
    fetch_handle: ConsumerFetchHandle,
    last_handed_out_sequence: Option<u64>,
    last_acked_sequence: Option<u64>,
}

#[derive(Clone)]
pub struct ConsumerFetchHandle {
    object_store: Arc<dyn ObjectStore>,
}

impl Consumer {
    /// Claims a reader epoch and reconciles batches through the last acknowledged sequence.
    pub async fn new(
        wal: Arc<dyn BatchRead>,
        object_store: Arc<dyn ObjectStore>,
        last_acked_sequence: Option<u64>,
    ) -> Result<Self> {
        let epoch = wal.claim_reader().await?;
        if let Some(sequence) = last_acked_sequence {
            wal.ack_through(epoch, sequence).await?;
        }

        Ok(Self {
            wal,
            epoch: Some(epoch),
            fetch_handle: ConsumerFetchHandle { object_store },
            last_handed_out_sequence: last_acked_sequence,
            last_acked_sequence,
        })
    }

    /// Creates a non-fencing consumer that cannot acknowledge batches.
    pub fn new_read_only(
        wal: Arc<dyn BatchRead>,
        object_store: Arc<dyn ObjectStore>,
        last_sequence: Option<u64>,
    ) -> Self {
        Self {
            wal,
            epoch: None,
            fetch_handle: ConsumerFetchHandle { object_store },
            last_handed_out_sequence: last_sequence,
            last_acked_sequence: None,
        }
    }

    /// Hands out up to `max` descriptors once per consumer without fetching their files.
    pub async fn next_descriptors(&mut self, max: usize) -> Result<Vec<BatchDescriptor>> {
        if max == 0 {
            return Ok(Vec::new());
        }

        let entries = self
            .wal
            .read_after(self.last_handed_out_sequence, max)
            .await?;
        if let Some(last) = entries.last() {
            self.last_handed_out_sequence = Some(last.sequence);
        }

        Ok(entries)
    }

    /// Returns an independent handle for fetching descriptors concurrently.
    pub fn fetch_handle(&self) -> ConsumerFetchHandle {
        self.fetch_handle.clone()
    }

    /// Acknowledges a strictly increasing sequence after the WAL accepts the acknowledgment.
    pub async fn ack_through(&mut self, sequence: u64) -> Result<()> {
        let epoch = self.epoch.ok_or_else(|| {
            Error::InvalidInput("read-only consumer cannot acknowledge batches".into())
        })?;

        if let Some(last) = self.last_acked_sequence
            && sequence <= last
        {
            return Err(Error::InvalidInput(format!(
                "non-monotonic ack_through: last_acked={last}, requested={sequence}"
            )));
        }

        self.wal.ack_through(epoch, sequence).await?;
        self.last_acked_sequence = Some(sequence);

        Ok(())
    }

    /// Closes the consumer without acknowledging unread or unprocessed batches.
    pub async fn close(self) -> Result<()> {
        Ok(())
    }
}

impl ConsumerFetchHandle {
    /// Fetches and decodes a descriptor's IPC file without reading or changing WAL state.
    pub async fn fetch(&self, descriptor: BatchDescriptor) -> Result<ConsumedBatch> {
        let data = self
            .object_store
            .get(&Path::from(descriptor.metadata.location.as_str()))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let reader = StreamReader::try_new(Cursor::new(data), None)
            .map_err(|error| Error::Serialization(error.to_string()))?;
        let batches = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| Error::Serialization(error.to_string()))?;

        Ok(ConsumedBatch {
            batches,
            sequence: descriptor.sequence,
            metadata: descriptor.metadata,
        })
    }
}
