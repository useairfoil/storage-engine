use std::sync::Arc;

use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

use crate::IngestionOptions;

pub(crate) struct IngestionSemaphore {
    max_in_flight_writes: usize,
    writes: Arc<Semaphore>,
    stream_memory: Arc<dyn MemoryPool>,
}

pub(crate) struct IngestionPermit {
    _write: OwnedSemaphorePermit,
    stream_memory: MemoryReservation,
}

impl IngestionSemaphore {
    pub(crate) fn new(options: &IngestionOptions) -> Self {
        let max_in_flight_writes = options.max_in_flight_writes.get();
        Self {
            max_in_flight_writes,
            writes: Arc::new(Semaphore::new(max_in_flight_writes)),
            stream_memory: Arc::new(GreedyMemoryPool::new(
                usize::try_from(options.max_in_flight_bytes.get()).unwrap_or(usize::MAX),
            )),
        }
    }

    pub(crate) fn max_in_flight_writes(&self) -> usize {
        self.max_in_flight_writes
    }

    pub(crate) fn has_write_capacity(&self) -> bool {
        self.writes.available_permits() > 0
    }

    pub(crate) fn try_acquire(&self, batch_bytes: usize) -> Result<IngestionPermit, Status> {
        let write = Arc::clone(&self.writes)
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("concurrent ingestion write limit exceeded"))?;
        let permit = IngestionPermit {
            _write: write,
            stream_memory: MemoryConsumer::new("ingestion batch").register(&self.stream_memory),
        };

        permit.resize(batch_bytes)?;

        Ok(permit)
    }
}

impl IngestionPermit {
    pub(crate) fn resize(&self, batch_bytes: usize) -> Result<(), Status> {
        self.stream_memory
            .try_resize(batch_bytes)
            .map_err(|_| Status::resource_exhausted("per-stream ingestion memory limit exceeded"))
    }
}
