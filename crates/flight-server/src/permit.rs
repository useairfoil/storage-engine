use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

use crate::IngestionOptions;

const MEMORY_PERMIT_BYTES: u64 = 1024;

pub(crate) struct IngestionSemaphore {
    max_in_flight_writes: usize,
    writes: Arc<Semaphore>,
    stream_memory: Arc<Semaphore>,
    global_memory: Arc<Semaphore>,
}

pub(crate) struct IngestionPermit {
    _write: OwnedSemaphorePermit,
    _stream_memory: OwnedSemaphorePermit,
    _global_memory: OwnedSemaphorePermit,
}

impl IngestionSemaphore {
    pub(crate) fn global_memory(options: &IngestionOptions) -> Arc<Semaphore> {
        Arc::new(Semaphore::new(
            memory_permits(options.max_global_in_flight_bytes.get()) as usize,
        ))
    }

    pub(crate) fn new(options: &IngestionOptions, global_memory: Arc<Semaphore>) -> Self {
        let max_in_flight_writes = options.max_in_flight_writes.get();
        Self {
            max_in_flight_writes,
            writes: Arc::new(Semaphore::new(max_in_flight_writes)),
            stream_memory: Arc::new(Semaphore::new(
                memory_permits(options.max_in_flight_bytes.get()) as usize,
            )),
            global_memory,
        }
    }

    pub(crate) fn max_in_flight_writes(&self) -> usize {
        self.max_in_flight_writes
    }

    pub(crate) fn has_write_capacity(&self) -> bool {
        self.writes.available_permits() > 0
    }

    pub(crate) fn try_acquire(&self, batch_bytes: u64) -> Result<IngestionPermit, Status> {
        let write = Arc::clone(&self.writes)
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("concurrent ingestion write limit exceeded"))?;
        let memory_permits = memory_permits(batch_bytes);
        let stream_memory = Arc::clone(&self.stream_memory)
            .try_acquire_many_owned(memory_permits)
            .map_err(|_| {
                Status::resource_exhausted("per-stream ingestion memory limit exceeded")
            })?;
        let global_memory = Arc::clone(&self.global_memory)
            .try_acquire_many_owned(memory_permits)
            .map_err(|_| Status::resource_exhausted("global ingestion memory limit exceeded"))?;

        Ok(IngestionPermit {
            _write: write,
            _stream_memory: stream_memory,
            _global_memory: global_memory,
        })
    }
}

fn memory_permits(bytes: u64) -> u32 {
    let max_permits = u64::try_from(Semaphore::MAX_PERMITS)
        .unwrap_or(u64::MAX)
        .min(u64::from(u32::MAX));
    u32::try_from(bytes.div_ceil(MEMORY_PERMIT_BYTES).min(max_permits)).unwrap_or(u32::MAX)
}
