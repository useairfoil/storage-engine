pub mod commit;
pub mod consumer;
pub mod memory;
pub mod metadata;
pub mod producer;

#[cfg(any(test, feature = "test-util"))]
pub mod test_util;

pub use commit::BatchCommit;
pub use consumer::{BatchRead, ConsumedBatch, Consumer, ConsumerFetchHandle};
pub use memory::InMemoryWal;
pub use metadata::{BatchDescriptor, BatchMetadata, WalRequest};
pub use producer::{CompressionType, DurabilityWatcher, Producer, ProducerOptions, WriteHandle};

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("Arrow IPC serialization failed: {0}")]
    Serialization(String),

    #[error("object store operation failed: {0}")]
    Storage(String),

    #[error("WAL commit failed: {0}")]
    Commit(String),

    #[error("WAL reader has been fenced")]
    Fenced,

    #[error("WAL producer is closed")]
    Closed,

    #[error("WAL writer task failed: {0}")]
    Writer(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
