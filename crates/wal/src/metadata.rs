pub use se_metadata::{BatchDescriptor, BatchMetadata};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum WalRequest {
    Commit { batch: BatchMetadata },
    ClaimReader,
    ReadAfter { sequence: Option<u64>, max: usize },
    AckThrough { epoch: u64, sequence: u64 },
}
