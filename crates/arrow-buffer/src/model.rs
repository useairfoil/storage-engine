use serde::{Deserialize, Serialize};

/// Compression algorithm applied to record batches in an Arrow IPC stream.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    #[default]
    None = 0,
    Zstd = 1,
}
