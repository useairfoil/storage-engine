//! Persistent metadata schema:
//!
//! - ordered binary keys and little-endian binary values.
//! - Key strings are escaped and terminated; value strings and collections have u32 lengths.
//! - UUIDs and ULIDs occupy 16 bytes; timestamps retain seconds and nanoseconds.
//! - Only embedded Iceberg commit payloads use length-prefixed JSON.

mod keys;
mod values;

pub(crate) use keys::{CatalogKey, ReadyWorkKey, RecordType, TableKey, WorkerAssignmentKey};
pub(crate) use values::{CatalogValue, WalStateValue, decode, encode};
