//! Persistent metadata schema:
//!
//! - ordered binary keys and little-endian binary values.
//! - Table keys use scope | catalog ID | table UUID | version | record type | suffix.
//! - Table-wide scans omit the version so all versions stay in one table partition.
//! - Global keys use a separate scope for catalogs and scheduler indexes.
//! - Key strings are escaped and terminated; value strings and collections have u32 lengths.
//! - UUIDs and ULIDs occupy 16 bytes; timestamps retain seconds and nanoseconds.
//! - Only embedded Iceberg commit payloads use length-prefixed JSON.

mod keys;
mod values;

pub(crate) use keys::{CatalogKey, ReadyWorkKey, TableKey, TableRecordType, WorkerAssignmentKey};
pub(crate) use values::{CatalogValue, WalStateValue, decode, encode};
