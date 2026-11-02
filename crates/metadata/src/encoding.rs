//! Metadata keys use `[key version: u8, record type: u8, ...record fields]`.
//! Catalog IDs use OpenData's escaped, zero-terminated bytes encoding.
//! Catalog values contain a fixed-width 16-byte ULID.
//! WAL keys contain a table UUID and big-endian sequence suffix for ordered scans.
//! WAL values use little-endian numbers and terminated locations; timestamps retain nanoseconds.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use common::serde::terminated_bytes;
use ulid::Ulid;
use uuid::Uuid;

use crate::{BatchMetadata, CatalogId, Error, Result};

const KEY_VERSION: u8 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum RecordType {
    Catalog = 0x01,
    WalState = 0x02,
    WalBatch = 0x03,
    TableWork = 0x04,
    DataFile = 0x08,
    DeletionVector = 0x09,
    CurrentDeletionVector = 0x0a,
    PreparedCommit = 0x0b,
    IndexDelta = 0x0c,
    MaterializationReceipt = 0x0d,
}

#[derive(Debug)]
pub(super) struct CatalogKey<'a> {
    pub id: &'a CatalogId,
}

#[derive(Debug)]
pub(super) struct CatalogValue {
    pub secret_id: Ulid,
}

#[derive(Debug)]
pub(super) struct WalStateKey<'a> {
    pub catalog_id: &'a CatalogId,
    pub table_uuid: Uuid,
}

#[derive(Debug)]
pub(super) struct WalBatchKey<'a> {
    pub catalog_id: &'a CatalogId,
    pub table_uuid: Uuid,
    pub sequence: u64,
}

#[derive(Default, Debug)]
pub(super) struct WalStateValue {
    pub next_sequence: u64,
    pub materializer_epoch: u64,
}

#[derive(Debug)]
pub(super) struct WalBatchValue<'a> {
    pub schema_id: i32,
    pub location: &'a str,
    pub timestamp: DateTime<Utc>,
    pub wal_size_bytes: u64,
}

impl CatalogKey<'_> {
    pub fn encode(&self) -> Bytes {
        let id = self.id.as_str().as_bytes();
        let mut bytes = BytesMut::with_capacity(3 + id.len());

        bytes.extend_from_slice(&[KEY_VERSION, RecordType::Catalog as u8]);
        terminated_bytes::serialize(id, &mut bytes);

        bytes.freeze()
    }
}

impl CatalogValue {
    pub fn encode(&self) -> Bytes {
        Bytes::copy_from_slice(&self.secret_id.to_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            secret_id: Ulid::from_bytes(bytes.try_into()?),
        })
    }
}

impl WalStateKey<'_> {
    pub fn encode(&self) -> Bytes {
        RecordType::WalState
            .table_key(self.catalog_id, self.table_uuid)
            .freeze()
    }
}

impl WalBatchKey<'_> {
    pub fn prefix(&self) -> Bytes {
        RecordType::WalBatch
            .table_key(self.catalog_id, self.table_uuid)
            .freeze()
    }

    pub fn encode(&self) -> Bytes {
        let mut bytes = RecordType::WalBatch.table_key(self.catalog_id, self.table_uuid);
        bytes.put_u64(self.sequence);

        bytes.freeze()
    }
}

impl WalStateValue {
    pub fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(16);
        bytes.put_u64_le(self.next_sequence);
        bytes.put_u64_le(self.materializer_epoch);

        bytes.freeze()
    }

    pub fn decode(mut bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 16 {
            return Err(Error::InvalidWalState);
        }

        Ok(Self {
            next_sequence: bytes.get_u64_le(),
            materializer_epoch: bytes.get_u64_le(),
        })
    }
}

impl WalBatchValue<'_> {
    pub fn decode(mut bytes: &[u8]) -> Result<BatchMetadata> {
        if bytes.len() < 17 {
            return Err(Error::InvalidWalBatch);
        }

        let schema_id = bytes.get_i32_le();
        let seconds = bytes.get_i64_le();
        let nanos = bytes.get_u32_le();
        let timestamp = DateTime::from_timestamp(seconds, nanos).ok_or(Error::InvalidWalBatch)?;
        let location =
            terminated_bytes::deserialize(&mut bytes).map_err(|_| Error::InvalidWalBatch)?;
        let location = String::from_utf8(location.to_vec()).map_err(|_| Error::InvalidWalBatch)?;

        let wal_size_bytes = match bytes.len() {
            0 => 0,
            8 => bytes.get_u64_le(),
            _ => return Err(Error::InvalidWalBatch),
        };

        Ok(BatchMetadata {
            schema_id,
            location,
            timestamp,
            wal_size_bytes,
        })
    }

    pub fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(25 + self.location.len());
        bytes.put_i32_le(self.schema_id);
        bytes.put_i64_le(self.timestamp.timestamp());
        bytes.put_u32_le(self.timestamp.timestamp_subsec_nanos());
        terminated_bytes::serialize(self.location.as_bytes(), &mut bytes);
        bytes.put_u64_le(self.wal_size_bytes);

        bytes.freeze()
    }
}

impl RecordType {
    pub(super) fn table_key(self, catalog_id: &CatalogId, table_uuid: Uuid) -> BytesMut {
        let mut bytes = BytesMut::with_capacity(19 + catalog_id.as_str().len());
        bytes.extend_from_slice(&[KEY_VERSION, self as u8]);
        terminated_bytes::serialize(catalog_id.as_str().as_bytes(), &mut bytes);
        bytes.extend_from_slice(table_uuid.as_bytes());

        bytes
    }
}

#[cfg(test)]
mod tests {
    use ulid::Ulid;

    use super::{CatalogKey, CatalogValue, WalBatchValue};
    use crate::CatalogId;

    #[test]
    fn wal_batch_encoding_reads_legacy_values_and_rejects_partial_sizes()
    -> Result<(), Box<dyn std::error::Error>> {
        let legacy = [
            17_i32.to_le_bytes().as_slice(),
            123_i64.to_le_bytes().as_slice(),
            456_u32.to_le_bytes().as_slice(),
            b"wal/batch\0",
        ]
        .concat();
        let batch = WalBatchValue::decode(&legacy)?;
        assert_eq!(batch.wal_size_bytes, 0);
        assert_eq!(batch.location, "wal/batch");

        let encoded = WalBatchValue {
            schema_id: batch.schema_id,
            location: &batch.location,
            timestamp: batch.timestamp,
            wal_size_bytes: 0x0102030405060708,
        }
        .encode();
        assert_eq!(
            encoded.as_ref(),
            [legacy.as_slice(), &[8, 7, 6, 5, 4, 3, 2, 1]].concat()
        );
        assert_eq!(
            WalBatchValue::decode(&encoded)?.wal_size_bytes,
            0x0102030405060708
        );

        let invalid: Vec<_> = (1..=9)
            .filter(|length| *length != 8)
            .map(|length| {
                WalBatchValue::decode(&[legacy.as_slice(), &vec![0; length]].concat()).is_err()
            })
            .collect();
        insta::assert_debug_snapshot!(invalid, @"
        [
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
        ]
        ");
        Ok(())
    }

    #[test]
    fn catalog_encoding_preserves_wire_format_and_rejects_invalid_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let id = CatalogId::new("A/\0\x01é")?;
        let key = CatalogKey { id: &id }.encode();

        insta::assert_snapshot!(format!("{:02x?}", key.as_ref()), @"[01, 01, 41, 2f, 01, 01, 01, 02, c3, a9, 00]");

        let secret_id = Ulid::from_bytes([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ]);
        let value = CatalogValue { secret_id }.encode();

        insta::assert_snapshot!(format!("{:02x?}", value.as_ref()), @"[01, 23, 45, 67, 89, ab, cd, ef, fe, dc, ba, 98, 76, 54, 32, 10]");
        assert_eq!(CatalogValue::decode(&value)?.secret_id, secret_id);

        let invalid: Vec<_> = [0, 15, 17]
            .into_iter()
            .map(|len| CatalogValue::decode(&vec![0; len]).is_err())
            .collect();

        insta::assert_debug_snapshot!(invalid, @r"
        [
            true,
            true,
            true,
        ]
        ");

        Ok(())
    }
}
