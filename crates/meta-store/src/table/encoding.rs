//! Table records use `[key version: u8, record type: u8, ...record fields]` keys.
//! The table UUID is a singleton with no key suffix and a 16-byte UUID value.
//! Numeric singleton values use fixed-width little-endian encoding.

use bytes::{BufMut, Bytes, BytesMut};
use iceberg::spec::{Schema, SortOrder};
use uuid::Uuid;

use super::{PendingCommit, PublicationIntent, Result};

/// Persist bounds as typed Iceberg bytes: JSON numbers cannot represent infinities.
pub(super) mod column_bounds {
    use std::collections::HashMap;

    use iceberg::spec::{Datum, PrimitiveType};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        bounds: &HashMap<i32, Datum>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let encoded = bounds
            .iter()
            .map(|(&id, bound)| {
                let bytes = bound.to_bytes().map_err(serde::ser::Error::custom)?;
                Ok((id, (bound.data_type().clone(), bytes.into_vec())))
            })
            .collect::<std::result::Result<HashMap<_, _>, S::Error>>()?;
        encoded.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<HashMap<i32, Datum>, D::Error> {
        let encoded = HashMap::<i32, (PrimitiveType, Vec<u8>)>::deserialize(deserializer)?;
        encoded
            .into_iter()
            .map(|(id, (data_type, bytes))| {
                let bound =
                    Datum::try_from_bytes(&bytes, data_type).map_err(serde::de::Error::custom)?;
                Ok((id, bound))
            })
            .collect()
    }
}

const KEY_VERSION: u8 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum RecordType {
    TableUuid = 0x01,
    CurrentSchemaId = 0x02,
    DefaultSortOrderId = 0x03,
    Schema = 0x04,
    SortOrder = 0x05,
    MaterializedSequence = 0x06,
    PendingCommit = 0x07,
    PublishedSequence = 0x08,
    PublicationIntent = 0x09,
    RowLocation = 0x0a,
    DeletionBitmap = 0x0b,
    DeletionVector = 0x0c,
    UpsertSchemaId = 0x0d,
}

pub(super) struct TableUuidKey;

pub(super) struct TableUuidValue {
    pub uuid: Uuid,
}

pub(super) struct CurrentSchemaIdKey;

pub(super) struct CurrentSchemaIdValue {
    pub id: i32,
}

pub(super) struct DefaultSortOrderIdKey;

pub(super) struct DefaultSortOrderIdValue {
    pub id: i64,
}

pub(super) struct SchemaKey {
    pub id: i32,
}

pub(super) struct SortOrderKey {
    pub id: i64,
}

pub(super) struct SchemaValue;

pub(super) struct SortOrderValue;

pub(super) struct MaterializedSequenceKey;
pub(super) struct MaterializedSequenceValue {
    pub sequence: u64,
}

pub(super) struct PendingCommitKey {
    pub last_sequence: u64,
}

pub(super) struct PendingCommitValue;

pub(super) struct PublishedSequenceKey;
pub(super) struct PublicationIntentKey;
pub(super) struct PublicationIntentValue;

impl RecordType {
    pub fn key(self, suffix: &[u8]) -> Bytes {
        let mut bytes = BytesMut::with_capacity(2 + suffix.len());
        bytes.extend_from_slice(&[KEY_VERSION, self as u8]);
        bytes.extend_from_slice(suffix);
        bytes.freeze()
    }
}

impl TableUuidKey {
    pub fn encode() -> Bytes {
        Bytes::from_static(&[KEY_VERSION, RecordType::TableUuid as u8])
    }
}

impl TableUuidValue {
    pub fn encode(&self) -> Bytes {
        Bytes::copy_from_slice(self.uuid.as_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            uuid: Uuid::from_slice(bytes)?,
        })
    }
}

impl CurrentSchemaIdKey {
    pub fn encode() -> Bytes {
        Bytes::from_static(&[KEY_VERSION, RecordType::CurrentSchemaId as u8])
    }
}

impl CurrentSchemaIdValue {
    pub fn encode(&self) -> Bytes {
        Bytes::copy_from_slice(&self.id.to_le_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            id: i32::from_le_bytes(bytes.try_into()?),
        })
    }
}

impl DefaultSortOrderIdKey {
    pub fn encode() -> Bytes {
        Bytes::from_static(&[KEY_VERSION, RecordType::DefaultSortOrderId as u8])
    }
}

impl DefaultSortOrderIdValue {
    pub fn encode(&self) -> Bytes {
        Bytes::copy_from_slice(&self.id.to_le_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            id: i64::from_le_bytes(bytes.try_into()?),
        })
    }
}

impl SchemaKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(6);
        buf.put_u8(KEY_VERSION);
        buf.put_u8(RecordType::Schema as u8);
        // Flip the sign bit before big-endian encoding to preserve signed ordering.
        buf.put_u32((self.id as u32) ^ 0x8000_0000);
        buf.freeze()
    }
}

impl SortOrderKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(10);
        buf.put_u8(KEY_VERSION);
        buf.put_u8(RecordType::SortOrder as u8);
        // Flip the sign bit before big-endian encoding to preserve signed ordering.
        buf.put_u64((self.id as u64) ^ 0x8000_0000_0000_0000);
        buf.freeze()
    }
}

impl SchemaValue {
    pub fn encode(schema: &Schema) -> Result<Bytes> {
        Ok(serde_json::to_vec(schema)?.into())
    }

    pub fn decode(bytes: &[u8]) -> Result<Schema> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

impl SortOrderValue {
    pub fn encode(sort_order: &SortOrder) -> Result<Bytes> {
        Ok(serde_json::to_vec(sort_order)?.into())
    }

    pub fn decode(bytes: &[u8]) -> Result<SortOrder> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

impl MaterializedSequenceKey {
    pub fn encode() -> Bytes {
        Bytes::from_static(&[KEY_VERSION, RecordType::MaterializedSequence as u8])
    }
}

impl MaterializedSequenceValue {
    pub fn encode(&self) -> Bytes {
        Bytes::copy_from_slice(&self.sequence.to_le_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            sequence: u64::from_le_bytes(bytes.try_into()?),
        })
    }
}

impl PendingCommitKey {
    pub fn prefix() -> Bytes {
        Bytes::from_static(&[KEY_VERSION, RecordType::PendingCommit as u8])
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(10);
        buf.extend_from_slice(&Self::prefix());
        buf.put_u64(self.last_sequence);
        buf.freeze()
    }
}

impl PendingCommitValue {
    pub fn encode(commit: &PendingCommit) -> Result<Bytes> {
        Ok(serde_json::to_vec(commit)?.into())
    }

    pub fn decode(bytes: &[u8]) -> Result<PendingCommit> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

impl PublishedSequenceKey {
    pub fn encode() -> Bytes {
        Bytes::from_static(&[KEY_VERSION, RecordType::PublishedSequence as u8])
    }
}

impl PublicationIntentKey {
    pub fn encode() -> Bytes {
        Bytes::from_static(&[KEY_VERSION, RecordType::PublicationIntent as u8])
    }
}

impl PublicationIntentValue {
    pub fn encode(intent: &PublicationIntent) -> Result<Bytes> {
        Ok(serde_json::to_vec(intent)?.into())
    }

    pub fn decode(bytes: &[u8]) -> Result<PublicationIntent> {
        Ok(serde_json::from_slice(bytes)?)
    }
}
