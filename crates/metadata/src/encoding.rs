//! Metadata keys use `[key version: u8, record type: u8, ...record fields]`.
//! Catalog IDs use OpenData's escaped, zero-terminated bytes encoding.
//! Catalog values contain a fixed-width 16-byte ULID.

use bytes::{Bytes, BytesMut};
use common::serde::terminated_bytes;
use ulid::Ulid;

use crate::{CatalogId, Result};

const KEY_VERSION: u8 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum RecordType {
    Catalog = 0x01,
}

pub(super) struct CatalogKey<'a> {
    pub id: &'a CatalogId,
}

pub(super) struct CatalogValue {
    pub secret_id: Ulid,
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

#[cfg(test)]
mod tests {
    use ulid::Ulid;

    use super::{CatalogKey, CatalogValue};
    use crate::CatalogId;

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
