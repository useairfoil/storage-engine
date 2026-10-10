//! Codec for batch metadata stored in the ingestion WAL.
//! The payload is exactly one little-endian `i32` schema ID.

use bytes::Bytes;

/// Metadata attached to each published batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchMetadata {
    pub schema_id: i32,
}

/// Invalid batch metadata encoding.
#[derive(Debug, thiserror::Error)]
#[error("invalid batch metadata length: expected 4 bytes, got {length}")]
pub struct DecodeError {
    pub length: usize,
}

impl BatchMetadata {
    pub fn encode(&self) -> Bytes {
        Bytes::copy_from_slice(&self.schema_id.to_le_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let bytes = bytes.try_into().map_err(|_| DecodeError {
            length: bytes.len(),
        })?;
        Ok(Self {
            schema_id: i32::from_le_bytes(bytes),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_preserves_signed_schema_ids_and_rejects_invalid_lengths() {
        let metadata = BatchMetadata {
            schema_id: -0x1234567,
        };
        let encoded = [0x99, 0xba, 0xdc, 0xfe];
        assert_eq!(metadata.encode().as_ref(), encoded);
        assert_eq!(BatchMetadata::decode(&encoded).unwrap(), metadata);
        for length in [0, 3, 5] {
            assert_eq!(
                BatchMetadata::decode(&vec![0; length]).unwrap_err().length,
                length
            );
        }
    }
}
