//! Binary values use little-endian numbers, u32 lengths, and explicit one-byte tags.
//! Only Iceberg's external commit payloads use length-prefixed JSON.

use std::collections::BTreeMap;

use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use iceberg::{NamespaceIdent, TableIdent, spec::DataFileFormat};
use ulid::Ulid;
use uuid::Uuid;

use crate::{
    BatchMetadata, CatalogId, DataFileMetadata, DeletionVectorMetadata, DeletionVectorReplacement,
    Error, IndexDelta, MaterializationResult, PreparedCommit, ReconciliationPlan, Result, Task,
    TaskAssignment, TaskToken, VersionedDataFile, VersionedDeletionVector, WalRange,
    tasks::{MaterializationReceipt, Scheduled, TableWork, Target, TaskKind},
};

pub(crate) struct CatalogValue {
    pub secret_id: Ulid,
}

#[derive(Default)]
pub(crate) struct WalStateValue {
    pub next_sequence: u64,
    pub materializer_epoch: u64,
}

pub(crate) trait Value: Sized {
    fn encode_into(&self, bytes: &mut BytesMut) -> Result<()>;
    fn decode_from(bytes: &mut &[u8]) -> Result<Self>;
}

struct Encoder<'a>(&'a mut BytesMut);

struct Decoder<'a, 'b>(&'a mut &'b [u8]);

/// Encodes a metadata value in its binary persistence format.
pub(crate) fn encode<T: Value>(value: &T) -> Result<Bytes> {
    let mut bytes = BytesMut::new();
    value.encode_into(&mut bytes)?;
    Ok(bytes.freeze())
}

/// Decodes a complete metadata value, rejecting trailing bytes.
pub(crate) fn decode<T: Value>(mut bytes: &[u8]) -> Result<T> {
    let value = T::decode_from(&mut bytes)?;
    if !bytes.is_empty() {
        return Err(Error::InvalidEncoding("trailing bytes"));
    }
    Ok(value)
}

macro_rules! number {
    ($ty:ty, $put:ident, $size:literal) => {
        impl Value for $ty {
            fn encode_into(&self, bytes: &mut BytesMut) -> Result<()> {
                bytes.$put(*self);
                Ok(())
            }

            fn decode_from(bytes: &mut &[u8]) -> Result<Self> {
                let raw = Decoder(bytes).take($size)?;
                let mut array = [0; $size];
                array.copy_from_slice(raw);
                Ok(Self::from_le_bytes(array))
            }
        }
    };
}

number!(u8, put_u8, 1);
number!(u32, put_u32_le, 4);
number!(i32, put_i32_le, 4);
number!(u64, put_u64_le, 8);
number!(i64, put_i64_le, 8);

macro_rules! record {
    ($ty:ident { $($field:ident),* $(,)? }) => {
        impl Value for $ty {
            fn encode_into(&self, bytes: &mut BytesMut) -> Result<()> {
                let mut encoder = Encoder(bytes);
                $(encoder.field(&self.$field)?;)*
                Ok(())
            }

            fn decode_from(bytes: &mut &[u8]) -> Result<Self> {
                let mut decoder = Decoder(bytes);
                Ok(Self { $($field: decoder.field()?,)* })
            }
        }
    };
}

record!(CatalogValue { secret_id });
record!(WalStateValue {
    next_sequence,
    materializer_epoch
});
record!(BatchMetadata {
    schema_id,
    timestamp,
    wal_size_bytes,
    location
});
record!(TableWork {
    table_ident,
    deletion,
    checkpoint_interval_sec,
    materialized_next,
    desired_revision,
    reconciled_revision,
    reconciled_snapshot_id,
    materialization,
    reconciliation,
    reconciliation_plan,
});
record!(Scheduled {
    eligible_at,
    assignment
});
record!(Target {
    catalog_id,
    table_uuid,
    kind
});
record!(TaskToken {
    worker_id,
    catalog_id,
    table_uuid,
    table_ident,
    attempt_id
});
record!(TaskAssignment {
    token,
    eligible_at,
    expires_at,
    task
});
record!(WalRange { start, end });
record!(ReconciliationPlan {
    id,
    base_revision,
    target_revision,
    base_snapshot_id
});
record!(VersionedDataFile {
    metadata,
    schema_id,
    added_revision,
    removed_revision
});
record!(VersionedDeletionVector {
    metadata,
    added_revision,
    removed_revision
});
record!(DataFileMetadata {
    path,
    format,
    size,
    num_rows,
    partition_spec_id,
    partition,
    sort_order_id,
    column_sizes,
    value_counts,
    null_value_counts,
    nan_value_counts,
    lower_bounds,
    upper_bounds,
});
record!(DeletionVectorMetadata {
    puffin_path,
    target_file,
    cardinality,
    file_size,
    offset,
    length
});
record!(DeletionVectorReplacement { previous, current });
record!(MaterializationResult {
    range,
    schema_id,
    index_delta,
    data_files,
    deletion_vectors
});
record!(IndexDelta { id, range });
record!(MaterializationReceipt { token, result });

impl Value for String {
    fn encode_into(&self, bytes: &mut BytesMut) -> Result<()> {
        Encoder(bytes).blob(self.as_bytes())
    }

    fn decode_from(bytes: &mut &[u8]) -> Result<Self> {
        let value = Decoder(bytes).blob()?;
        String::from_utf8(value.to_vec()).map_err(|_| Error::InvalidEncoding("invalid UTF-8"))
    }
}

impl<T: Value> Value for Option<T> {
    fn encode_into(&self, bytes: &mut BytesMut) -> Result<()> {
        let mut encoder = Encoder(bytes);
        match self {
            None => encoder.field(&0_u8),
            Some(value) => {
                encoder.field(&1_u8)?;
                encoder.field(value)
            }
        }
    }

    fn decode_from(bytes: &mut &[u8]) -> Result<Self> {
        let mut decoder = Decoder(bytes);
        match decoder.field::<u8>()? {
            0 => Ok(None),
            1 => Ok(Some(decoder.field()?)),
            _ => Err(Error::InvalidEncoding("invalid option tag")),
        }
    }
}

impl<T: Value> Value for Vec<T> {
    fn encode_into(&self, bytes: &mut BytesMut) -> Result<()> {
        let mut encoder = Encoder(bytes);
        encoder.length(self.len())?;
        for value in self {
            encoder.field(value)?;
        }
        Ok(())
    }

    fn decode_from(bytes: &mut &[u8]) -> Result<Self> {
        let mut decoder = Decoder(bytes);
        let count = decoder.length()?;
        let mut values = Vec::new();
        for _ in 0..count {
            values.push(decoder.field()?);
        }
        Ok(values)
    }
}

impl<T: Value> Value for BTreeMap<i32, T> {
    fn encode_into(&self, bytes: &mut BytesMut) -> Result<()> {
        let mut encoder = Encoder(bytes);
        encoder.length(self.len())?;
        for (key, value) in self {
            encoder.field(key)?;
            encoder.field(value)?;
        }
        Ok(())
    }

    fn decode_from(bytes: &mut &[u8]) -> Result<Self> {
        let mut decoder = Decoder(bytes);
        let count = decoder.length()?;
        let mut values = BTreeMap::new();
        for _ in 0..count {
            let key = decoder.field()?;
            let value = decoder.field()?;
            if values.insert(key, value).is_some() {
                return Err(Error::InvalidEncoding("duplicate map key"));
            }
        }
        Ok(values)
    }
}

impl Value for Uuid {
    fn encode_into(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.extend_from_slice(self.as_bytes());
        Ok(())
    }

    fn decode_from(bytes: &mut &[u8]) -> Result<Self> {
        let mut raw = [0; 16];
        raw.copy_from_slice(Decoder(bytes).take(16)?);
        Ok(Self::from_bytes(raw))
    }
}

impl Value for Ulid {
    fn encode_into(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.extend_from_slice(&self.to_bytes());
        Ok(())
    }

    fn decode_from(bytes: &mut &[u8]) -> Result<Self> {
        let mut raw = [0; 16];
        raw.copy_from_slice(Decoder(bytes).take(16)?);
        Ok(Self::from_bytes(raw))
    }
}

impl Value for DateTime<Utc> {
    fn encode_into(&self, bytes: &mut BytesMut) -> Result<()> {
        let mut encoder = Encoder(bytes);
        encoder.field(&self.timestamp())?;
        encoder.field(&self.timestamp_subsec_nanos())
    }

    fn decode_from(bytes: &mut &[u8]) -> Result<Self> {
        let mut decoder = Decoder(bytes);
        let seconds = decoder.field()?;
        let nanos = decoder.field()?;
        Self::from_timestamp(seconds, nanos).ok_or(Error::InvalidEncoding("invalid timestamp"))
    }
}

impl Value for CatalogId {
    fn encode_into(&self, bytes: &mut BytesMut) -> Result<()> {
        self.as_uuid().encode_into(bytes)
    }

    fn decode_from(bytes: &mut &[u8]) -> Result<Self> {
        Ok(Self::from(Uuid::decode_from(bytes)?))
    }
}

impl Value for TableIdent {
    fn encode_into(&self, bytes: &mut BytesMut) -> Result<()> {
        let mut encoder = Encoder(bytes);
        encoder.field(self.namespace.as_ref())?;
        encoder.field(&self.name)
    }

    fn decode_from(bytes: &mut &[u8]) -> Result<Self> {
        let mut decoder = Decoder(bytes);
        let namespace = NamespaceIdent::from_vec(decoder.field()?)
            .map_err(|_| Error::InvalidEncoding("invalid namespace"))?;
        Ok(Self::new(namespace, decoder.field()?))
    }
}

impl Value for DataFileFormat {
    fn encode_into(&self, bytes: &mut BytesMut) -> Result<()> {
        let tag: u8 = match self {
            Self::Avro => 0,
            Self::Orc => 1,
            Self::Parquet => 2,
            Self::Puffin => 3,
        };
        tag.encode_into(bytes)
    }

    fn decode_from(bytes: &mut &[u8]) -> Result<Self> {
        match u8::decode_from(bytes)? {
            0 => Ok(Self::Avro),
            1 => Ok(Self::Orc),
            2 => Ok(Self::Parquet),
            3 => Ok(Self::Puffin),
            _ => Err(Error::InvalidEncoding("invalid data file format")),
        }
    }
}

impl Value for TaskKind {
    fn encode_into(&self, bytes: &mut BytesMut) -> Result<()> {
        let tag: u8 = match self {
            Self::Materialize => 0,
            Self::Reconcile => 1,
            Self::DeleteTable => 2,
        };
        tag.encode_into(bytes)
    }

    fn decode_from(bytes: &mut &[u8]) -> Result<Self> {
        match u8::decode_from(bytes)? {
            0 => Ok(Self::Materialize),
            1 => Ok(Self::Reconcile),
            2 => Ok(Self::DeleteTable),
            _ => Err(Error::InvalidEncoding("invalid task kind")),
        }
    }
}

impl Value for Task {
    fn encode_into(&self, bytes: &mut BytesMut) -> Result<()> {
        let mut encoder = Encoder(bytes);
        match self {
            Self::Materialize {
                epoch,
                range,
                schema_id,
            } => {
                encoder.field(&0_u8)?;
                encoder.field(epoch)?;
                encoder.field(range)?;
                encoder.field(schema_id)
            }
            Self::Reconcile { plan } => {
                encoder.field(&1_u8)?;
                encoder.field(plan)
            }
            Self::DeleteTable => encoder.field(&2_u8),
        }
    }

    fn decode_from(bytes: &mut &[u8]) -> Result<Self> {
        let mut decoder = Decoder(bytes);
        match decoder.field::<u8>()? {
            0 => Ok(Self::Materialize {
                epoch: decoder.field()?,
                range: decoder.field()?,
                schema_id: decoder.field()?,
            }),
            1 => Ok(Self::Reconcile {
                plan: decoder.field()?,
            }),
            2 => Ok(Self::DeleteTable),
            _ => Err(Error::InvalidEncoding("invalid task tag")),
        }
    }
}

impl Value for PreparedCommit {
    fn encode_into(&self, bytes: &mut BytesMut) -> Result<()> {
        let mut encoder = Encoder(bytes);
        // Iceberg owns these complex serde schemas; all metadata-owned framing remains binary.
        encoder.blob(&serde_json::to_vec(&self.updates).map_err(Error::PreparedCommitEncoding)?)?;
        encoder
            .blob(&serde_json::to_vec(&self.requirements).map_err(Error::PreparedCommitEncoding)?)
    }

    fn decode_from(bytes: &mut &[u8]) -> Result<Self> {
        let mut decoder = Decoder(bytes);
        Ok(Self {
            updates: serde_json::from_slice(decoder.blob()?)
                .map_err(Error::PreparedCommitEncoding)?,
            requirements: serde_json::from_slice(decoder.blob()?)
                .map_err(Error::PreparedCommitEncoding)?,
        })
    }
}

impl Encoder<'_> {
    fn field<T: Value>(&mut self, value: &T) -> Result<()> {
        value.encode_into(self.0)
    }

    fn length(&mut self, length: usize) -> Result<()> {
        self.field(
            &u32::try_from(length).map_err(|_| Error::InvalidEncoding("length exceeds u32"))?,
        )
    }

    fn blob(&mut self, bytes: &[u8]) -> Result<()> {
        self.length(bytes.len())?;
        self.0.extend_from_slice(bytes);
        Ok(())
    }
}

impl<'b> Decoder<'_, 'b> {
    fn field<T: Value>(&mut self) -> Result<T> {
        T::decode_from(self.0)
    }

    fn take(&mut self, length: usize) -> Result<&'b [u8]> {
        if length > self.0.len() {
            return Err(Error::InvalidEncoding("truncated value"));
        }
        let (value, rest) = self.0.split_at(length);
        *self.0 = rest;
        Ok(value)
    }

    fn length(&mut self) -> Result<usize> {
        let length = usize::try_from(self.field::<u32>()?)
            .map_err(|_| Error::InvalidEncoding("length exceeds usize"))?;
        // Every collection element consumes at least one byte; never allocate from an unchecked count.
        if length > self.0.len() {
            return Err(Error::InvalidEncoding("truncated collection"));
        }
        Ok(length)
    }

    fn blob(&mut self) -> Result<&'b [u8]> {
        let length = self.length()?;
        self.take(length)
    }
}

#[cfg(test)]
mod tests {
    use iceberg::{TableRequirement, TableUpdate};

    use super::*;

    #[test]
    fn wal_and_catalog_values_have_explicit_binary_layouts() -> Result<()> {
        let timestamp = DateTime::from_timestamp(-123, 456).unwrap();
        let batch = BatchMetadata {
            schema_id: -17,
            location: "wal/\0é".into(),
            timestamp,
            wal_size_bytes: 0x0102030405060708,
        };
        let encoded = encode(&batch)?;

        insta::assert_snapshot!(hex::encode(&encoded), @"efffffff85ffffffffffffffc801000008070605040302010700000077616c2f00c3a9");
        assert_eq!(decode::<BatchMetadata>(&encoded)?, batch);

        for len in 0..encoded.len() {
            assert!(decode::<BatchMetadata>(&encoded[..len]).is_err());
        }

        assert!(decode::<BatchMetadata>(&[encoded.as_ref(), &[0]].concat()).is_err());

        let secret_id = Ulid::from_bytes([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ]);
        let value = encode(&CatalogValue { secret_id })?;

        insta::assert_snapshot!(hex::encode(&value), @"0123456789abcdeffedcba9876543210");
        assert_eq!(decode::<CatalogValue>(&value)?.secret_id, secret_id);

        for len in [0, 15, 17] {
            assert!(decode::<CatalogValue>(&vec![0; len]).is_err());
        }

        Ok(())
    }

    #[test]
    fn nested_materialization_receipt_roundtrips() -> Result<()> {
        let receipt = MaterializationReceipt {
            token: TaskToken {
                worker_id: Uuid::from_u128(1),
                catalog_id: CatalogId::from(Uuid::from_u128(5)),
                table_uuid: Uuid::from_u128(2),
                table_ident: TableIdent::new(
                    NamespaceIdent::from_vec(vec!["a.b".into(), "c\0é".into()])?,
                    "table.name".into(),
                ),
                attempt_id: Uuid::from_u128(3),
            },
            result: MaterializationResult {
                range: WalRange { start: 5, end: 9 },
                schema_id: -7,
                index_delta: Some(Uuid::from_u128(4)),
                data_files: vec![DataFileMetadata {
                    path: "data/é\0.parquet".into(),
                    format: DataFileFormat::Parquet,
                    size: u64::MAX,
                    num_rows: 12,
                    partition_spec_id: -1,
                    partition: BTreeMap::from([
                        (-3, None),
                        (1, Some(vec![0, 255, 1])),
                        (2, Some(vec![])),
                    ]),
                    sort_order_id: Some(i64::MIN),
                    column_sizes: BTreeMap::from([(1, 89)]),
                    value_counts: BTreeMap::from([(1, 12)]),
                    null_value_counts: BTreeMap::from([(1, 2)]),
                    nan_value_counts: BTreeMap::from([(1, 1)]),
                    lower_bounds: BTreeMap::from([(1, vec![0, 1])]),
                    upper_bounds: BTreeMap::from([(1, vec![254, 255])]),
                }],
                deletion_vectors: vec![DeletionVectorReplacement {
                    previous: Some(DeletionVectorMetadata {
                        puffin_path: "old.puffin".into(),
                        target_file: "existing.parquet".into(),
                        cardinality: 1,
                        file_size: 100,
                        offset: 4,
                        length: 20,
                    }),
                    current: DeletionVectorMetadata {
                        puffin_path: "new.puffin".into(),
                        target_file: "existing.parquet".into(),
                        cardinality: 2,
                        file_size: 120,
                        offset: 8,
                        length: 30,
                    },
                }],
            },
        };

        let bytes = encode(&receipt)?;
        let decoded: MaterializationReceipt = decode(&bytes)?;

        assert_eq!(decoded.token, receipt.token);
        assert_eq!(decoded.result, receipt.result);

        let file = VersionedDataFile {
            metadata: receipt.result.data_files[0].clone(),
            schema_id: -7,
            added_revision: 99,
            removed_revision: Some(100),
        };

        assert_eq!(decode::<VersionedDataFile>(&encode(&file)?)?, file);

        let vector = VersionedDeletionVector {
            metadata: receipt.result.deletion_vectors[0].current.clone(),
            added_revision: 99,
            removed_revision: None,
        };

        assert_eq!(
            decode::<VersionedDeletionVector>(&encode(&vector)?)?,
            vector
        );

        let delta = IndexDelta {
            id: Uuid::from_u128(4),
            range: receipt.result.range,
        };

        assert_eq!(decode::<IndexDelta>(&encode(&delta)?)?, delta);

        for end in 0..bytes.len() {
            assert!(decode::<MaterializationReceipt>(&bytes[..end]).is_err());
        }

        assert!(decode::<MaterializationReceipt>(&[bytes.as_ref(), &[0]].concat()).is_err());

        Ok(())
    }

    #[test]
    fn work_assignments_and_all_enum_variants_roundtrip() -> Result<()> {
        let time = DateTime::from_timestamp(-123, 987_654_321)
            .ok_or(Error::InvalidEncoding("fixture timestamp"))?;
        let plan = ReconciliationPlan {
            id: Uuid::from_u128(1),
            base_revision: 3,
            target_revision: 7,
            base_snapshot_id: Some(i64::MIN),
        };
        let token = TaskToken {
            worker_id: Uuid::from_u128(2),
            catalog_id: CatalogId::from(Uuid::from_u128(1)),
            table_uuid: Uuid::from_u128(3),
            table_ident: TableIdent::new(NamespaceIdent::new("ns".into()), "table".into()),
            attempt_id: Uuid::from_u128(4),
        };

        for task in [
            Task::DeleteTable,
            Task::Reconcile { plan: plan.clone() },
            Task::Materialize {
                epoch: u64::MAX,
                range: WalRange { start: 1, end: 2 },
                schema_id: -1,
            },
        ] {
            let assignment = TaskAssignment {
                token: token.clone(),
                eligible_at: Some(time),
                expires_at: time,
                task,
            };

            assert_eq!(decode::<TaskAssignment>(&encode(&assignment)?)?, assignment);

            let work = TableWork {
                table_ident: token.table_ident.clone(),
                deletion: Some(Scheduled {
                    eligible_at: time,
                    assignment: None,
                }),
                checkpoint_interval_sec: 60,
                materialized_next: 7,
                desired_revision: 8,
                reconciled_revision: 3,
                reconciled_snapshot_id: Some(-10),
                materialization: Some(Scheduled {
                    eligible_at: time,
                    assignment: Some(assignment),
                }),
                reconciliation: None,
                reconciliation_plan: Some(plan.clone()),
            };
            let bytes = encode(&work)?;

            assert_eq!(encode(&decode::<TableWork>(&bytes)?)?, bytes);
        }

        for kind in [
            TaskKind::Materialize,
            TaskKind::Reconcile,
            TaskKind::DeleteTable,
        ] {
            let target = Target {
                catalog_id: token.catalog_id.clone(),
                table_uuid: token.table_uuid,
                kind,
            };
            let bytes = encode(&target)?;

            assert_eq!(encode(&decode::<Target>(&bytes)?)?, bytes);
        }

        for format in [
            DataFileFormat::Avro,
            DataFileFormat::Orc,
            DataFileFormat::Parquet,
            DataFileFormat::Puffin,
        ] {
            assert_eq!(decode::<DataFileFormat>(&encode(&format)?)?, format);
        }

        insta::assert_snapshot!(hex::encode(encode(&time)?), @"85ffffffffffffffb168de3a");

        Ok(())
    }

    #[test]
    fn prepared_commit_frames_external_iceberg_payloads() -> Result<()> {
        let commit = PreparedCommit {
            updates: vec![TableUpdate::SetProperties {
                updates: [("property".into(), "value\0é".into())].into(),
            }],
            requirements: vec![
                TableRequirement::UuidMatch {
                    uuid: Uuid::from_u128(123),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: "main".into(),
                    snapshot_id: Some(-55),
                },
            ],
        };

        let bytes = encode(&commit)?;

        assert_eq!(decode::<PreparedCommit>(&bytes)?, commit);

        let empty = PreparedCommit {
            updates: vec![],
            requirements: vec![],
        };

        insta::assert_snapshot!(hex::encode(encode(&empty)?), @"020000005b5d020000005b5d");

        for end in 0..bytes.len() {
            assert!(decode::<PreparedCommit>(&bytes[..end]).is_err());
        }

        assert!(decode::<PreparedCommit>(&[bytes.as_ref(), &[0]].concat()).is_err());

        assert!(matches!(
            decode::<PreparedCommit>(&[1, 0, 0, 0, b'!']),
            Err(Error::PreparedCommitEncoding(_))
        ));

        Ok(())
    }

    #[test]
    fn malformed_values_are_rejected() {
        let invalid_time = [0_i64.to_le_bytes().as_slice(), &u32::MAX.to_le_bytes()].concat();
        let out_of_range_time = [i64::MAX.to_le_bytes().as_slice(), &0_u32.to_le_bytes()].concat();
        let duplicate_map = [
            2_u32.to_le_bytes().as_slice(),
            &1_i32.to_le_bytes(),
            &[0],
            &1_i32.to_le_bytes(),
            &[1],
        ]
        .concat();

        assert!(matches!(
            decode::<Option<u64>>(&[2]),
            Err(Error::InvalidEncoding(_))
        ));

        assert!(matches!(
            decode::<Task>(&[3]),
            Err(Error::InvalidEncoding(_))
        ));

        assert!(matches!(
            decode::<TaskKind>(&[3]),
            Err(Error::InvalidEncoding(_))
        ));

        assert!(matches!(
            decode::<DataFileFormat>(&[4]),
            Err(Error::InvalidEncoding(_))
        ));

        assert!(matches!(
            decode::<DateTime<Utc>>(&invalid_time),
            Err(Error::InvalidEncoding(_))
        ));

        assert!(matches!(
            decode::<DateTime<Utc>>(&out_of_range_time),
            Err(Error::InvalidEncoding(_))
        ));

        assert!(matches!(
            decode::<String>(&[1, 0, 0, 0, 255]),
            Err(Error::InvalidEncoding(_))
        ));

        assert!(matches!(
            decode::<Vec<u8>>(&u32::MAX.to_le_bytes()),
            Err(Error::InvalidEncoding(_))
        ));

        assert!(matches!(
            decode::<BTreeMap<i32, u8>>(&duplicate_map),
            Err(Error::InvalidEncoding(_))
        ));

        assert!(matches!(
            decode::<TableIdent>(&[0, 0, 0, 0]),
            Err(Error::InvalidEncoding(_))
        ));

        assert!(matches!(
            decode::<CatalogId>(&[0, 0, 0, 0]),
            Err(Error::InvalidEncoding(_))
        ));
    }
}
