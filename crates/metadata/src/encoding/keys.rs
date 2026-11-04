use std::ops::{Bound, RangeBounds};

use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use common::serde::{sortable::encode_i64_sortable, terminated_bytes};
use uuid::Uuid;

use crate::{
    CatalogId, Error, Result, TaskToken,
    tasks::{Target, TaskKind},
};

const KEY_VERSION: u8 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum RecordType {
    // Key: CatalogKey; value: CatalogValue.
    Catalog = 0x01,
    // Key: TableKey::record(WalState); value: WalStateValue.
    WalState = 0x02,
    // Key: TableKey::wal_batch(sequence: u64); value: BatchMetadata.
    WalBatch = 0x03,
    // Key: TableKey::record(TableWork); value: TableWork.
    TableWork = 0x04,
    // Key: ReadyWorkKey; value: Target.
    ReadyWork = 0x05,
    // Key: WorkerAssignmentKey; value: TaskAssignment.
    WorkerAssignment = 0x06,
    // Key: TableKey::data_file(path: &str); value: VersionedDataFile.
    DataFile = 0x07,
    // Key: TableKey::deletion_vector(target: &str, revision: u64); value: VersionedDeletionVector.
    DeletionVector = 0x08,
    // Key: TableKey::current_deletion_vector(target: &str); value: VersionedDeletionVector.
    CurrentDeletionVector = 0x09,
    // Key: TableKey::record(PreparedCommit); value: PreparedCommit.
    PreparedCommit = 0x0a,
    // Key: TableKey::index_delta(start: u64); value: IndexDelta.
    IndexDelta = 0x0b,
    // Key: TableKey::materialization_receipt(attempt_id: Uuid); value: MaterializationReceipt.
    MaterializationReceipt = 0x0c,
}

pub(crate) struct Scan {
    pub prefix: Bytes,
    pub suffix: (Bound<Bytes>, Bound<Bytes>),
}

pub(crate) struct CatalogKey<'a> {
    pub id: &'a CatalogId,
}

#[derive(Clone, Copy)]
pub(crate) struct TableKey<'a> {
    pub catalog_id: &'a CatalogId,
    pub table_uuid: Uuid,
}

pub(crate) struct ReadyWorkKey<'a> {
    pub target: &'a Target,
    pub at: DateTime<Utc>,
}

pub(crate) struct WorkerAssignmentKey {
    pub worker_id: Uuid,
}

impl CatalogKey<'_> {
    pub fn encode(&self) -> Bytes {
        let mut bytes = RecordType::Catalog.prefix();
        terminated_bytes::serialize(self.id.as_str().as_bytes(), &mut bytes);
        bytes.freeze()
    }
}

impl<'a> TableKey<'a> {
    pub fn for_token(token: &'a TaskToken) -> Self {
        Self {
            catalog_id: &token.catalog_id,
            table_uuid: token.table_uuid,
        }
    }

    pub fn record(&self, kind: RecordType) -> Bytes {
        self.buffer(kind).freeze()
    }

    pub fn scan(&self, kind: RecordType) -> Scan {
        Scan {
            prefix: self.record(kind),
            suffix: (Bound::Unbounded, Bound::Unbounded),
        }
    }

    pub fn wal_batch(&self, sequence: u64) -> Bytes {
        self.numbered(RecordType::WalBatch, sequence)
    }

    pub fn wal_batches(&self, range: impl RangeBounds<u64>) -> Scan {
        self.numbered_scan(RecordType::WalBatch, range)
    }

    pub fn wal_sequence(&self, key: &[u8]) -> Result<u64> {
        let prefix = self.record(RecordType::WalBatch);
        let suffix = key
            .strip_prefix(prefix.as_ref())
            .ok_or(Error::InvalidEncoding("invalid WAL key prefix"))?;
        Ok(u64::from_be_bytes(suffix.try_into().map_err(|_| {
            Error::InvalidEncoding("invalid WAL sequence")
        })?))
    }

    pub fn data_file(&self, path: &str) -> Bytes {
        self.file(RecordType::DataFile, path).freeze()
    }

    pub fn current_deletion_vector(&self, target: &str) -> Bytes {
        self.file(RecordType::CurrentDeletionVector, target)
            .freeze()
    }

    pub fn deletion_vector(&self, target: &str, revision: u64) -> Bytes {
        let mut bytes = self.file(RecordType::DeletionVector, target);
        bytes.put_u64(revision);
        bytes.freeze()
    }

    pub fn index_delta(&self, start: u64) -> Bytes {
        self.numbered(RecordType::IndexDelta, start)
    }

    pub fn index_deltas_before(&self, sequence: u64) -> Scan {
        self.numbered_scan(RecordType::IndexDelta, ..sequence)
    }

    pub fn materialization_receipt(&self, attempt_id: Uuid) -> Bytes {
        let mut bytes = self.buffer(RecordType::MaterializationReceipt);
        bytes.extend_from_slice(attempt_id.as_bytes());
        bytes.freeze()
    }

    pub fn records(&self) -> impl Iterator<Item = Scan> + '_ {
        [
            RecordType::WalState,
            RecordType::WalBatch,
            RecordType::TableWork,
            RecordType::DataFile,
            RecordType::DeletionVector,
            RecordType::CurrentDeletionVector,
            RecordType::PreparedCommit,
            RecordType::IndexDelta,
            RecordType::MaterializationReceipt,
        ]
        .into_iter()
        .map(|kind| self.scan(kind))
    }

    fn buffer(&self, kind: RecordType) -> BytesMut {
        let mut bytes = kind.prefix();
        terminated_bytes::serialize(self.catalog_id.as_str().as_bytes(), &mut bytes);
        bytes.extend_from_slice(self.table_uuid.as_bytes());
        bytes
    }

    fn file(&self, kind: RecordType, path: &str) -> BytesMut {
        let mut bytes = self.buffer(kind);
        terminated_bytes::serialize(path.as_bytes(), &mut bytes);
        bytes
    }

    fn numbered(&self, kind: RecordType, number: u64) -> Bytes {
        let mut bytes = self.buffer(kind);
        bytes.put_u64(number);
        bytes.freeze()
    }

    fn numbered_scan(&self, kind: RecordType, range: impl RangeBounds<u64>) -> Scan {
        let encode = |number: &u64| Bytes::copy_from_slice(&number.to_be_bytes());
        Scan {
            prefix: self.record(kind),
            suffix: (
                range.start_bound().map(encode),
                range.end_bound().map(encode),
            ),
        }
    }
}

impl ReadyWorkKey<'_> {
    pub fn encode(&self) -> Bytes {
        let mut bytes = RecordType::ReadyWork.prefix();
        bytes.put_u64(encode_i64_sortable(self.at.timestamp()));
        bytes.put_u32(self.at.timestamp_subsec_nanos());
        terminated_bytes::serialize(self.target.catalog_id.as_str().as_bytes(), &mut bytes);
        bytes.extend_from_slice(self.target.table_uuid.as_bytes());
        bytes.put_u8(match self.target.kind {
            TaskKind::Materialize => 0,
            TaskKind::Reconcile => 1,
            TaskKind::DeleteTable => 2,
        });
        bytes.freeze()
    }

    pub fn scan() -> Scan {
        Scan {
            prefix: RecordType::ReadyWork.prefix().freeze(),
            suffix: (Bound::Unbounded, Bound::Unbounded),
        }
    }
}

impl WorkerAssignmentKey {
    pub fn for_token(token: &TaskToken) -> Self {
        Self {
            worker_id: token.worker_id,
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut bytes = RecordType::WorkerAssignment.prefix();
        bytes.extend_from_slice(self.worker_id.as_bytes());
        bytes.freeze()
    }
}

impl RecordType {
    fn prefix(self) -> BytesMut {
        BytesMut::from([KEY_VERSION, self as u8].as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scans_bound_sequences_and_isolate_tables() -> Result<()> {
        let catalog = CatalogId::new("a\0\x01é")?;
        let other_catalog = CatalogId::new("a\0\x01éx")?;
        let key = CatalogKey { id: &catalog }.encode();

        insta::assert_snapshot!(hex::encode(&key), @"01016101010102c3a900");

        let table = TableKey {
            catalog_id: &catalog,
            table_uuid: Uuid::from_u128(17),
        };
        let other = TableKey {
            catalog_id: &other_catalog,
            ..table
        };
        let scan = table.wal_batches(255..=257);
        let range = absolute_range(scan);

        for sequence in [255, 256, 257] {
            assert!(range.contains(&table.wal_batch(sequence)));
        }

        for sequence in [254, 258] {
            assert!(!range.contains(&table.wal_batch(sequence)));
        }

        for sequence in [254, 255, 256, 257, 258] {
            assert!(!range.contains(&other.wal_batch(sequence)));
        }

        for sequence in [0, 255, 256, u64::MAX] {
            assert_eq!(table.wal_sequence(&table.wal_batch(sequence))?, sequence);
        }

        let excluded = table.wal_batches((Bound::Excluded(255), Bound::Excluded(257)));
        let range = absolute_range(excluded);

        assert!(!range.contains(&table.wal_batch(255)));
        assert!(range.contains(&table.wal_batch(256)));
        assert!(!range.contains(&table.wal_batch(257)));

        Ok(())
    }

    #[test]
    fn ready_work_orders_time_before_target_and_kind() -> Result<()> {
        let catalog_id = CatalogId::new("prod")?;
        let mut target = Target {
            catalog_id,
            table_uuid: Uuid::from_u128(7),
            kind: TaskKind::Materialize,
        };

        let times = [(-1, 999_999_999), (0, 0), (0, 1), (1, 0)];
        let keys: Vec<_> = times
            .into_iter()
            .map(|(s, n)| {
                ReadyWorkKey {
                    target: &target,
                    at: DateTime::from_timestamp(s, n).unwrap(),
                }
                .encode()
            })
            .collect();

        for pair in keys.windows(2) {
            assert!(pair[0] < pair[1]);
        }

        let at = DateTime::from_timestamp(0, 0).unwrap();
        let mut kinds = Vec::new();

        for kind in [
            TaskKind::Materialize,
            TaskKind::Reconcile,
            TaskKind::DeleteTable,
        ] {
            target.kind = kind;
            kinds.push(
                ReadyWorkKey {
                    target: &target,
                    at,
                }
                .encode(),
            );
        }

        for pair in kinds.windows(2) {
            assert!(pair[0] < pair[1]);
        }

        Ok(())
    }

    fn absolute_range(scan: Scan) -> common::BytesRange {
        common::BytesRange::from_prefix_and_subrange(
            &scan.prefix,
            &common::BytesRange::new(scan.suffix.0, scan.suffix.1),
        )
    }
}
