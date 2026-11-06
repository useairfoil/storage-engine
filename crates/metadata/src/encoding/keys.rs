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
const GLOBAL_SCOPE: u8 = 0x00;
const TABLE_SCOPE: u8 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum GlobalRecordType {
    // Key: CatalogKey; value: CatalogValue.
    Catalog = 0x01,
    // Key: ReadyWorkKey; value: Target.
    ReadyWork = 0x02,
    // Key: WorkerAssignmentKey; value: TaskAssignment.
    WorkerAssignment = 0x03,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum TableRecordType {
    // Key: TableKey::record(WalState); value: WalStateValue.
    WalState = 0x01,
    // Key: TableKey::wal_batch(sequence: u64); value: BatchMetadata.
    WalBatch = 0x02,
    // Key: TableKey::record(TableWork); value: TableWork.
    TableWork = 0x03,
    // Key: TableKey::data_file(path: &str); value: VersionedDataFile.
    DataFile = 0x04,
    // Key: TableKey::deletion_vector(target: &str, revision: u64); value: VersionedDeletionVector.
    DeletionVector = 0x05,
    // Key: TableKey::current_deletion_vector(target: &str); value: VersionedDeletionVector.
    CurrentDeletionVector = 0x06,
    // Key: TableKey::record(PreparedCommit); value: PreparedCommit.
    PreparedCommit = 0x07,
    // Key: TableKey::index_delta(start: u64); value: IndexDelta.
    IndexDelta = 0x08,
    // Key: TableKey::materialization_receipt(attempt_id: Uuid); value: MaterializationReceipt.
    MaterializationReceipt = 0x09,
    // Key: TableKey::wal_deletion(sequence: u64); value: WalDeletion.
    WalDeletion = 0x0a,
    // Key: TableKey::wal_cleanup_receipt(attempt_id: Uuid); value: WalCleanupReceipt.
    WalCleanupReceipt = 0x0b,
    // Key: TableKey::receipt_prune_cursor(kind); value: raw receipt key suffix.
    ReceiptPruneCursor = 0x0c,
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
        let mut bytes = GlobalRecordType::Catalog.prefix();
        bytes.extend_from_slice(self.id.as_uuid().as_bytes());
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

    pub fn record(&self, kind: TableRecordType) -> Bytes {
        self.buffer(kind).freeze()
    }

    pub fn scan(&self, kind: TableRecordType) -> Scan {
        Scan {
            prefix: self.record(kind),
            suffix: (Bound::Unbounded, Bound::Unbounded),
        }
    }

    pub fn wal_batch(&self, sequence: u64) -> Bytes {
        self.numbered(TableRecordType::WalBatch, sequence)
    }

    pub fn wal_deletion(&self, sequence: u64) -> Bytes {
        self.numbered(TableRecordType::WalDeletion, sequence)
    }

    pub fn wal_batches(&self, range: impl RangeBounds<u64>) -> Scan {
        self.numbered_scan(TableRecordType::WalBatch, range)
    }

    pub fn wal_sequence(&self, key: &[u8]) -> Result<u64> {
        let prefix = self.record(TableRecordType::WalBatch);
        let suffix = key
            .strip_prefix(prefix.as_ref())
            .ok_or(Error::InvalidEncoding("invalid WAL key prefix"))?;
        Ok(u64::from_be_bytes(suffix.try_into().map_err(|_| {
            Error::InvalidEncoding("invalid WAL sequence")
        })?))
    }

    pub fn data_file(&self, path: &str) -> Bytes {
        self.file(TableRecordType::DataFile, path).freeze()
    }

    pub fn current_deletion_vector(&self, target: &str) -> Bytes {
        self.file(TableRecordType::CurrentDeletionVector, target)
            .freeze()
    }

    pub fn deletion_vector(&self, target: &str, revision: u64) -> Bytes {
        let mut bytes = self.file(TableRecordType::DeletionVector, target);
        bytes.put_u64(revision);
        bytes.freeze()
    }

    pub fn index_delta(&self, start: u64) -> Bytes {
        self.numbered(TableRecordType::IndexDelta, start)
    }

    pub fn index_deltas_before(&self, sequence: u64) -> Scan {
        self.numbered_scan(TableRecordType::IndexDelta, ..sequence)
    }

    pub fn materialization_receipt(&self, attempt_id: Uuid) -> Bytes {
        let mut bytes = self.buffer(TableRecordType::MaterializationReceipt);
        bytes.extend_from_slice(attempt_id.as_bytes());
        bytes.freeze()
    }

    pub fn wal_cleanup_receipt(&self, attempt_id: Uuid) -> Bytes {
        let mut bytes = self.buffer(TableRecordType::WalCleanupReceipt);
        bytes.extend_from_slice(attempt_id.as_bytes());
        bytes.freeze()
    }

    pub fn receipt_prune_cursor(&self, kind: TableRecordType) -> Bytes {
        let mut bytes = self.buffer(TableRecordType::ReceiptPruneCursor);
        bytes.put_u8(kind as u8);
        bytes.freeze()
    }

    pub fn records(&self) -> Scan {
        Scan {
            prefix: self.prefix().freeze(),
            suffix: (Bound::Unbounded, Bound::Unbounded),
        }
    }

    fn prefix(&self) -> BytesMut {
        let mut bytes = BytesMut::from([TABLE_SCOPE].as_slice());
        bytes.extend_from_slice(self.catalog_id.as_uuid().as_bytes());
        bytes.extend_from_slice(self.table_uuid.as_bytes());
        bytes
    }

    fn buffer(&self, kind: TableRecordType) -> BytesMut {
        let mut bytes = self.prefix();
        bytes.put_u8(KEY_VERSION);
        bytes.put_u8(kind as u8);
        bytes
    }

    fn file(&self, kind: TableRecordType, path: &str) -> BytesMut {
        let mut bytes = self.buffer(kind);
        terminated_bytes::serialize(path.as_bytes(), &mut bytes);
        bytes
    }

    fn numbered(&self, kind: TableRecordType, number: u64) -> Bytes {
        let mut bytes = self.buffer(kind);
        bytes.put_u64(number);
        bytes.freeze()
    }

    fn numbered_scan(&self, kind: TableRecordType, range: impl RangeBounds<u64>) -> Scan {
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
        let mut bytes = GlobalRecordType::ReadyWork.prefix();
        bytes.put_u64(encode_i64_sortable(self.at.timestamp()));
        bytes.put_u32(self.at.timestamp_subsec_nanos());
        bytes.extend_from_slice(self.target.catalog_id.as_uuid().as_bytes());
        bytes.extend_from_slice(self.target.table_uuid.as_bytes());
        bytes.put_u8(match self.target.kind {
            TaskKind::Materialize => 0,
            TaskKind::Reconcile => 1,
            TaskKind::DeleteTable => 2,
            TaskKind::GarbageCollectWal => 3,
        });
        bytes.freeze()
    }

    pub fn scan() -> Scan {
        Scan {
            prefix: GlobalRecordType::ReadyWork.prefix().freeze(),
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
        let mut bytes = GlobalRecordType::WorkerAssignment.prefix();
        bytes.extend_from_slice(self.worker_id.as_bytes());
        bytes.freeze()
    }
}

impl GlobalRecordType {
    fn prefix(self) -> BytesMut {
        BytesMut::from([KEY_VERSION, GLOBAL_SCOPE, self as u8].as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scans_bound_sequences_and_isolate_tables() -> Result<()> {
        let catalog = CatalogId::from(Uuid::from_u128(1));
        let other_catalog = CatalogId::from(Uuid::from_u128(2));
        let key = CatalogKey { id: &catalog }.encode();

        insta::assert_snapshot!(hex::encode(&key), @"01000100000000000000000000000000000001");

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
    fn table_prefix_groups_record_families_and_isolates_scopes() -> Result<()> {
        let catalog = CatalogId::from(Uuid::from_u128(1));
        let neighbor_catalog = CatalogId::from(Uuid::from_u128(2));
        let table = TableKey {
            catalog_id: &catalog,
            table_uuid: Uuid::from_bytes([0x11; 16]),
        };
        let neighbor = TableKey {
            table_uuid: Uuid::from_bytes([0x12; 16]),
            ..table
        };
        let other_catalog = TableKey {
            catalog_id: &neighbor_catalog,
            ..table
        };
        let scan = table.records();

        insta::assert_snapshot!(hex::encode(&scan.prefix), @"010000000000000000000000000000000111111111111111111111111111111111");
        insta::assert_snapshot!(hex::encode(table.wal_batch(257)), @"01000000000000000000000000000000011111111111111111111111111111111101020000000000000101");

        let range = absolute_range(scan);
        let keys = [
            table.record(TableRecordType::WalState),
            table.wal_batch(257),
            table.record(TableRecordType::TableWork),
            table.data_file("data/\0é"),
            table.deletion_vector("data/\0é", 256),
            table.current_deletion_vector("data/\0é"),
            table.record(TableRecordType::PreparedCommit),
            table.index_delta(255),
            table.materialization_receipt(Uuid::from_u128(23)),
        ];

        for key in &keys {
            assert!(range.contains(key));
            assert!(!absolute_range(neighbor.records()).contains(key));
            assert!(!absolute_range(other_catalog.records()).contains(key));
        }

        let version_offset = table.records().prefix.len();
        let versions_in_range: Vec<_> = [0x00, 0x01, 0x02, 0xff]
            .into_iter()
            .map(|version| {
                let mut key = table.wal_batch(257).to_vec();
                key[version_offset] = version;
                let key = Bytes::from(key);
                (
                    version,
                    range.contains(&key),
                    absolute_range(neighbor.records()).contains(&key),
                    absolute_range(other_catalog.records()).contains(&key),
                )
            })
            .collect();

        insta::assert_debug_snapshot!(versions_in_range, @r"
        [
            (
                0,
                true,
                false,
                false,
            ),
            (
                1,
                true,
                false,
                false,
            ),
            (
                2,
                true,
                false,
                false,
            ),
            (
                255,
                true,
                false,
                false,
            ),
        ]
        ");

        let target = Target {
            catalog_id: catalog.clone(),
            table_uuid: table.table_uuid,
            kind: TaskKind::Materialize,
        };
        let ready_key = ReadyWorkKey {
            target: &target,
            at: DateTime::UNIX_EPOCH,
        }
        .encode();
        let worker_key = WorkerAssignmentKey {
            worker_id: table.table_uuid,
        }
        .encode();

        assert!(!range.contains(&CatalogKey { id: &catalog }.encode()));
        assert!(!range.contains(&ready_key));
        assert!(!range.contains(&worker_key));

        let ready_range = absolute_range(ReadyWorkKey::scan());

        assert!(ready_range.contains(&ready_key));
        assert!(!ready_range.contains(&worker_key));

        for key in &keys {
            assert!(!ready_range.contains(key));
        }

        Ok(())
    }

    #[test]
    fn ready_work_orders_time_before_target_and_kind() -> Result<()> {
        let catalog_id = CatalogId::from(Uuid::from_u128(1));
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
