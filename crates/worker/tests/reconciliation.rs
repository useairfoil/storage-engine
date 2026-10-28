use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use iceberg::{
    TableCommit, TableIdent, TableRequirement, TableUpdate,
    io::FileIO,
    spec::{
        DataContentType, DataFileFormat, Datum, FormatVersion, NestedField, Operation,
        PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Transform, Type,
        UnboundPartitionSpec,
    },
    table::Table,
};
use se_metadata_client::{
    CatalogConfig, CatalogId, DataFileMetadata, DeletionVectorMetadata, Heartbeat,
    MaterializationResult, MetadataClient, PreparedCommit, RECONCILIATION_ID_PROPERTY,
    REVISION_PROPERTY, ReconciliationInput, ReconciliationPlan, Task, TaskAssignment, TaskToken,
    VersionedDataFile, VersionedDeletionVector,
};
use se_worker::reconciliation::prepare_reconciliation;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct RecoveryClient {
    task: TaskAssignment,
    commit: PreparedCommit,
    submitted: Mutex<Option<PreparedCommit>>,
    fail_completion: bool,
    released: AtomicUsize,
    cancel: CancellationToken,
}

#[tokio::test]
async fn worker_resubmits_saved_commit_and_releases_uncertain_publications_for_retry() -> Result<()>
{
    let table = table()?;
    let (heartbeat, _) = heartbeat();
    let prepared =
        prepare_reconciliation(&table, &plan(), &[file(&table, 0)], &[], &[], &heartbeat).await?;
    for fail_completion in [false, true] {
        let client = Arc::new(RecoveryClient {
            task: TaskAssignment {
                token: TaskToken {
                    worker_id: Uuid::from_u128(7),
                    catalog_id: CatalogId::new("test")?,
                    table_uuid: table.metadata().uuid(),
                    table_ident: table.identifier().clone(),
                    attempt_id: Uuid::from_u128(8),
                },
                expires_at: chrono::DateTime::UNIX_EPOCH + chrono::TimeDelta::seconds(30),
                task: Task::Reconcile { plan: plan() },
            },
            commit: prepared.clone(),
            submitted: Mutex::new(None),
            fail_completion,
            released: AtomicUsize::new(0),
            cancel: CancellationToken::new(),
        });
        se_worker::Worker::new(
            client.task.token.worker_id,
            client.clone(),
            Arc::new(se_common::clock::MockSystemClock::with_time(0)),
            Arc::new(object_store::memory::InMemory::new()),
            Arc::new(iceberg::io::MemoryStorageFactory),
        )
        .run(client.cancel.clone())
        .await?;
        assert_eq!(client.submitted.lock().await.as_ref(), Some(&prepared));
        assert_eq!(
            client.released.load(Ordering::SeqCst),
            usize::from(fail_completion)
        );
    }
    Ok(())
}

#[tokio::test]
async fn prepares_metrics_partition_markers_and_fenced_requirements() -> Result<()> {
    let table = table()?;
    let (heartbeat, beats) = heartbeat();
    let commit =
        prepare_reconciliation(&table, &plan(), &[file(&table, 0)], &[], &[], &heartbeat).await?;
    assert_eq!(beats.load(Ordering::SeqCst), 2);
    assert_eq!(
        commit.requirements,
        vec![
            TableRequirement::UuidMatch {
                uuid: table.metadata().uuid()
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".into(),
                snapshot_id: None
            },
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: 0
            },
            TableRequirement::DefaultSpecIdMatch { default_spec_id: 0 },
        ]
    );
    let snapshot = commit
        .updates
        .iter()
        .find_map(|update| match update {
            TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
            _ => None,
        })
        .ok_or("missing snapshot")?;
    assert_eq!(
        snapshot
            .summary()
            .additional_properties
            .get(RECONCILIATION_ID_PROPERTY),
        Some(&plan().id.to_string())
    );
    assert_eq!(
        snapshot
            .summary()
            .additional_properties
            .get(REVISION_PROPERTY),
        Some(&"1".into())
    );
    let manifests = table
        .manifest_list_reader(&Arc::new(snapshot.clone()))
        .load()
        .await?;
    let manifest = manifests
        .entries()
        .first()
        .ok_or("missing manifest")?
        .load_manifest(table.file_io())
        .await?;
    let data = manifest
        .entries()
        .first()
        .ok_or("missing file")?
        .data_file();
    assert_eq!(data.file_path(), "memory://table/data/0.parquet");
    assert_eq!(data.record_count(), 10);
    assert_eq!(data.file_size_in_bytes(), 100);
    assert_eq!(data.column_sizes().get(&1), Some(&80));
    assert_eq!(data.value_counts().get(&1), Some(&10));
    assert_eq!(data.null_value_counts().get(&1), Some(&1));
    assert_eq!(data.nan_value_counts().get(&1), Some(&0));
    assert_eq!(data.lower_bounds().get(&1), Some(&Datum::long(1)));
    assert_eq!(data.upper_bounds().get(&1), Some(&Datum::long(9)));
    assert_eq!(
        data.partition(),
        &vec![Some(Datum::long(5).into())].into_iter().collect()
    );
    // Preparing wrote manifests, not the table's catalog state.
    assert_eq!(table.metadata().current_snapshot_id(), None);
    // An expired preparation must not overwrite another attempt's manifest/list artifacts.
    let second =
        prepare_reconciliation(&table, &plan(), &[file(&table, 1)], &[], &[], &heartbeat).await?;
    let second_snapshot = second
        .updates
        .iter()
        .find_map(|update| match update {
            TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
            _ => None,
        })
        .ok_or("missing second snapshot")?;
    assert_ne!(snapshot.manifest_list(), second_snapshot.manifest_list());
    let original = table
        .manifest_list_reader(&Arc::new(snapshot.clone()))
        .load()
        .await?;
    let original = original
        .entries()
        .first()
        .ok_or("missing original manifest")?
        .load_manifest(table.file_io())
        .await?;
    assert_eq!(
        original
            .entries()
            .first()
            .ok_or("missing original file")?
            .data_file()
            .file_path(),
        "memory://table/data/0.parquet"
    );
    Ok(())
}

#[tokio::test]
async fn empty_append_publishes_progress_and_conversion_reports_bounded_progress() -> Result<()> {
    let table = table()?;
    for count in [0, 129] {
        let (heartbeat, beats) = heartbeat();
        let files = (0..count)
            .map(|index| file(&table, index))
            .collect::<Vec<_>>();
        let commit = prepare_reconciliation(&table, &plan(), &files, &[], &[], &heartbeat).await?;
        assert_eq!(beats.load(Ordering::SeqCst), count.div_ceil(128) + 1);
        assert_eq!(commit.updates.len(), 2);
    }
    Ok(())
}

#[tokio::test]
async fn publishes_and_replaces_vectors_without_removing_other_blobs() -> Result<()> {
    let mut table = table_with_layout(FormatVersion::V3, false)?;
    let (heartbeat, _) = heartbeat();
    let first = VersionedDeletionVector {
        metadata: DeletionVectorMetadata {
            puffin_path: "memory://table/shared.puffin".into(),
            target_file: "memory://table/data/0.parquet".into(),
            cardinality: 2,
            file_size: 200,
            offset: 4,
            length: 30,
        },
        added_revision: 1,
        removed_revision: None,
    };
    let mut other = first.clone();
    other.metadata.target_file = "memory://table/data/1.parquet".into();
    other.metadata.offset = 34;
    other.metadata.cardinality = 1;
    let added = [first.clone(), other.clone()];
    let commit = prepare_reconciliation(
        &table,
        &plan(),
        &[file(&table, 0), file(&table, 1)],
        &added,
        &[],
        &heartbeat,
    )
    .await?;
    let first_snapshot = match &commit.updates[0] {
        TableUpdate::AddSnapshot { snapshot } => snapshot,
        _ => return Err("missing snapshot".into()),
    };
    assert_eq!(first_snapshot.summary().operation, Operation::Overwrite);
    assert_eq!(
        first_snapshot
            .summary()
            .additional_properties
            .get(RECONCILIATION_ID_PROPERTY),
        Some(&plan().id.to_string())
    );
    table = TableCommit::builder()
        .ident(table.identifier().clone())
        .updates(commit.updates)
        .requirements(commit.requirements)
        .build()
        .apply(table)?;

    let mut next = plan();
    next.id = Uuid::from_u128(2);
    next.base_revision = 1;
    next.target_revision = 3;
    next.base_snapshot_id = table.metadata().current_snapshot_id();
    let mut replacement = first.clone();
    replacement.metadata.puffin_path = "memory://table/replacement.puffin".into();
    replacement.metadata.cardinality = 5;
    replacement.metadata.file_size = 90;
    replacement.metadata.length = 50;
    replacement.added_revision = 3;
    let mut removed = first;
    removed.removed_revision = Some(2);
    let commit = prepare_reconciliation(
        &table,
        &next,
        &[],
        &[replacement.clone()],
        &[removed],
        &heartbeat,
    )
    .await?;
    table = TableCommit::builder()
        .ident(table.identifier().clone())
        .updates(commit.updates)
        .requirements(commit.requirements)
        .build()
        .apply(table)?;

    // An append after a vector publication must retain both live vectors.
    next.id = Uuid::from_u128(4);
    next.base_revision = 3;
    next.target_revision = 4;
    next.base_snapshot_id = table.metadata().current_snapshot_id();
    let mut appended = file(&table, 2);
    appended.added_revision = 4;
    let commit = prepare_reconciliation(&table, &next, &[appended], &[], &[], &heartbeat).await?;
    table = TableCommit::builder()
        .ident(table.identifier().clone())
        .updates(commit.updates)
        .requirements(commit.requirements)
        .build()
        .apply(table)?;
    let snapshot = table
        .metadata()
        .current_snapshot()
        .ok_or("missing snapshot")?;
    let manifests = table.manifest_list_reader(snapshot).load().await?;
    let mut data = Vec::new();
    let mut vectors = Vec::new();
    for manifest in manifests.entries() {
        let manifest = manifest.load_manifest(table.file_io()).await?;
        for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            let file = entry.data_file();
            if file.content_type() == DataContentType::Data {
                data.push(file.file_path().to_owned());
            } else {
                vectors.push((
                    file.file_path().to_owned(),
                    file.referenced_data_file(),
                    file.content_offset(),
                    file.content_size_in_bytes(),
                    file.record_count(),
                    file.file_size_in_bytes(),
                    file.file_format(),
                ));
            }
        }
    }
    data.sort();
    vectors.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        data,
        (0..3)
            .map(|i| format!("memory://table/data/{i}.parquet"))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        vectors,
        vec![
            (
                replacement.metadata.puffin_path,
                Some(replacement.metadata.target_file),
                Some(4),
                Some(50),
                5,
                90,
                DataFileFormat::Puffin
            ),
            (
                other.metadata.puffin_path,
                Some(other.metadata.target_file),
                Some(34),
                Some(30),
                1,
                200,
                DataFileFormat::Puffin
            ),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn rejects_changed_base_missing_schema_and_invalid_partition() -> Result<()> {
    let table = table()?;
    let (heartbeat, beats) = heartbeat();
    let mut changed = plan();
    changed.base_snapshot_id = Some(42);
    let error = prepare_reconciliation(&table, &changed, &[], &[], &[], &heartbeat)
        .await
        .err()
        .ok_or("expected failure")?;
    insta::assert_snapshot!(error.to_string(), @"invalid reconciliation input: base snapshot changed");
    let mut missing = file(&table, 0);
    missing.schema_id = 42;
    let error = prepare_reconciliation(&table, &plan(), &[missing], &[], &[], &heartbeat)
        .await
        .err()
        .ok_or("expected failure")?;
    insta::assert_snapshot!(error.to_string(), @"invalid reconciliation input: schema 42 not found");
    let mut invalid = file(&table, 0);
    invalid.metadata.partition.clear();
    let error = prepare_reconciliation(&table, &plan(), &[invalid], &[], &[], &heartbeat)
        .await
        .err()
        .ok_or("expected failure")?;
    insta::assert_snapshot!(error.to_string(), @"invalid reconciliation input: partition fields do not match recorded spec");
    assert_eq!(beats.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn stops_on_fenced_progress() -> Result<()> {
    let table = table()?;
    let heartbeat = Heartbeat::new(|| async { Err(se_metadata_client::Error::TaskFenced) });
    let error = prepare_reconciliation(&table, &plan(), &[file(&table, 0)], &[], &[], &heartbeat)
        .await
        .err()
        .ok_or("expected failure")?;
    insta::assert_snapshot!(error.to_string(), @"progress heartbeat failed: task attempt is no longer active");
    Ok(())
}

fn plan() -> ReconciliationPlan {
    ReconciliationPlan {
        id: Uuid::from_u128(1),
        base_revision: 0,
        target_revision: 1,
        base_snapshot_id: None,
    }
}

fn heartbeat() -> (Heartbeat, Arc<AtomicUsize>) {
    let beats = Arc::new(AtomicUsize::new(0));
    let count = beats.clone();
    (
        Heartbeat::new(move || {
            count.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        }),
        beats,
    )
}

fn file(table: &Table, index: usize) -> VersionedDataFile {
    VersionedDataFile {
        metadata: DataFileMetadata {
            path: format!("memory://table/data/{index}.parquet"),
            format: DataFileFormat::Parquet,
            size: 100,
            num_rows: 10,
            partition_spec_id: 0,
            partition: table
                .metadata()
                .default_partition_spec()
                .fields()
                .iter()
                .map(|field| (field.field_id, Some(5_i64.to_le_bytes().to_vec())))
                .collect(),
            sort_order_id: Some(0),
            column_sizes: BTreeMap::from([(1, 80)]),
            value_counts: BTreeMap::from([(1, 10)]),
            null_value_counts: BTreeMap::from([(1, 1)]),
            nan_value_counts: BTreeMap::from([(1, 0)]),
            lower_bounds: BTreeMap::from([(1, 1_i64.to_le_bytes().to_vec())]),
            upper_bounds: BTreeMap::from([(1, 9_i64.to_le_bytes().to_vec())]),
        },
        schema_id: 0,
        added_revision: 1,
        removed_revision: None,
    }
}

fn table() -> Result<Table> {
    table_with_layout(FormatVersion::V2, true)
}

fn table_with_layout(version: FormatVersion, partitioned: bool) -> Result<Table> {
    let schema = Schema::builder()
        .with_fields([
            NestedField::optional(17, "key", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()?;
    let spec = if partitioned {
        UnboundPartitionSpec::builder()
            .add_partition_field(17, "key", Transform::Identity)?
            .build()
    } else {
        UnboundPartitionSpec::default()
    };
    let metadata = TableMetadataBuilder::new(
        schema,
        spec,
        SortOrder::unsorted_order(),
        "memory://table".into(),
        version,
        Default::default(),
    )?
    .build()?
    .metadata;
    Ok(Table::builder()
        .runtime(iceberg::Runtime::try_current()?)
        .identifier(TableIdent::from_strs(["test", "table"])?)
        .metadata(metadata)
        .metadata_location(
            "memory://table/metadata/00000-00000000-0000-0000-0000-000000000001.metadata.json",
        )
        .file_io(FileIO::new_with_memory())
        .build()?)
}

#[async_trait::async_trait]
impl MetadataClient for RecoveryClient {
    async fn get_catalog(
        &self,
        _: &CatalogId,
    ) -> se_metadata_client::Result<Option<CatalogConfig>> {
        // PANIC: Saved commits must be resubmitted without reloading/rebasing the catalog.
        unreachable!("saved commit must not reload catalog")
    }

    async fn resolve_table(
        &self,
        _: &CatalogId,
        _: &TableIdent,
    ) -> se_metadata_client::Result<iceberg::spec::TableMetadata> {
        // PANIC: Reconciliation does not resolve names.
        unreachable!("unexpected table resolution")
    }

    async fn claim_task(&self, _: Uuid) -> se_metadata_client::Result<Option<TaskAssignment>> {
        Ok(Some(self.task.clone()))
    }

    fn heartbeat_task(&self, _: TaskToken) -> Heartbeat {
        Heartbeat::new(|| async { Ok(()) })
    }

    async fn materialization_input(
        &self,
        _: TaskToken,
        _: u64,
    ) -> se_metadata_client::Result<se_metadata_client::MaterializationInput> {
        // PANIC: This fixture only runs reconciliation tasks.
        unreachable!("reconciliation does not read materialization input")
    }

    async fn reconciliation_input(
        &self,
        _: TaskToken,
    ) -> se_metadata_client::Result<ReconciliationInput> {
        Ok(ReconciliationInput {
            plan: plan(),
            files: Vec::new(),
            added_vectors: Vec::new(),
            removed_vectors: Vec::new(),
            prepared: Some(self.commit.clone()),
        })
    }

    async fn complete_reconciliation(
        &self,
        _: TaskToken,
        commit: PreparedCommit,
    ) -> se_metadata_client::Result<()> {
        *self.submitted.lock().await = Some(commit);
        if self.fail_completion {
            return Err(se_metadata_client::Error::InvalidBaseUrl);
        }
        self.cancel.cancel();
        Ok(())
    }

    async fn fail_task(&self, _: TaskToken) -> se_metadata_client::Result<()> {
        self.released.fetch_add(1, Ordering::SeqCst);
        self.cancel.cancel();
        Ok(())
    }

    async fn complete_materialization(
        &self,
        _: TaskToken,
        _: MaterializationResult,
    ) -> se_metadata_client::Result<()> {
        // PANIC: A reconciliation worker must not report materialization completion.
        unreachable!("unexpected materialization")
    }

    async fn wal_append(
        &self,
        _: &CatalogId,
        _: Uuid,
        _: se_wal::BatchMetadata,
    ) -> se_wal::Result<()> {
        // PANIC: Publication does not write WAL.
        unreachable!("unexpected WAL append")
    }

    async fn wal_read_after(
        &self,
        _: &CatalogId,
        _: Uuid,
        _: Option<u64>,
        _: usize,
    ) -> se_wal::Result<Vec<se_wal::BatchDescriptor>> {
        // PANIC: Publication does not read WAL.
        unreachable!("unexpected WAL read")
    }
}
