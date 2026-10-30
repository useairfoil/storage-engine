use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::DateTime;
use iceberg::{
    TableCommit, TableIdent, TableRequirement, TableUpdate,
    io::FileIO,
    spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, NestedField,
        PrimitiveType, Schema, SortOrder, Struct, TableMetadataBuilder, Type, UnboundPartitionSpec,
    },
    table::Table,
    transaction::Transaction,
};
use object_store::memory::InMemory;
use se_common::clock::{MockSystemClock, SystemClock};
use se_metadata::{
    CatalogConfig, CatalogId, DataFileMetadata, DeletionVectorMetadata, DeletionVectorReplacement,
    MaterializationResult, Metadata, PreparedCommit, RECONCILIATION_ID_PROPERTY, REVISION_PROPERTY,
    ReconciliationInput, RestCatalogConfig, Task,
};
use se_secret_store::memory::InMemorySecretStore;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use uuid::Uuid;

struct CatalogState {
    table: Mutex<Table>,
    failure: AtomicUsize,
    commits: AtomicUsize,
}

struct Server(tokio::task::JoinHandle<()>);

#[derive(Deserialize)]
struct CommitRequest {
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

#[tokio::test]
async fn publishes_fixed_revisions_and_recovers_prepared_commits_across_restarts()
-> Result<(), Box<dyn std::error::Error>> {
    let table = table()?;
    let state = Arc::new(CatalogState {
        table: Mutex::new(table.clone()),
        failure: AtomicUsize::new(0),
        commits: AtomicUsize::new(0),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let config = CatalogConfig::Rest(RestCatalogConfig {
        uri: format!("http://{}", listener.local_addr()?),
        warehouse: None,
        properties: HashMap::new(),
    });
    let app = Router::new()
        .route(
            "/v1/config",
            get(|| async { Json(json!({"defaults": {}, "overrides": {}})) }),
        )
        .route(
            "/v1/namespaces/{namespace}/tables/{table}",
            get(load).post(commit),
        )
        .with_state(state.clone());
    let _server = Server(tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    }));
    let store = Arc::new(InMemory::new());
    let secrets = Arc::new(InMemorySecretStore::new());
    let clock = Arc::new(MockSystemClock::with_time(1000));
    let metadata = Metadata::from_db(
        slatedb::Db::open("metadata", store.clone()).await?,
        secrets.clone(),
        clock.clone(),
    );
    let catalog = CatalogId::new("catalog")?;
    metadata.create_catalog(catalog.clone(), config).await?;
    metadata.resolve_table(&catalog, table.identifier()).await?;
    // Publish two files in one checkpoint before reconciliation becomes eligible.
    materialize_result(
        &metadata,
        &catalog,
        &table,
        &clock,
        vec![file("first"), file("second")],
        Vec::new(),
    )
    .await?;
    clock.set(21_000);
    let first = metadata
        .claim_task(Uuid::from_u128(2))
        .await?
        .ok_or("missing reconciliation")?;
    let input = metadata.reconciliation_input(&first.token).await?;
    assert_eq!(input.plan.base_revision, 0);
    assert_eq!(input.plan.target_revision, 1);
    assert_eq!(input.files.len(), 2);
    assert_eq!(
        metadata.claim_task(first.token.worker_id).await?,
        Some(first.clone())
    );
    insta::assert_debug_snapshot!(metadata.claim_task(Uuid::from_u128(3)).await?, @"None");
    // New materialization must not change the pinned revision or its input.
    materialize(&metadata, &catalog, &table, &clock, "third").await?;
    let fixed = metadata.reconciliation_input(&first.token).await?;
    assert_eq!(fixed.plan, input.plan);
    assert_eq!(fixed.files, input.files);
    let prepared = prepare(&table, &input).await?;
    let mut invalid = prepared.clone();
    invalid.requirements.clear();
    insta::assert_compact_debug_snapshot!(metadata.complete_reconciliation(&first.token, invalid).await.err(), @"Some(InvalidPreparedCommit)");
    assert_eq!(state.commits.load(Ordering::SeqCst), 0);

    // External branch changes must not cause this plan to rebase or overwrite the branch.
    let mut external = input.clone();
    external.plan.id = Uuid::from_u128(99);
    let external = prepare(&table, &external).await?;
    let changed = TableCommit::builder()
        .ident(table.identifier().clone())
        .updates(external.updates)
        .requirements(external.requirements)
        .build()
        .apply(table.clone())?;
    *state.table.lock().await = changed;
    insta::assert_compact_debug_snapshot!(metadata.complete_reconciliation(&first.token, prepared.clone()).await.err(), @"Some(ReconciliationConflict)");
    assert_eq!(state.commits.load(Ordering::SeqCst), 0);
    *state.table.lock().await = table.clone();

    // The catalog applies the commit but reports an error: metadata cannot know its outcome yet.
    state.failure.store(2, Ordering::SeqCst);
    let error = metadata
        .complete_reconciliation(&first.token, prepared.clone())
        .await
        .err();
    insta::assert_debug_snapshot!(matches!(error, Some(se_metadata::Error::Iceberg(_))), @"true");
    assert_eq!(state.commits.load(Ordering::SeqCst), 1);
    let debug = serde_json::to_value(
        metadata
            .debug_table(&catalog, table.metadata().uuid())
            .await?,
    )?;
    assert_eq!(debug["reconciled_revision"], 0);
    assert_eq!(debug["desired_revision"], 2);
    assert_eq!(debug["prepared_commit"], serde_json::to_value(&prepared)?);
    metadata.close().await?;

    let metadata = Metadata::from_db(
        slatedb::Db::open("metadata", store).await?,
        secrets,
        clock.clone(),
    );
    clock.set(51_000);
    let recovered = metadata
        .claim_task(Uuid::from_u128(4))
        .await?
        .ok_or("missing recovered reconciliation")?;
    assert_eq!(recovered.task, first.task);
    assert_ne!(recovered.token.attempt_id, first.token.attempt_id);
    insta::assert_compact_debug_snapshot!(metadata.complete_reconciliation(&first.token, prepared.clone()).await.err(), @"Some(TaskFenced)");
    let saved = metadata.reconciliation_input(&recovered.token).await?;
    assert_eq!(saved.prepared.as_ref(), Some(&prepared));
    let mut different = prepared.clone();
    different
        .requirements
        .push(TableRequirement::DefaultSpecIdMatch { default_spec_id: 0 });
    insta::assert_compact_debug_snapshot!(metadata.complete_reconciliation(&recovered.token, different).await.err(), @"Some(InvalidPreparedCommit)");
    metadata
        .complete_reconciliation(
            &recovered.token,
            saved.prepared.ok_or("missing prepared commit")?,
        )
        .await?;
    // Recovery observes the marker instead of sending another catalog commit.
    assert_eq!(state.commits.load(Ordering::SeqCst), 1);
    let debug = serde_json::to_value(
        metadata
            .debug_table(&catalog, table.metadata().uuid())
            .await?,
    )?;
    assert_eq!(debug["reconciled_revision"], 1);
    assert_eq!(debug["desired_revision"], 2);
    assert_eq!(debug["prepared_commit"], Value::Null);

    // A later plan starts at the confirmed snapshot and contains only revision 2's file.
    clock.set(56_000);
    let next = metadata
        .claim_task(Uuid::from_u128(5))
        .await?
        .ok_or("missing next reconciliation")?;
    let next_input = metadata.reconciliation_input(&next.token).await?;
    assert_eq!(next_input.plan.base_revision, 1);
    assert_eq!(next_input.plan.target_revision, 2);
    assert_eq!(next_input.files.len(), 1);
    assert_eq!(
        next_input.files[0].metadata.path,
        "memory://table/data/third.parquet"
    );
    let published = state.table.lock().await.clone();
    assert_eq!(
        next_input.plan.base_snapshot_id,
        published.metadata().current_snapshot_id()
    );
    let next_prepared = prepare(&published, &next_input).await?;
    // A failure before catalog mutation is retried using exactly the saved payload.
    state.failure.store(1, Ordering::SeqCst);
    let error = metadata
        .complete_reconciliation(&next.token, next_prepared.clone())
        .await
        .err();
    insta::assert_debug_snapshot!(matches!(error, Some(se_metadata::Error::Iceberg(_))), @"true");
    assert_eq!(state.commits.load(Ordering::SeqCst), 1);
    metadata
        .complete_reconciliation(&next.token, next_prepared)
        .await?;
    assert_eq!(state.commits.load(Ordering::SeqCst), 2);
    let debug = serde_json::to_value(
        metadata
            .debug_table(&catalog, table.metadata().uuid())
            .await?,
    )?;
    assert_eq!(debug["reconciled_revision"], 2);
    assert_eq!(debug["reconciliation"], Value::Null);
    assert_eq!(debug["reconciliation_plan"], Value::Null);
    // Read real manifests: three files, no duplicates, and no skipped intermediate materialization.
    let published = state.table.lock().await.clone();
    let snapshot = published
        .metadata()
        .current_snapshot()
        .ok_or("missing snapshot")?;
    let manifests = published.manifest_list_reader(snapshot).load().await?;
    let mut paths = Vec::new();
    for manifest in manifests.entries() {
        let manifest = manifest.load_manifest(published.file_io()).await?;
        paths.extend(
            manifest
                .entries()
                .iter()
                .map(|entry| entry.data_file().file_path().to_owned()),
        );
    }
    paths.sort();
    insta::assert_debug_snapshot!(paths, @r#"
    [
        "memory://table/data/first.parquet",
        "memory://table/data/second.parquet",
        "memory://table/data/third.parquet",
    ]
    "#);

    // Publish two initial vectors; the second stays unchanged through later replacements.
    let first_vector = vector("first", "first-v1", 1);
    let baseline = vector("second", "second-v1", 1);
    materialize_vectors(
        &metadata,
        &catalog,
        &table,
        &clock,
        vec![
            DeletionVectorReplacement {
                previous: None,
                current: first_vector.clone(),
            },
            DeletionVectorReplacement {
                previous: None,
                current: baseline.clone(),
            },
        ],
    )
    .await?;
    clock.set(clock.now().timestamp_millis() + 5_000);
    let task = metadata
        .claim_task(Uuid::from_u128(6))
        .await?
        .ok_or("missing vector reconciliation")?;
    let vectors = metadata.reconciliation_input(&task.token).await?;
    assert_eq!(vectors.plan.base_revision, 2);
    assert_eq!(vectors.plan.target_revision, 3);
    assert_eq!(vectors.files.len(), 0);
    assert_eq!(vectors.added_vectors.len(), 2);
    assert_eq!(vectors.removed_vectors.len(), 0);
    // Even a correctly fenced and marked append must not acknowledge vector changes.
    let append = prepare(&published, &vectors).await?;
    insta::assert_compact_debug_snapshot!(metadata.complete_reconciliation(&task.token, append).await.err(), @"Some(InvalidPreparedCommit)");
    assert_eq!(state.commits.load(Ordering::SeqCst), 2);
    check_revision(&metadata, &catalog, &table, 2, 3).await?;
    let prepared_vectors = prepare_vectors(&published, &vectors).await?;
    // A lost overwrite response is recovered by its marker, without duplicating vectors.
    state.failure.store(2, Ordering::SeqCst);
    let error = metadata
        .complete_reconciliation(&task.token, prepared_vectors.clone())
        .await
        .err();
    insta::assert_debug_snapshot!(matches!(error, Some(se_metadata::Error::Iceberg(_))), @"true");
    assert_eq!(state.commits.load(Ordering::SeqCst), 3);
    assert_eq!(
        metadata.reconciliation_input(&task.token).await?.prepared,
        Some(prepared_vectors.clone())
    );
    metadata
        .complete_reconciliation(&task.token, prepared_vectors)
        .await?;
    assert_eq!(state.commits.load(Ordering::SeqCst), 3);
    check_revision(&metadata, &catalog, &table, 3, 3).await?;
    check_live_files(
        &*state.table.lock().await,
        &[first_vector.clone(), baseline.clone()],
    )
    .await?;

    let replacement = vector("first", "first-v2", 2);
    materialize_vectors(
        &metadata,
        &catalog,
        &table,
        &clock,
        vec![DeletionVectorReplacement {
            previous: Some(first_vector.clone()),
            current: replacement.clone(),
        }],
    )
    .await?;
    clock.set(clock.now().timestamp_millis() + 5_000);
    let pinned = metadata
        .claim_task(Uuid::from_u128(7))
        .await?
        .ok_or("missing replacement reconciliation")?;
    let pinned_input = metadata.reconciliation_input(&pinned.token).await?;
    assert_eq!(pinned_input.plan.base_revision, 3);
    assert_eq!(pinned_input.plan.target_revision, 4);
    assert_eq!(pinned_input.added_vectors.len(), 1);
    assert_eq!(pinned_input.added_vectors[0].metadata, replacement);
    assert_eq!(pinned_input.removed_vectors.len(), 1);
    assert_eq!(pinned_input.removed_vectors[0].metadata, first_vector);

    // Two later materializations replace the pinned vector before it is published.
    let intermediate = vector("first", "first-v3", 3);
    let latest = vector("first", "first-v4", 4);
    for (previous, current) in [
        (replacement.clone(), intermediate.clone()),
        (intermediate.clone(), latest.clone()),
    ] {
        materialize_vectors(
            &metadata,
            &catalog,
            &table,
            &clock,
            vec![DeletionVectorReplacement {
                previous: Some(previous),
                current,
            }],
        )
        .await?;
        // Keep the pinned plan leased across the two checkpoint intervals.
        metadata.heartbeat_task(&pinned.token).await?;
    }
    let fixed = metadata.reconciliation_input(&pinned.token).await?;
    assert_eq!(fixed.plan, pinned_input.plan);
    assert_eq!(fixed.files, pinned_input.files);
    assert_eq!(fixed.removed_vectors, pinned_input.removed_vectors);
    assert_eq!(fixed.added_vectors.len(), 1);
    assert_eq!(fixed.added_vectors[0].metadata, replacement);
    assert_eq!(fixed.added_vectors[0].added_revision, 4);
    // MVCC bookkeeping may change, but visibility at the pinned target does not.
    assert_eq!(fixed.added_vectors[0].removed_revision, Some(5));
    let published = state.table.lock().await.clone();
    metadata
        .complete_reconciliation(&pinned.token, prepare_vectors(&published, &fixed).await?)
        .await?;
    check_revision(&metadata, &catalog, &table, 4, 6).await?;
    check_live_files(
        &*state.table.lock().await,
        &[replacement.clone(), baseline.clone()],
    )
    .await?;

    clock.set(clock.now().timestamp_millis() + 5_000);
    let batched = metadata
        .claim_task(Uuid::from_u128(8))
        .await?
        .ok_or("missing batched reconciliation")?;
    let batched_input = metadata.reconciliation_input(&batched.token).await?;
    assert_eq!(batched_input.plan.base_revision, 4);
    assert_eq!(batched_input.plan.target_revision, 6);
    assert_eq!(batched_input.files.len(), 0);
    assert_eq!(batched_input.added_vectors.len(), 1);
    assert_eq!(batched_input.added_vectors[0].metadata, latest);
    assert_eq!(batched_input.added_vectors[0].added_revision, 6);
    assert_eq!(batched_input.removed_vectors.len(), 1);
    assert_eq!(batched_input.removed_vectors[0].metadata, replacement);
    assert_eq!(batched_input.removed_vectors[0].removed_revision, Some(5));
    // Neither the unpublished intermediate nor the unchanged baseline is in the delta.
    let published = state.table.lock().await.clone();
    metadata
        .complete_reconciliation(
            &batched.token,
            prepare_vectors(&published, &batched_input).await?,
        )
        .await?;
    check_revision(&metadata, &catalog, &table, 6, 6).await?;
    check_live_files(&*state.table.lock().await, &[latest, baseline]).await?;
    assert_eq!(state.commits.load(Ordering::SeqCst), 5);
    metadata.close().await?;
    Ok(())
}

async fn materialize(
    metadata: &Metadata,
    catalog: &CatalogId,
    table: &Table,
    clock: &MockSystemClock,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    materialize_result(
        metadata,
        catalog,
        table,
        clock,
        vec![file(name)],
        Vec::new(),
    )
    .await
}

async fn materialize_vectors(
    metadata: &Metadata,
    catalog: &CatalogId,
    table: &Table,
    clock: &MockSystemClock,
    vectors: Vec<DeletionVectorReplacement>,
) -> Result<(), Box<dyn std::error::Error>> {
    materialize_result(metadata, catalog, table, clock, Vec::new(), vectors).await
}

async fn materialize_result(
    metadata: &Metadata,
    catalog: &CatalogId,
    table: &Table,
    clock: &MockSystemClock,
    data_files: Vec<DataFileMetadata>,
    deletion_vectors: Vec<DeletionVectorReplacement>,
) -> Result<(), Box<dyn std::error::Error>> {
    metadata
        .wal_append(
            catalog,
            table.metadata().uuid(),
            0,
            "wal",
            DateTime::UNIX_EPOCH,
        )
        .await?;
    clock.set(clock.now().timestamp_millis() + 15_000);
    let task = metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing materialization")?;
    let Task::Materialize {
        range, schema_id, ..
    } = task.task
    else {
        return Err("wrong task".into());
    };
    metadata
        .complete_materialization(
            &task.token,
            MaterializationResult {
                range,
                schema_id,
                index_delta: None,
                data_files,
                deletion_vectors,
            },
        )
        .await?;
    Ok(())
}

fn file(name: &str) -> DataFileMetadata {
    DataFileMetadata {
        path: format!("memory://table/data/{name}.parquet"),
        format: DataFileFormat::Parquet,
        size: 100,
        num_rows: 10,
        partition_spec_id: 0,
        partition: BTreeMap::new(),
        sort_order_id: Some(0),
        column_sizes: BTreeMap::new(),
        value_counts: BTreeMap::new(),
        null_value_counts: BTreeMap::new(),
        nan_value_counts: BTreeMap::new(),
        lower_bounds: BTreeMap::new(),
        upper_bounds: BTreeMap::new(),
    }
}

async fn prepare(
    table: &Table,
    input: &ReconciliationInput,
) -> Result<PreparedCommit, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    for file in &input.files {
        files.push(
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(file.metadata.path.clone())
                .file_format(file.metadata.format)
                .partition(Struct::empty())
                .partition_spec_id(0)
                .record_count(file.metadata.num_rows)
                .file_size_in_bytes(file.metadata.size)
                .build()?,
        );
    }
    let mut action = Transaction::new(table)
        .fast_append()
        .set_commit_uuid(input.plan.id)
        .set_snapshot_properties(HashMap::from([
            (RECONCILIATION_ID_PROPERTY.into(), input.plan.id.to_string()),
            (
                REVISION_PROPERTY.into(),
                input.plan.target_revision.to_string(),
            ),
        ]))
        .add_data_files(files)
        .prepare(table)
        .await?;
    let mut requirements = action.take_requirements();
    requirements.push(TableRequirement::CurrentSchemaIdMatch {
        current_schema_id: table.metadata().current_schema_id(),
    });
    Ok(PreparedCommit {
        updates: action.take_updates(),
        requirements,
    })
}

fn vector(target: &str, name: &str, cardinality: u64) -> DeletionVectorMetadata {
    DeletionVectorMetadata {
        puffin_path: format!("memory://table/deletes/{name}.puffin"),
        target_file: file(target).path,
        cardinality,
        file_size: 80,
        offset: 4,
        length: 20,
    }
}

fn vector_file(vector: &DeletionVectorMetadata) -> Result<DataFile, Box<dyn std::error::Error>> {
    Ok(DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(vector.puffin_path.clone())
        .file_format(DataFileFormat::Puffin)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .record_count(vector.cardinality)
        .file_size_in_bytes(vector.file_size)
        .referenced_data_file(Some(vector.target_file.clone()))
        .content_offset(Some(vector.offset.try_into()?))
        .content_size_in_bytes(Some(vector.length.try_into()?))
        .build()?)
}

async fn prepare_vectors(
    table: &Table,
    input: &ReconciliationInput,
) -> Result<PreparedCommit, Box<dyn std::error::Error>> {
    let prepared = se_iceberg::publication::prepare_upsert(
        table,
        input.plan.id,
        Vec::new(),
        input
            .added_vectors
            .iter()
            .map(|vector| vector_file(&vector.metadata))
            .collect::<Result<Vec<_>, _>>()?,
        input
            .removed_vectors
            .iter()
            .map(|vector| vector_file(&vector.metadata))
            .collect::<Result<Vec<_>, _>>()?,
        HashMap::from([
            (RECONCILIATION_ID_PROPERTY.into(), input.plan.id.to_string()),
            (
                REVISION_PROPERTY.into(),
                input.plan.target_revision.to_string(),
            ),
        ]),
    )
    .await?;
    Ok(PreparedCommit {
        updates: prepared.updates,
        requirements: vec![
            TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".into(),
                snapshot_id: input.plan.base_snapshot_id,
            },
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: table.metadata().current_schema_id(),
            },
        ],
    })
}

async fn check_revision(
    metadata: &Metadata,
    catalog: &CatalogId,
    table: &Table,
    reconciled: u64,
    desired: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let debug = serde_json::to_value(
        metadata
            .debug_table(catalog, table.metadata().uuid())
            .await?,
    )?;
    assert_eq!(debug["reconciled_revision"], reconciled);
    assert_eq!(debug["desired_revision"], desired);
    assert_eq!(debug["prepared_commit"], Value::Null);
    Ok(())
}

async fn check_live_files(
    table: &Table,
    expected: &[DeletionVectorMetadata],
) -> Result<(), Box<dyn std::error::Error>> {
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
                assert_eq!(file.content_type(), DataContentType::PositionDeletes);
                assert_eq!(file.file_format(), DataFileFormat::Puffin);
                vectors.push(DeletionVectorMetadata {
                    puffin_path: file.file_path().to_owned(),
                    target_file: file
                        .referenced_data_file()
                        .ok_or("missing target")?
                        .to_owned(),
                    cardinality: file.record_count(),
                    file_size: file.file_size_in_bytes(),
                    offset: file.content_offset().ok_or("missing offset")?.try_into()?,
                    length: file
                        .content_size_in_bytes()
                        .ok_or("missing length")?
                        .try_into()?,
                });
            }
        }
    }
    data.sort();
    assert_eq!(
        data,
        [file("first").path, file("second").path, file("third").path]
    );
    vectors.sort_by(|left, right| left.target_file.cmp(&right.target_file));
    let mut expected = expected.to_vec();
    expected.sort_by(|left, right| left.target_file.cmp(&right.target_file));
    assert_eq!(vectors, expected);
    Ok(())
}

fn table() -> iceberg::Result<Table> {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "value",
            Type::Primitive(PrimitiveType::String),
        ))])
        .build()?;
    let metadata = TableMetadataBuilder::new(
        schema,
        UnboundPartitionSpec::default(),
        SortOrder::unsorted_order(),
        "memory://table".into(),
        FormatVersion::V3,
        HashMap::from([(
            se_metadata::CHECKPOINT_INTERVAL_PROPERTY.into(),
            "15".into(),
        )]),
    )?
    .assign_uuid(Uuid::from_u128(42))
    .build()?
    .metadata;
    Table::builder()
        .runtime(iceberg::Runtime::try_current()?)
        .identifier(TableIdent::from_strs(["test", "table"])?)
        .metadata(metadata)
        .metadata_location(
            "memory://table/metadata/00000-00000000-0000-0000-0000-000000000001.metadata.json",
        )
        .file_io(FileIO::new_with_memory())
        .build()
}

async fn load(State(state): State<Arc<CatalogState>>) -> Json<Value> {
    let table = state.table.lock().await;
    Json(
        json!({"metadata": table.metadata(), "metadata-location": table.metadata_location(), "config": {}}),
    )
}

async fn commit(
    State(state): State<Arc<CatalogState>>,
    Json(request): Json<CommitRequest>,
) -> Response {
    let failure = state.failure.swap(0, Ordering::SeqCst);
    if failure == 1 {
        return failure_response();
    }
    let mut table = state.table.lock().await;
    let commit = TableCommit::builder()
        .ident(table.identifier().clone())
        .updates(request.updates)
        .requirements(request.requirements)
        .build();
    match commit.apply(table.clone()) {
        Ok(updated) => {
            *table = updated;
            state.commits.fetch_add(1, Ordering::SeqCst);
            if failure == 2 { return failure_response(); }
            Json(json!({"metadata": table.metadata(), "metadata-location": table.metadata_location(), "config": {}})).into_response()
        }
        Err(error) => (StatusCode::CONFLICT, Json(json!({"error": {"message": error.to_string(), "type": "CommitFailedException", "code": 409}}))).into_response(),
    }
}

fn failure_response() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": {"message": "injected uncertain commit", "type": "InternalServerError", "code": 500}}))).into_response()
}

impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}
