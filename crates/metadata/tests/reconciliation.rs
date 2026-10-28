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
        DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, NestedField,
        PrimitiveType, Schema, SortOrder, Struct, TableMetadataBuilder, Type, UnboundPartitionSpec,
    },
    table::Table,
    transaction::Transaction,
};
use object_store::memory::InMemory;
use se_common::clock::MockSystemClock;
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
    materialize(&metadata, &catalog, &table, "first").await?;
    // Batch a second materialization into the first publication.
    materialize(&metadata, &catalog, &table, "second").await?;
    clock.set(6000);
    let first = metadata
        .claim_task(Uuid::from_u128(2))
        .await?
        .ok_or("missing reconciliation")?;
    let input = metadata.reconciliation_input(&first.token).await?;
    assert_eq!(input.plan.base_revision, 0);
    assert_eq!(input.plan.target_revision, 2);
    assert_eq!(input.files.len(), 2);
    assert_eq!(
        metadata.claim_task(first.token.worker_id).await?,
        Some(first.clone())
    );
    insta::assert_debug_snapshot!(metadata.claim_task(Uuid::from_u128(3)).await?, @"None");
    // New materialization must not change the pinned revision or its input.
    materialize(&metadata, &catalog, &table, "third").await?;
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
    assert_eq!(debug["desired_revision"], 3);
    assert_eq!(debug["prepared_commit"], serde_json::to_value(&prepared)?);
    metadata.close().await?;

    let metadata = Metadata::from_db(
        slatedb::Db::open("metadata", store).await?,
        secrets,
        clock.clone(),
    );
    clock.set(36_000);
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
    assert_eq!(debug["reconciled_revision"], 2);
    assert_eq!(debug["desired_revision"], 3);
    assert_eq!(debug["prepared_commit"], Value::Null);

    // A later plan starts at the confirmed snapshot and contains only revision 3's file.
    clock.set(41_000);
    let next = metadata
        .claim_task(Uuid::from_u128(5))
        .await?
        .ok_or("missing next reconciliation")?;
    let next_input = metadata.reconciliation_input(&next.token).await?;
    assert_eq!(next_input.plan.base_revision, 2);
    assert_eq!(next_input.plan.target_revision, 3);
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
    assert_eq!(debug["reconciled_revision"], 3);
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

    // Unsupported deletion changes never produce an append-only partial publication.
    metadata
        .wal_append(
            &catalog,
            table.metadata().uuid(),
            0,
            "deletes-wal",
            DateTime::UNIX_EPOCH,
        )
        .await?;
    let task = metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing delete materialization")?;
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
                data_files: Vec::new(),
                deletion_vectors: vec![DeletionVectorReplacement {
                    previous: None,
                    current: DeletionVectorMetadata {
                        puffin_path: "memory://table/deletes.puffin".into(),
                        target_file: file("first").path,
                        cardinality: 1,
                        file_size: 80,
                        offset: 4,
                        length: 20,
                    },
                }],
            },
        )
        .await?;
    clock.set(46_000);
    let task = metadata
        .claim_task(Uuid::from_u128(6))
        .await?
        .ok_or("missing unsupported reconciliation")?;
    insta::assert_compact_debug_snapshot!(metadata.reconciliation_input(&task.token).await.err(), @"Some(UnsupportedReconciliation)");
    assert_eq!(state.commits.load(Ordering::SeqCst), 2);
    let before_failure = serde_json::to_value(
        metadata
            .debug_table(&catalog, table.metadata().uuid())
            .await?,
    )?;
    assert_eq!(before_failure["reconciled_revision"], 3);
    assert_eq!(before_failure["desired_revision"], 4);
    metadata.fail_task(&task.token).await?;
    clock.set(51_000);
    let retry = metadata
        .claim_task(Uuid::from_u128(7))
        .await?
        .ok_or("missing retry")?;
    assert_eq!(retry.task, task.task);
    metadata.close().await?;
    Ok(())
}

async fn materialize(
    metadata: &Metadata,
    catalog: &CatalogId,
    table: &Table,
    name: &str,
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
                data_files: vec![file(name)],
                deletion_vectors: Vec::new(),
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
        FormatVersion::V2,
        HashMap::new(),
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
