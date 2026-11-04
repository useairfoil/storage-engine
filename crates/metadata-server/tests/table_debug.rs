use std::{collections::HashMap, sync::Arc};

use axum::{Json, routing::get};
use iceberg::{
    TableIdent,
    spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
        TableMetadataBuilder, Type,
    },
};
use object_store::memory::InMemory;
use reqwest::{Client, StatusCode, Url};
use se_common::clock::{MockSystemClock, SystemClock};
use se_metadata::{
    CHECKPOINT_INTERVAL_PROPERTY, CatalogConfig, CatalogId, MaterializationResult, Metadata,
    RestCatalogConfig, Task,
};
use se_metadata_client::{HttpMetadataClient, MetadataClient};
use se_secret_store::memory::InMemorySecretStore;
use serde_json::{Value, json};
use tokio_util::task::AbortOnDropHandle;
use uuid::Uuid;

#[tokio::test]
async fn table_endpoints_track_lifecycle_without_changing_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(MockSystemClock::with_time(1000));
    let metadata = Metadata::from_db(
        // Use the real database clock so durability flushes do not require advancing test time.
        slatedb::Db::open("metadata", Arc::new(InMemory::new())).await?,
        Arc::new(InMemorySecretStore::new()),
        clock.clone(),
    );
    let table_uuid = Uuid::from_u128(42);
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "value",
            Type::Primitive(PrimitiveType::String),
        ))])
        .build()?;
    let table = TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec().into_unbound(),
        SortOrder::unsorted_order(),
        "file:///tmp/table".into(),
        FormatVersion::V2,
        HashMap::from([(CHECKPOINT_INTERVAL_PROPERTY.into(), "15".into())]),
    )?
    .assign_uuid(table_uuid)
    .build()?
    .metadata;
    let router = se_metadata_server::router(metadata.clone(), clock.clone())
        .route("/v1/config", get(|| async { Json(json!({"defaults": {}, "overrides": {}})) }))
        .route("/v1/namespaces/{namespace}/tables/{table}", get(move || {
            let table = table.clone();
            async move { Json(json!({"metadata": table, "metadata-location": "file:///tmp/table/metadata.json", "config": {}})) }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}", listener.local_addr()?);
    let client = HttpMetadataClient::new(base.parse()?);
    let _server =
        AbortOnDropHandle::new(tokio::spawn(
            async move { axum::serve(listener, router).await },
        ));
    let catalog = CatalogId::new("catalog /é")?;
    let mut url: Url = base.parse()?;
    url.path_segments_mut()
        .map_err(|_| "HTTP URL must support path segments")?
        .push("catalogs")
        .push(catalog.as_str())
        .push("by-uuid")
        .push(&table_uuid.to_string())
        .push("debug");
    let http = Client::new();
    let mut status_url = url.clone();
    status_url
        .path_segments_mut()
        .map_err(|_| "HTTP URL must support path segments")?
        .pop()
        .push("status");
    let status = async || {
        http.get(status_url.clone())
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await
    };

    assert_eq!(
        http.get(url.clone()).send().await?.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        http.get(status_url.clone()).send().await?.status(),
        StatusCode::NOT_FOUND
    );
    metadata
        .create_catalog(
            catalog.clone(),
            CatalogConfig::Rest(RestCatalogConfig {
                uri: base,
                warehouse: None,
                properties: HashMap::new(),
            }),
        )
        .await?;
    let ident = TableIdent::from_strs(["test", "table"])?;
    metadata.resolve_table(&catalog, &ident).await?;
    insta::assert_yaml_snapshot!(status().await?, @r#"
    conditions:
      - message: All pending changes have been processed.
        reason: CaughtUp
        status: "True"
        type: Materialized
      - message: All pending changes have been processed.
        reason: CaughtUp
        status: "True"
        type: Reconciled
      - message: Table deletion has not been requested.
        reason: NotRequested
        status: "False"
        type: Deleting
    iceberg:
      desiredRevision: 0
      reconciledRevision: 0
      reconciledSnapshotId: ~
    observedAt: "1970-01-01T00:00:01Z"
    tableUuid: 00000000-0000-0000-0000-00000000002a
    wal:
      acceptedNextSequence: 0
      materializedNextSequence: 0
      oldestPendingAt: ~
      pendingBatches: 0
      pendingBytes: 0
    work:
      deletion:
        eligibleAt: ~
        leaseExpiresAt: ~
        state: Idle
      materialization:
        eligibleAt: ~
        leaseExpiresAt: ~
        state: Idle
      reconciliation:
        eligibleAt: ~
        leaseExpiresAt: ~
        state: Idle
    "#);
    let initial: Value = http
        .get(url.clone())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    insta::assert_yaml_snapshot!(initial, @r#"
    catalog_id: catalog /é
    checkpoint_interval_sec: 15
    deletion_vectors: []
    desired_revision: 0
    files: []
    index_deltas: []
    materialization: ~
    materialized_next: 0
    materializer_epoch: 0
    next_sequence: 0
    prepared_commit: ~
    reconciled_revision: 0
    reconciled_snapshot_id: ~
    reconciliation: ~
    reconciliation_plan: ~
    table_ident:
      name: table
      namespace:
        - test
    table_uuid: 00000000-0000-0000-0000-00000000002a
    wal: []
    "#);

    // The same UUID in another catalog must not leak into this table's dump.
    let other = CatalogId::new("other")?;
    metadata
        .create_catalog(
            other.clone(),
            metadata
                .get_catalog(&catalog)
                .await?
                .ok_or("missing catalog")?,
        )
        .await?;
    metadata.resolve_table(&other, &ident).await?;
    metadata
        .wal_append(&other, table_uuid, 0, "unrelated", clock.now(), 999)
        .await?;
    metadata
        .wal_append(&catalog, table_uuid, 0, "wal/first", clock.now(), 123)
        .await?;
    metadata
        .wal_append(&catalog, table_uuid, 0, "wal/second", clock.now(), 456)
        .await?;
    let queued: Value = http
        .get(url.clone())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(queued["next_sequence"], 2);
    assert_eq!(
        queued["wal"],
        json!([
            {"sequence": 0, "metadata": {"schema_id": 0, "location": "wal/first", "timestamp": "1970-01-01T00:00:01Z", "wal_size_bytes": 123}},
            {"sequence": 1, "metadata": {"schema_id": 0, "location": "wal/second", "timestamp": "1970-01-01T00:00:01Z", "wal_size_bytes": 456}},
        ])
    );
    assert_eq!(
        queued["materialization"],
        json!({"eligible_at": "1970-01-01T00:00:16Z", "assignment": null})
    );
    let scheduled_status = status().await?;
    insta::assert_yaml_snapshot!(scheduled_status["wal"], @r#"
    acceptedNextSequence: 2
    materializedNextSequence: 0
    oldestPendingAt: "1970-01-01T00:00:01Z"
    pendingBatches: 2
    pendingBytes: 579
    "#);
    insta::assert_yaml_snapshot!(scheduled_status["conditions"][0], @r#"
    message: Pending changes are scheduled for processing.
    reason: Scheduled
    status: "False"
    type: Materialized
    "#);
    insta::assert_yaml_snapshot!(scheduled_status["work"]["materialization"], @r#"
    eligibleAt: "1970-01-01T00:00:16Z"
    leaseExpiresAt: ~
    state: Scheduled
    "#);

    // Another table may be claimed first; claim both with separate worker sessions.
    clock.set(16_000);
    insta::assert_yaml_snapshot!(status().await?["work"]["materialization"], @r#"
    eligibleAt: "1970-01-01T00:00:16Z"
    leaseExpiresAt: ~
    state: Queued
    "#);
    let first = metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing task")?;
    let task = if first.token.catalog_id == catalog {
        first
    } else {
        metadata
            .claim_task(Uuid::from_u128(2))
            .await?
            .ok_or("missing target task")?
    };
    let response = client
        .claim_task(task.token.worker_id)
        .await?
        .ok_or("missing HTTP task")?;
    assert_eq!(response.assignment, task);
    assert_eq!(response.timeout, (task.expires_at - clock.now()).to_std()?);
    clock.set(45_999);
    let active: Value = http
        .get(url.clone())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        active["materialization"]["assignment"],
        serde_json::to_value(&task)?
    );
    assert_eq!(active["materializer_epoch"], 1);
    insta::assert_yaml_snapshot!(status().await?["work"]["materialization"], @r#"
    eligibleAt: "1970-01-01T00:00:46Z"
    leaseExpiresAt: "1970-01-01T00:00:46Z"
    state: Assigned
    "#);
    assert_eq!(
        metadata.claim_task(task.token.worker_id).await?,
        Some(task.clone())
    );
    clock.set(46_000);
    // PANIC: An expired task must be fenced rather than renewed.
    insta::assert_debug_snapshot!(metadata.heartbeat_task(&task.token).await.unwrap_err(), @"TaskFenced");
    let expired: Value = http
        .get(url.clone())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(expired, active);
    insta::assert_yaml_snapshot!(status().await?["work"]["materialization"], @r#"
    eligibleAt: "1970-01-01T00:00:46Z"
    leaseExpiresAt: "1970-01-01T00:00:46Z"
    state: Queued
    "#);

    // Recover both expired assignments, completing only the target table.
    let first = metadata
        .claim_task(Uuid::from_u128(3))
        .await?
        .ok_or("missing recovered task")?;
    let task = if first.token.catalog_id == catalog {
        first
    } else {
        metadata
            .claim_task(Uuid::from_u128(4))
            .await?
            .ok_or("missing recovered target task")?
    };
    let Task::Materialize {
        range, schema_id, ..
    } = task.task
    else {
        return Err("expected materialization task".into());
    };
    insta::assert_debug_snapshot!(client.materialization_input(task.token.clone(), 0).await?.deltas, @"[]");
    let result = MaterializationResult {
        range,
        schema_id,
        index_delta: Some(task.token.attempt_id),
        data_files: Vec::new(),
        deletion_vectors: Vec::new(),
    };
    client
        .complete_materialization(task.token.clone(), result.clone())
        .await?;
    client
        .complete_materialization(task.token.clone(), result)
        .await?;
    let completed: Value = http
        .get(url.clone())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(completed["materialized_next"], 2);
    assert_eq!(completed["next_sequence"], 2);
    assert_eq!(completed["materializer_epoch"], 2);
    assert_eq!(completed["desired_revision"], 1);
    assert_eq!(completed["reconciled_revision"], 0);
    let completed_status = status().await?;
    insta::assert_yaml_snapshot!(completed_status["wal"], @r#"
    acceptedNextSequence: 2
    materializedNextSequence: 2
    oldestPendingAt: ~
    pendingBatches: 0
    pendingBytes: 0
    "#);
    insta::assert_yaml_snapshot!(completed_status["iceberg"], @r#"
    desiredRevision: 1
    reconciledRevision: 0
    reconciledSnapshotId: ~
    "#);
    insta::assert_yaml_snapshot!(completed_status["conditions"], @r#"
    - message: All pending changes have been processed.
      reason: CaughtUp
      status: "True"
      type: Materialized
    - message: Pending changes are scheduled for processing.
      reason: Scheduled
      status: "False"
      type: Reconciled
    - message: Table deletion has not been requested.
      reason: NotRequested
      status: "False"
      type: Deleting
    "#);
    assert_eq!(completed["wal"], json!([]));
    assert_eq!(completed["materialization"], Value::Null);
    assert_eq!(completed["files"], json!([]));
    assert_eq!(completed["deletion_vectors"], json!([]));
    assert_eq!(
        completed["index_deltas"],
        json!([{"id": task.token.attempt_id, "range": range}])
    );
    assert_eq!(
        metadata
            .wal_read_after(&other, table_uuid, None, 10)
            .await?
            .len(),
        1
    );
    metadata
        .wal_append(&catalog, table_uuid, 0, "wal/third", clock.now(), 789)
        .await?;
    insta::assert_yaml_snapshot!(status().await?["wal"], @r#"
    acceptedNextSequence: 3
    materializedNextSequence: 2
    oldestPendingAt: "1970-01-01T00:00:46Z"
    pendingBatches: 1
    pendingBytes: 789
    "#);
    // Reconciliation becomes due before the next checkpoint; keep it on another worker.
    clock.set(51_000);
    let reconciliation = metadata
        .claim_task(Uuid::from_u128(5))
        .await?
        .ok_or("missing reconciliation")?;
    insta::assert_debug_snapshot!(matches!(reconciliation.task, Task::Reconcile { .. }), @"true");
    insta::assert_yaml_snapshot!(status().await?["conditions"][1], @r#"
    message: Pending changes are assigned to a worker.
    reason: Running
    status: "False"
    type: Reconciled
    "#);
    clock.set(61_000);
    let next = client
        .claim_task(task.token.worker_id)
        .await?
        .ok_or("missing next task")?
        .assignment;
    assert_eq!(
        client
            .materialization_input(next.token.clone(), 0)
            .await?
            .deltas,
        vec![se_metadata::IndexDelta {
            id: task.token.attempt_id,
            range
        }]
    );
    let invalid = client.materialization_input(next.token.clone(), 1).await;
    assert_eq!(
        match invalid {
            Err(se_metadata_client::Error::Http(error)) => error.status(),
            _ => None,
        },
        Some(StatusCode::BAD_REQUEST)
    );
    insta::assert_compact_debug_snapshot!(client.materialization_input(task.token, 0).await.err(), @"Some(TaskFenced)");
    metadata.delete_table(&catalog, table_uuid).await?;
    let deleting = status().await?;
    insta::assert_yaml_snapshot!(deleting["conditions"], @r#"
    - message: Pending changes will not be processed because the table is being deleted.
      reason: Deleting
      status: "False"
      type: Materialized
    - message: Pending changes will not be processed because the table is being deleted.
      reason: Deleting
      status: "False"
      type: Reconciled
    - message: Table cleanup is scheduled for a later time.
      reason: Scheduled
      status: "True"
      type: Deleting
    "#);
    insta::assert_yaml_snapshot!(deleting["work"]["deletion"], @r#"
    eligibleAt: "1970-01-01T00:01:31Z"
    leaseExpiresAt: ~
    state: Scheduled
    "#);
    clock.set(91_000);
    insta::assert_yaml_snapshot!(status().await?["conditions"][2]["reason"], @"DeletionRequested");
    let first = metadata
        .claim_task(Uuid::from_u128(6))
        .await?
        .ok_or("missing eligible task")?;
    let deletion = if first.token.catalog_id == catalog {
        first
    } else {
        metadata
            .claim_task(Uuid::from_u128(7))
            .await?
            .ok_or("missing deletion")?
    };
    insta::assert_yaml_snapshot!(status().await?["conditions"][2]["reason"], @"CleanupRunning");
    metadata.fail_task(&deletion.token).await?;
    insta::assert_yaml_snapshot!(status().await?["conditions"][2]["reason"], @"Scheduled");
    clock.set(96_000);
    let deletion = metadata
        .claim_task(deletion.token.worker_id)
        .await?
        .ok_or("missing deletion retry")?;
    metadata.complete_delete_table(&deletion.token).await?;
    assert_eq!(
        http.get(status_url.clone()).send().await?.status(),
        StatusCode::NOT_FOUND
    );
    metadata.close().await?;
    Ok(())
}
