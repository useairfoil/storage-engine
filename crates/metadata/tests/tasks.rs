mod common;

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use chrono::DateTime;
use futures::{TryStreamExt, future::try_join_all};
use iceberg::spec::DataFileFormat;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};
use se_common::clock::{MockSystemClock, SystemClock};
use se_metadata::{
    CHECKPOINT_INTERVAL_PROPERTY, CatalogId, DataFileMetadata, DeletionVectorMetadata,
    DeletionVectorReplacement, Error, MaterializationResult, Metadata, Task, TaskAssignment,
    VersionedDataFile, VersionedDeletionVector, WalRange,
};
use se_secret_store::memory::InMemorySecretStore;
use slatedb::Db;
use uuid::Uuid;

#[tokio::test]
async fn coalesces_ingestion_and_acknowledges_materialization_across_restarts()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let secrets = Arc::new(InMemorySecretStore::new());
    let clock = Arc::new(MockSystemClock::with_time(1000));
    let metadata = Metadata::from_db(
        Db::open("metadata", store.clone()).await?,
        secrets.clone(),
        clock.clone(),
    );

    let catalog = CatalogId::new("catalog")?;
    let table = Uuid::from_u128(42);
    let worker = Uuid::from_u128(1);
    let other_worker = Uuid::from_u128(2);
    let ident = common::resolve(&metadata, &catalog, table).await?;

    for schema in [3, 3, 7] {
        metadata
            .wal_append(&catalog, table, schema, "wal", DateTime::UNIX_EPOCH)
            .await?;
    }

    clock.set(17_000);
    let first = metadata
        .claim_task(worker)
        .await?
        .ok_or("missing first task")?;

    assert_eq!(first.token.table_ident, ident);
    assert_eq!(
        first.eligible_at,
        Some(DateTime::from_timestamp_millis(16_000).unwrap())
    );
    common::resolve(&metadata, &catalog, table).await?;

    insta::assert_debug_snapshot!(first.task, @"
    Materialize {
        epoch: 1,
        range: WalRange {
            start: 0,
            end: 2,
        },
        schema_id: 3,
    }
    ");

    assert_eq!(metadata.claim_task(worker).await?, Some(first.clone()));

    insta::assert_debug_snapshot!(metadata.claim_task(other_worker).await?, @"None");

    let file = DataFileMetadata {
        path: "data/first.parquet".into(),
        format: DataFileFormat::Parquet,
        size: 1234,
        num_rows: 17,
        partition_spec_id: 2,
        partition: BTreeMap::from([(1000, Some(vec![5, 0, 0, 0])), (1001, None)]),
        sort_order_id: Some(3),
        column_sizes: BTreeMap::from([(1, 120)]),
        value_counts: BTreeMap::from([(1, 17)]),
        null_value_counts: BTreeMap::from([(1, 2)]),
        nan_value_counts: BTreeMap::from([(1, 0)]),
        lower_bounds: BTreeMap::from([(1, vec![4, 0, 0, 0])]),
        upper_bounds: BTreeMap::from([(1, vec![99, 0, 0, 0])]),
    };
    let vector = DeletionVectorMetadata {
        puffin_path: "deletes/first.puffin".into(),
        target_file: file.path.clone(),
        cardinality: 2,
        file_size: 90,
        offset: 10,
        length: 30,
    };
    let mut first_result = new_result_from_assignment(&first)?;
    first_result.data_files.push(file.clone());
    first_result
        .deletion_vectors
        .push(DeletionVectorReplacement {
            previous: None,
            current: vector.clone(),
        });

    metadata
        .complete_materialization(&first.token, first_result.clone())
        .await?;
    common::resolve(&metadata, &catalog, table).await?;

    assert_eq!(
        metadata
            .wal_read_after(&catalog, table, None, 10)
            .await?
            .iter()
            .map(|batch| batch.sequence)
            .collect::<Vec<_>>(),
        vec![2]
    );

    // Publication can proceed while the next schema's checkpoint waits.
    clock.set(32_000);
    let publication = metadata
        .claim_task(Uuid::from_u128(99))
        .await?
        .ok_or("missing publication")?;
    let second = metadata
        .claim_task(worker)
        .await?
        .ok_or("missing second task")?;

    insta::assert_debug_snapshot!(second.task, @"
    Materialize {
        epoch: 2,
        range: WalRange {
            start: 2,
            end: 3,
        },
        schema_id: 7,
    }
    ");
    insta::assert_debug_snapshot!(metadata.claim_task(other_worker).await?, @"None");

    let second_file = DataFileMetadata {
        path: "data/second.parquet".into(),
        ..file.clone()
    };
    let next_vector = DeletionVectorMetadata {
        puffin_path: "deletes/second.puffin".into(),
        cardinality: 5,
        ..vector.clone()
    };
    let mut second_result = new_result_from_assignment(&second)?;
    second_result.index_delta = Some(second.token.attempt_id);
    second_result.data_files.push(second_file.clone());
    second_result
        .deletion_vectors
        .push(DeletionVectorReplacement {
            previous: Some(vector.clone()),
            current: next_vector.clone(),
        });

    // Rejections must roll back staged files as well as WAL, revisions, and task ownership.
    let before = serde_json::to_value(metadata.debug_table(&catalog, table).await?)?;
    let mut stale = second_result.clone();
    stale.deletion_vectors[0].previous = None;
    let mut wrong_previous = second_result.clone();
    wrong_previous.deletion_vectors[0].previous = Some(DeletionVectorMetadata {
        puffin_path: "deletes/stale.puffin".into(),
        ..vector.clone()
    });
    let mut unknown = second_result.clone();
    unknown.deletion_vectors[0].current.target_file = "missing.parquet".into();
    unknown.deletion_vectors[0].previous = None;
    let mut duplicate_file = second_result.clone();
    duplicate_file.data_files.push(second_file.clone());
    let mut duplicate_target = second_result.clone();
    duplicate_target
        .deletion_vectors
        .push(duplicate_target.deletion_vectors[0].clone());
    let mut reused_path = second_result.clone();
    reused_path.data_files.push(file.clone());
    let mut too_many_deletes = second_result.clone();
    too_many_deletes.deletion_vectors[0].current.cardinality = 18;
    let mut lost_deletes = second_result.clone();
    lost_deletes.deletion_vectors[0].current.cardinality = 1;
    for invalid in [
        stale,
        wrong_previous,
        unknown,
        duplicate_file,
        duplicate_target,
        reused_path,
        too_many_deletes,
        lost_deletes,
    ] {
        let error = metadata
            .complete_materialization(&second.token, invalid)
            .await
            .err();
        insta::allow_duplicates! {
            insta::assert_compact_debug_snapshot!(error, @"Some(InvalidTaskResult)");
        }
        assert_eq!(
            serde_json::to_value(metadata.debug_table(&catalog, table).await?)?,
            before
        );
    }

    tokio::try_join!(
        metadata.wal_append(
            &catalog,
            table,
            7,
            "arrived-during-task",
            DateTime::UNIX_EPOCH,
        ),
        metadata.complete_materialization(&second.token, second_result)
    )?;

    clock.set(47_000);
    let third = metadata
        .claim_task(worker)
        .await?
        .ok_or("lost concurrent ingestion")?;

    insta::assert_debug_snapshot!(third.task, @"
    Materialize {
        epoch: 3,
        range: WalRange {
            start: 3,
            end: 4,
        },
        schema_id: 7,
    }
    ");

    // Old completion retries neither acknowledge newer WAL nor release another assignment.
    metadata
        .complete_materialization(&first.token, first_result.clone())
        .await?;

    assert_eq!(metadata.claim_task(worker).await?, Some(third.clone()));

    metadata.close().await?;

    let metadata = Metadata::from_db(Db::open("metadata", store).await?, secrets, clock.clone());

    assert_eq!(metadata.claim_task(worker).await?, Some(third.clone()));
    metadata
        .complete_materialization(&first.token, first_result.clone())
        .await?;

    let state = metadata.debug_table(&catalog, table).await?;
    assert_eq!(
        state.files,
        vec![
            VersionedDataFile {
                metadata: file,
                schema_id: 3,
                added_revision: 1,
                removed_revision: None
            },
            VersionedDataFile {
                metadata: second_file,
                schema_id: 7,
                added_revision: 2,
                removed_revision: None
            },
        ]
    );
    assert_eq!(
        state.deletion_vectors,
        vec![
            VersionedDeletionVector {
                metadata: vector,
                added_revision: 1,
                removed_revision: Some(2)
            },
            VersionedDeletionVector {
                metadata: next_vector,
                added_revision: 2,
                removed_revision: None
            },
        ]
    );
    let json = serde_json::to_value(&state)?;
    assert_eq!(json["desired_revision"], 2);
    assert_eq!(json["reconciled_revision"], 0);
    assert_eq!(json["materialized_next"], 3);

    metadata
        .complete_materialization(&third.token, new_result_from_assignment(&third)?)
        .await?;

    let final_state = serde_json::to_value(metadata.debug_table(&catalog, table).await?)?;
    assert_eq!(final_state["desired_revision"], 3);
    assert_eq!(final_state["reconciled_revision"], 0);
    assert_eq!(final_state["files"], json["files"]);
    assert_eq!(final_state["deletion_vectors"], json["deletion_vectors"]);

    // Identical object paths in other catalogs or tables have independent state.
    for (other_catalog, other_table) in [
        (CatalogId::new("other")?, table),
        (catalog.clone(), Uuid::from_u128(43)),
    ] {
        metadata.heartbeat_task(&publication.token).await?;
        let final_state = serde_json::to_value(metadata.debug_table(&catalog, table).await?)?;
        common::resolve(&metadata, &other_catalog, other_table).await?;
        metadata
            .wal_append(&other_catalog, other_table, 3, "wal", DateTime::UNIX_EPOCH)
            .await?;
        clock.set(clock.now().timestamp_millis() + 15_000);
        let first_claim = metadata
            .claim_task(Uuid::from_u128(98))
            .await?
            .ok_or("missing isolated task")?;
        let task = if matches!(first_claim.task, Task::Reconcile { .. }) {
            metadata
                .claim_task(worker)
                .await?
                .ok_or("missing isolated materialization")?
        } else {
            first_claim
        };
        let mut result = new_result_from_assignment(&task)?;
        result.data_files = first_result.data_files.clone();
        result.deletion_vectors = first_result.deletion_vectors.clone();
        metadata
            .complete_materialization(&task.token, result)
            .await?;
        let isolated = metadata.debug_table(&other_catalog, other_table).await?;
        assert_eq!(isolated.files.len(), 1);
        assert_eq!(isolated.deletion_vectors.len(), 1);
        assert_eq!(isolated.deletion_vectors[0].removed_revision, None);
        assert_eq!(
            serde_json::to_value(metadata.debug_table(&catalog, table).await?)?,
            final_state
        );
    }

    insta::assert_debug_snapshot!(metadata.claim_task(worker).await?, @"None");
    insta::assert_debug_snapshot!(metadata.wal_read_after(&catalog, table, None, 10).await?, @"[]");

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn fences_expired_attempts_renews_heartbeats_and_retries_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(MockSystemClock::with_time(1000));
    let metadata = Metadata::from_db(
        Db::open("metadata", Arc::new(InMemory::new())).await?,
        Arc::new(InMemorySecretStore::new()),
        clock.clone(),
    );

    let catalog = CatalogId::new("catalog")?;
    let table = Uuid::from_u128(42);
    common::resolve(&metadata, &catalog, table).await?;

    metadata
        .wal_append(&catalog, table, 3, "wal", DateTime::UNIX_EPOCH)
        .await?;

    clock.set(16_000);
    let first = metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing task")?;

    insta::assert_debug_snapshot!(first.task, @"
    Materialize {
        epoch: 1,
        range: WalRange {
            start: 0,
            end: 1,
        },
        schema_id: 3,
    }
    ");

    clock.set(45_999);

    let renewed = metadata.heartbeat_task(&first.token).await?;

    assert_eq!(renewed.token, first.token);

    clock.set(46_000);

    insta::assert_debug_snapshot!(metadata.claim_task(Uuid::from_u128(2)).await?, @"None");

    clock.set(75_999);

    insta::assert_compact_debug_snapshot!(metadata.heartbeat_task(&first.token).await.err(), @"Some(TaskFenced)");
    insta::assert_compact_debug_snapshot!(metadata.complete_materialization(&first.token, new_result_from_assignment(&first)?).await.err(), @"Some(TaskFenced)");

    let second = metadata
        .claim_task(Uuid::from_u128(2))
        .await?
        .ok_or("expired work not recovered")?;

    insta::assert_debug_snapshot!(second.task, @"
    Materialize {
        epoch: 2,
        range: WalRange {
            start: 0,
            end: 1,
        },
        schema_id: 3,
    }
    ");
    insta::assert_compact_debug_snapshot!(metadata.fail_task(&first.token).await.err(), @"Some(TaskFenced)");

    metadata.fail_task(&second.token).await?;
    insta::assert_compact_debug_snapshot!(metadata.complete_materialization(&second.token, new_result_from_assignment(&second)?).await.err(), @"Some(TaskFenced)");

    clock.set(80_998);

    insta::assert_debug_snapshot!(metadata.claim_task(Uuid::from_u128(1)).await?, @"None");

    clock.set(80_999);

    let third = metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("retry missing")?;
    insta::assert_debug_snapshot!(third.task, @"
    Materialize {
        epoch: 3,
        range: WalRange {
            start: 0,
            end: 1,
        },
        schema_id: 3,
    }
    ");

    metadata
        .complete_materialization(&third.token, new_result_from_assignment(&third)?)
        .await?;

    insta::assert_debug_snapshot!(metadata.claim_task(Uuid::from_u128(1)).await?, @"None");
    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn concurrent_claims_enforce_worker_and_table_exclusivity()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(MockSystemClock::with_time(1000));
    let metadata = Metadata::from_db(
        Db::open("metadata", Arc::new(InMemory::new())).await?,
        Arc::new(InMemorySecretStore::new()),
        clock.clone(),
    );

    let catalog = CatalogId::new("catalog")?;

    for table in [42, 43] {
        common::resolve(&metadata, &catalog, Uuid::from_u128(table)).await?;
        metadata
            .wal_append(
                &catalog,
                Uuid::from_u128(table),
                1,
                "wal",
                DateTime::UNIX_EPOCH,
            )
            .await?;
    }

    clock.set(16_000);
    let same_worker = try_join_all((0..8).map(|_| metadata.claim_task(Uuid::from_u128(1)))).await?;

    for assignment in &same_worker {
        assert_eq!(assignment, &same_worker[0]);
    }

    let claims = try_join_all((2..10).map(|id| metadata.claim_task(Uuid::from_u128(id)))).await?;

    assert_eq!(claims.iter().flatten().count(), 1);

    let other = claims
        .into_iter()
        .flatten()
        .next()
        .ok_or("missing other table")?;

    let first = same_worker[0].as_ref().ok_or("missing first assignment")?;

    assert_ne!(first.token.table_uuid, other.token.table_uuid);

    // Recovering the old table must not erase its former worker's newer assignment.
    clock.set(17_000);
    common::resolve(&metadata, &catalog, Uuid::from_u128(44)).await?;

    metadata
        .wal_append(
            &catalog,
            Uuid::from_u128(44),
            1,
            "new-table",
            DateTime::UNIX_EPOCH,
        )
        .await?;

    clock.set(46_000);

    let newer = metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing newer work")?;

    assert_eq!(newer.token.table_uuid, Uuid::from_u128(44));

    let recovered = metadata
        .claim_task(Uuid::from_u128(11))
        .await?
        .ok_or("missing expired work")?;

    assert_eq!(recovered.token.table_uuid, first.token.table_uuid);
    assert_eq!(metadata.claim_task(Uuid::from_u128(1)).await?, Some(newer));

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn rejects_wrong_ranges_schemas_and_worker_tokens_without_losing_work()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(MockSystemClock::with_time(1000));
    let metadata = Metadata::from_db(
        Db::open("metadata", Arc::new(InMemory::new())).await?,
        Arc::new(InMemorySecretStore::new()),
        clock.clone(),
    );

    let catalog = CatalogId::new("catalog")?;
    let table = Uuid::from_u128(42);
    common::resolve(&metadata, &catalog, table).await?;

    metadata
        .wal_append(&catalog, table, 3, "wal", DateTime::UNIX_EPOCH)
        .await?;

    clock.set(16_000);
    let task = metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing task")?;

    let valid = new_result_from_assignment(&task)?;

    let mut wrong = valid.clone();
    wrong.range.end += 1;
    insta::assert_compact_debug_snapshot!(metadata.complete_materialization(&task.token, wrong).await.err(), @"Some(InvalidTaskResult)");

    let mut wrong = valid.clone();
    wrong.schema_id += 1;
    insta::assert_compact_debug_snapshot!(metadata.complete_materialization(&task.token, wrong).await.err(), @"Some(InvalidTaskResult)");

    let mut token = task.token.clone();
    token.worker_id = Uuid::from_u128(99);
    insta::assert_compact_debug_snapshot!(metadata.complete_materialization(&token, valid.clone()).await.err(), @"Some(TaskFenced)");

    assert_eq!(
        metadata
            .wal_read_after(&catalog, table, None, 10)
            .await?
            .len(),
        1
    );

    metadata
        .complete_materialization(&task.token, valid.clone())
        .await?;

    let mut altered = valid;
    altered.schema_id += 1;
    insta::assert_compact_debug_snapshot!(metadata.complete_materialization(&task.token, altered).await.err(), @"Some(InvalidTaskResult)");

    insta::assert_debug_snapshot!(metadata.claim_task(Uuid::from_u128(1)).await?, @"None");

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn accepted_index_deltas_are_fenced_ordered_and_durable()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let secrets = Arc::new(InMemorySecretStore::new());
    let clock = Arc::new(MockSystemClock::with_time(1000));
    let metadata = Metadata::from_db(
        Db::open("metadata", store.clone()).await?,
        secrets.clone(),
        clock.clone(),
    );
    let catalog = CatalogId::new("catalog")?;
    let table = Uuid::from_u128(42);
    let worker = Uuid::from_u128(1);
    common::resolve(&metadata, &catalog, table).await?;
    // Different schemas make three assignments; the first delta spans two batches.
    for schema in [3, 3, 4, 5] {
        metadata
            .wal_append(&catalog, table, schema, "wal", DateTime::UNIX_EPOCH)
            .await?;
    }
    clock.set(16_000);
    let first = metadata.claim_task(worker).await?.ok_or("missing task")?;
    insta::assert_compact_debug_snapshot!(metadata.materialization_input(&first.token, 0).await?, @"MaterializationInput { deltas: [] }");
    let mut result = new_result_from_assignment(&first)?;
    result.index_delta = Some(Uuid::nil());
    insta::assert_compact_debug_snapshot!(metadata.complete_materialization(&first.token, result.clone()).await.err(), @"Some(InvalidTaskResult)");
    insta::assert_debug_snapshot!(metadata.debug_table(&catalog, table).await?.index_deltas, @"[]");
    result.index_delta = Some(first.token.attempt_id);
    metadata
        .complete_materialization(&first.token, result.clone())
        .await?;

    clock.set(31_000);
    let reconcile = metadata
        .claim_task(Uuid::from_u128(2))
        .await?
        .ok_or("missing reconciliation")?;
    let second = metadata
        .claim_task(worker)
        .await?
        .ok_or("missing second task")?;
    let first_delta = se_metadata::IndexDelta {
        id: first.token.attempt_id,
        range: result.range,
    };
    assert_eq!(
        metadata
            .materialization_input(&second.token, 0)
            .await?
            .deltas,
        vec![first_delta.clone()]
    );
    insta::assert_compact_debug_snapshot!(metadata.materialization_input(&second.token, 1).await.err(), @"Some(InvalidTaskResult)");
    insta::assert_compact_debug_snapshot!(metadata.materialization_input(&second.token, 3).await.err(), @"Some(InvalidTaskResult)");
    insta::assert_debug_snapshot!(metadata.materialization_input(&second.token, 2).await?.deltas, @"[]");
    insta::assert_compact_debug_snapshot!(metadata.materialization_input(&first.token, 0).await.err(), @"Some(TaskFenced)");
    let mut forged = second.token.clone();
    forged.attempt_id = first.token.attempt_id;
    insta::assert_compact_debug_snapshot!(metadata.materialization_input(&forged, 0).await.err(), @"Some(TaskFenced)");
    let mut second_result = new_result_from_assignment(&second)?;
    second_result.index_delta = Some(second.token.attempt_id);
    metadata
        .complete_materialization(&second.token, second_result.clone())
        .await?;
    clock.set(46_000);
    let third = metadata
        .claim_task(worker)
        .await?
        .ok_or("missing third task")?;
    let expected = vec![
        first_delta,
        se_metadata::IndexDelta {
            id: second.token.attempt_id,
            range: second_result.range,
        },
    ];
    assert_eq!(
        metadata
            .materialization_input(&third.token, 0)
            .await?
            .deltas,
        expected
    );
    assert_eq!(
        metadata
            .materialization_input(&third.token, 2)
            .await?
            .deltas,
        expected[1..]
    );
    // A retry must not release this worker's newer assignment or increment revisions.
    let before = serde_json::to_value(metadata.debug_table(&catalog, table).await?)?;
    metadata
        .complete_materialization(&first.token, result.clone())
        .await?;
    assert_eq!(
        serde_json::to_value(metadata.debug_table(&catalog, table).await?)?,
        before
    );
    let mut mismatch = result.clone();
    mismatch.index_delta = None;
    insta::assert_compact_debug_snapshot!(metadata.complete_materialization(&first.token, mismatch).await.err(), @"Some(InvalidTaskResult)");
    let mut wrong_token = first.token.clone();
    wrong_token.worker_id = Uuid::nil();
    insta::assert_compact_debug_snapshot!(metadata.complete_materialization(&wrong_token, result.clone()).await.err(), @"Some(InvalidTaskResult)");
    // Reconciliation assignments cannot read materialization input.
    metadata.heartbeat_task(&reconcile.token).await?;
    insta::assert_compact_debug_snapshot!(metadata.materialization_input(&reconcile.token, 0).await.err(), @"Some(InvalidTaskResult)");
    metadata.close().await?;

    clock.set(76_000);
    let metadata = Metadata::from_db(Db::open("metadata", store).await?, secrets, clock);
    metadata
        .complete_materialization(&first.token, result)
        .await?;
    assert_eq!(
        metadata.debug_table(&catalog, table).await?.index_deltas,
        expected
    );
    insta::assert_compact_debug_snapshot!(metadata.materialization_input(&third.token, 0).await.err(), @"Some(TaskFenced)");
    let recovered = metadata
        .claim_task(worker)
        .await?
        .ok_or("missing recovered task")?;
    assert_eq!(
        metadata
            .materialization_input(&recovered.token, 0)
            .await?
            .deltas,
        expected
    );
    let append_result = new_result_from_assignment(&recovered)?;
    metadata
        .complete_materialization(&recovered.token, append_result.clone())
        .await?;
    metadata
        .complete_materialization(&recovered.token, append_result)
        .await?;
    assert_eq!(
        metadata.debug_table(&catalog, table).await?.index_deltas,
        expected
    );
    metadata.close().await?;
    Ok(())
}

#[tokio::test]
async fn completion_retry_waits_for_durable_acceptance() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let secrets = Arc::new(InMemorySecretStore::new());
    let clock = Arc::new(MockSystemClock::with_time(1000));
    let metadata = Metadata::from_db(
        Db::open("metadata", store.clone()).await?,
        secrets.clone(),
        clock.clone(),
    );
    let catalog = CatalogId::new("catalog")?;
    let table = Uuid::from_u128(42);
    common::resolve(&metadata, &catalog, table).await?;
    metadata
        .wal_append(&catalog, table, 0, "wal", DateTime::UNIX_EPOCH)
        .await?;
    clock.set(16_000);
    let assignment = metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing task")?;
    metadata.close().await?;

    // Disable background flushing so a visible receipt is not necessarily durable.
    let db = Db::builder("metadata", store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    let metadata = Metadata::from_db(db, secrets.clone(), clock.clone());
    let mut result = new_result_from_assignment(&assignment)?;
    result.index_delta = Some(assignment.token.attempt_id);
    let completing = {
        let metadata = metadata.clone();
        let assignment = assignment.clone();
        let result = result.clone();
        tokio::spawn(async move {
            metadata
                .complete_materialization(&assignment.token, result)
                .await
        })
    };
    let expected = vec![se_metadata::IndexDelta {
        id: assignment.token.attempt_id,
        range: result.range,
    }];
    let mut visible = false;
    for _ in 0..10000 {
        if metadata.debug_table(&catalog, table).await?.index_deltas == expected {
            visible = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    if !visible || completing.is_finished() {
        return Err("fixture did not expose an acceptance awaiting durability".into());
    }
    metadata
        .complete_materialization(&assignment.token, result)
        .await?;

    // Capture only object-store state before any graceful close can flush it.
    let crashed = Arc::new(InMemory::new());
    let mut objects = store.list(None);
    while let Some(object) = objects.try_next().await? {
        let bytes = store.get(&object.location).await?.bytes().await?;
        crashed.put(&object.location, bytes.into()).await?;
    }
    let recovered = Metadata::from_db(Db::open("metadata", crashed).await?, secrets, clock);
    assert_eq!(
        recovered.debug_table(&catalog, table).await?.index_deltas,
        expected
    );
    insta::assert_debug_snapshot!(recovered.wal_read_after(&catalog, table, None, 10).await?, @"[]");
    completing.await??;
    recovered.close().await?;
    metadata.close().await?;
    Ok(())
}

#[tokio::test]
async fn checkpoint_interval_batches_writes_and_survives_restarts()
-> Result<(), Box<dyn std::error::Error>> {
    for (value, seconds) in [(None, 60), (Some("15"), 15), (Some("900"), 900)] {
        let store = Arc::new(InMemory::new());
        let secrets = Arc::new(InMemorySecretStore::new());
        let clock = Arc::new(MockSystemClock::with_time(1000));
        let metadata = Metadata::from_db(
            Db::open("metadata", store.clone()).await?,
            secrets.clone(),
            clock.clone(),
        );
        let catalog = CatalogId::new("catalog")?;
        let table = Uuid::from_u128(42);
        let worker = Uuid::from_u128(1);
        let properties = value
            .map(|value: &str| HashMap::from([(CHECKPOINT_INTERVAL_PROPERTY.into(), value.into())]))
            .unwrap_or_default();
        common::resolve_with_properties(&metadata, &catalog, table, properties).await?;
        metadata
            .wal_append(&catalog, table, 3, "first", DateTime::UNIX_EPOCH)
            .await?;

        let first_due = 1000 + seconds * 1000;
        let task = metadata.claim_task(worker).await?;
        insta::allow_duplicates! { insta::assert_debug_snapshot!(task, @"None"); }
        clock.set(first_due - 1);
        let task = metadata.claim_task(worker).await?;
        insta::allow_duplicates! { insta::assert_debug_snapshot!(task, @"None"); }
        // More writes must join the batch without pushing its deadline back.
        metadata
            .wal_append(&catalog, table, 3, "second", DateTime::UNIX_EPOCH)
            .await?;
        metadata.close().await?;
        let metadata =
            Metadata::from_db(Db::open("metadata", store).await?, secrets, clock.clone());
        clock.set(first_due);
        let first = metadata
            .claim_task(worker)
            .await?
            .ok_or("missing first checkpoint")?;
        let result = new_result_from_assignment(&first)?;
        assert_eq!(result.range, WalRange { start: 0, end: 2 });
        assert_eq!(result.schema_id, 3);

        // Writes arriving during materialization must also respect the interval.
        metadata
            .wal_append(&catalog, table, 7, "third", DateTime::UNIX_EPOCH)
            .await?;
        metadata
            .complete_materialization(&first.token, result)
            .await?;
        let second_due = first_due + seconds * 1000;
        let task = metadata.claim_task(worker).await?;
        insta::allow_duplicates! { insta::assert_debug_snapshot!(task, @"None"); }
        clock.set(second_due - 1);
        // Publication is independent of checkpoint pacing; let another worker claim it.
        let publication = metadata
            .claim_task(Uuid::from_u128(2))
            .await?
            .ok_or("missing publication")?;
        if !matches!(publication.task, Task::Reconcile { .. }) {
            return Err("checkpoint became eligible before its deadline".into());
        }
        let task = metadata.claim_task(worker).await?;
        insta::allow_duplicates! { insta::assert_debug_snapshot!(task, @"None"); }
        clock.set(second_due);
        let second = metadata
            .claim_task(worker)
            .await?
            .ok_or("missing second checkpoint")?;
        let result = new_result_from_assignment(&second)?;
        assert_eq!(result.range, WalRange { start: 2, end: 3 });
        assert_eq!(result.schema_id, 7);
        metadata
            .complete_materialization(&second.token, result)
            .await?;
        metadata.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn checkpoint_interval_is_validated_and_refreshed_on_resolve()
-> Result<(), Box<dyn std::error::Error>> {
    let metadata = Metadata::from_db(
        Db::open("metadata", Arc::new(InMemory::new())).await?,
        Arc::new(InMemorySecretStore::new()),
        Arc::new(MockSystemClock::with_time(1000)),
    );
    let catalog = CatalogId::new("catalog")?;
    let table = Uuid::from_u128(42);
    for value in [
        "-1",
        "0",
        "14",
        "901",
        "1.5",
        "",
        "invalid",
        "18446744073709551616",
        "18446744073709551615",
        "9223372036854775",
    ] {
        let error = common::resolve_with_properties(
            &metadata,
            &catalog,
            table,
            HashMap::from([(CHECKPOINT_INTERVAL_PROPERTY.into(), value.into())]),
        )
        .await
        .err()
        .ok_or("invalid interval must fail table resolution")?;
        if !matches!(
            error.downcast_ref::<Error>(),
            Some(Error::InvalidCheckpointInterval(_))
        ) {
            return Err(error);
        }
    }
    for (value, expected) in [(Some("15"), 15), (None, 60), (Some("900"), 900)] {
        common::resolve_with_properties(
            &metadata,
            &catalog,
            table,
            value
                .map(|value: &str| {
                    HashMap::from([(CHECKPOINT_INTERVAL_PROPERTY.into(), value.into())])
                })
                .unwrap_or_default(),
        )
        .await?;
        assert_eq!(
            metadata
                .debug_table(&catalog, table)
                .await?
                .state
                .checkpoint_interval_sec,
            expected
        );
    }
    metadata.close().await?;
    Ok(())
}

fn new_result_from_assignment(assignment: &TaskAssignment) -> Result<MaterializationResult, Error> {
    let Task::Materialize {
        range, schema_id, ..
    } = assignment.task
    else {
        return Err(Error::InvalidTaskResult);
    };
    Ok(MaterializationResult {
        range,
        schema_id,
        index_delta: None,
        data_files: Vec::new(),
        deletion_vectors: Vec::new(),
    })
}
