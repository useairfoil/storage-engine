mod common;

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use chrono::DateTime;
use common::TestEnvironment;
use futures::{TryStreamExt, future::try_join_all};
use iceberg::spec::DataFileFormat;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};
use se_common::clock::SystemClock;
use se_metadata::{
    CHECKPOINT_INTERVAL_PROPERTY, CatalogId, DataFileMetadata, DeletionVectorMetadata,
    DeletionVectorReplacement, Error, IndexDelta, MaterializationResult, Metadata, Task,
    TaskAssignment,
};
use slatedb::Db;
use uuid::Uuid;

#[tokio::test]
async fn coalesces_same_schema_batches_and_preserves_concurrent_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let mut env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);
    let worker = Uuid::from_u128(1);
    let other_worker = Uuid::from_u128(2);

    let ident = env.resolve(&catalog, table).await?;

    for schema in [3, 3, 7] {
        append_wal(&env.metadata, &catalog, table, schema, "wal").await?;
    }

    // First task coalesces the two schema-3 batches and stops before schema 7.
    env.clock.set(17_000);
    let first = env
        .metadata
        .claim_task(worker)
        .await?
        .ok_or("missing first task")?;

    assert_eq!(first.token.table_ident, ident);
    insta::assert_debug_snapshot!(first.eligible_at, @"
    Some(
        1970-01-01T00:00:16Z,
    )
    ");
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

    // Same worker gets the same live assignment; other workers are excluded.
    assert_eq!(env.metadata.claim_task(worker).await?, Some(first.clone()));
    insta::assert_debug_snapshot!(env.metadata.claim_task(other_worker).await?, @"None");

    // Resolving again does not disturb the active assignment.
    env.resolve(&catalog, table).await?;

    let first_file = sample_data_file("data/first.parquet");
    let first_vector = sample_deletion_vector("deletes/first.puffin", &first_file.path, 2);
    let mut first_result = common::empty_materialization_result(&first)?;
    first_result.data_files.push(first_file.clone());
    first_result
        .deletion_vectors
        .push(DeletionVectorReplacement {
            previous: None,
            current: first_vector.clone(),
        });

    env.metadata
        .complete_materialization(&first.token, first_result.clone())
        .await?;
    env.resolve(&catalog, table).await?;

    let remaining_sequences: Vec<u64> = env
        .metadata
        .wal_read_after(&catalog, table, None, 10)
        .await?
        .into_iter()
        .map(|batch| batch.sequence)
        .collect();
    insta::assert_compact_debug_snapshot!(remaining_sequences, @"[2]");

    // Reconciliation and the next schema's materialization can proceed in parallel.
    env.clock.set(32_000);
    let publication = env
        .metadata
        .claim_task(Uuid::from_u128(99))
        .await?
        .ok_or("missing publication")?;
    assert!(matches!(publication.task, Task::Reconcile { .. }));

    let second = env
        .metadata
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
    insta::assert_debug_snapshot!(env.metadata.claim_task(other_worker).await?, @"None");

    let second_file = sample_data_file("data/second.parquet");
    let next_vector = sample_deletion_vector("deletes/second.puffin", &first_file.path, 5);
    let mut second_result = common::empty_materialization_result(&second)?;
    second_result.index_delta = Some(second.token.attempt_id);
    second_result.data_files.push(second_file);
    second_result
        .deletion_vectors
        .push(DeletionVectorReplacement {
            previous: Some(first_vector),
            current: next_vector,
        });

    // A WAL append racing with completion must not be lost.
    tokio::try_join!(
        append_wal(&env.metadata, &catalog, table, 7, "arrived-during-task"),
        env.metadata
            .complete_materialization(&second.token, second_result),
    )?;

    env.clock.set(47_000);
    let third = env
        .metadata
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

    // Retrying an old completion neither acknowledges newer WAL nor releases a newer task.
    env.metadata
        .complete_materialization(&first.token, first_result.clone())
        .await?;
    assert_eq!(env.metadata.claim_task(worker).await?, Some(third.clone()));

    env.reopen().await?;
    assert_eq!(env.metadata.claim_task(worker).await?, Some(third.clone()));
    env.metadata
        .complete_materialization(&first.token, first_result)
        .await?;

    let state = env.metadata.debug_table(&catalog, table).await?;
    insta::assert_debug_snapshot!(state.files, @r#"
    [
        VersionedDataFile {
            metadata: DataFileMetadata {
                path: "data/first.parquet",
                format: Parquet,
                size: 1234,
                num_rows: 17,
                partition_spec_id: 2,
                partition: {
                    1000: Some(
                        [
                            5,
                            0,
                            0,
                            0,
                        ],
                    ),
                    1001: None,
                },
                sort_order_id: Some(
                    3,
                ),
                column_sizes: {
                    1: 120,
                },
                value_counts: {
                    1: 17,
                },
                null_value_counts: {
                    1: 2,
                },
                nan_value_counts: {
                    1: 0,
                },
                lower_bounds: {
                    1: [
                        4,
                        0,
                        0,
                        0,
                    ],
                },
                upper_bounds: {
                    1: [
                        99,
                        0,
                        0,
                        0,
                    ],
                },
            },
            schema_id: 3,
            added_revision: 1,
            removed_revision: None,
        },
        VersionedDataFile {
            metadata: DataFileMetadata {
                path: "data/second.parquet",
                format: Parquet,
                size: 1234,
                num_rows: 17,
                partition_spec_id: 2,
                partition: {
                    1000: Some(
                        [
                            5,
                            0,
                            0,
                            0,
                        ],
                    ),
                    1001: None,
                },
                sort_order_id: Some(
                    3,
                ),
                column_sizes: {
                    1: 120,
                },
                value_counts: {
                    1: 17,
                },
                null_value_counts: {
                    1: 2,
                },
                nan_value_counts: {
                    1: 0,
                },
                lower_bounds: {
                    1: [
                        4,
                        0,
                        0,
                        0,
                    ],
                },
                upper_bounds: {
                    1: [
                        99,
                        0,
                        0,
                        0,
                    ],
                },
            },
            schema_id: 7,
            added_revision: 2,
            removed_revision: None,
        },
    ]
    "#);
    insta::assert_debug_snapshot!(state.deletion_vectors, @r#"
    [
        VersionedDeletionVector {
            metadata: DeletionVectorMetadata {
                puffin_path: "deletes/first.puffin",
                target_file: "data/first.parquet",
                cardinality: 2,
                file_size: 90,
                offset: 10,
                length: 30,
            },
            added_revision: 1,
            removed_revision: Some(
                2,
            ),
        },
        VersionedDeletionVector {
            metadata: DeletionVectorMetadata {
                puffin_path: "deletes/second.puffin",
                target_file: "data/first.parquet",
                cardinality: 5,
                file_size: 90,
                offset: 10,
                length: 30,
            },
            added_revision: 2,
            removed_revision: None,
        },
    ]
    "#);
    insta::assert_compact_debug_snapshot!(
        (
            state.state.desired_revision,
            state.state.reconciled_revision,
            state.state.materialized_next,
        ),
        @"(2, 0, 3)"
    );

    env.metadata
        .complete_materialization(&third.token, common::empty_materialization_result(&third)?)
        .await?;

    let final_state = env.metadata.debug_table(&catalog, table).await?;
    insta::assert_compact_debug_snapshot!(
        (
            final_state.state.desired_revision,
            final_state.state.reconciled_revision,
            final_state.state.materialized_next,
        ),
        @"(3, 0, 4)"
    );
    assert_eq!(final_state.files, state.files);
    assert_eq!(final_state.deletion_vectors, state.deletion_vectors);
    insta::assert_debug_snapshot!(env.metadata.claim_task(worker).await?, @"None");
    insta::assert_debug_snapshot!(
        env.metadata.wal_read_after(&catalog, table, None, 10).await?,
        @"[]"
    );

    env.metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn rejects_invalid_file_and_deletion_vector_results_without_mutating_state()
-> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        metadata,
        server,
        clock,
        ..
    } = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);
    let worker = Uuid::from_u128(1);

    common::resolve(&metadata, &server, &catalog, table).await?;
    append_wal(&metadata, &catalog, table, 3, "wal/0").await?;
    append_wal(&metadata, &catalog, table, 7, "wal/1").await?;

    // Seed revision 1 with one data file (17 rows) and one deletion vector (cardinality 2).
    clock.set(16_000);
    let first = metadata
        .claim_task(worker)
        .await?
        .ok_or("missing first task")?;
    let first_file = sample_data_file("data/first.parquet");
    let first_vector = sample_deletion_vector("deletes/first.puffin", &first_file.path, 2);
    let mut first_result = common::empty_materialization_result(&first)?;
    first_result.data_files.push(first_file.clone());
    first_result
        .deletion_vectors
        .push(DeletionVectorReplacement {
            previous: None,
            current: first_vector.clone(),
        });
    metadata
        .complete_materialization(&first.token, first_result)
        .await?;

    // Hold reconciliation on another worker and claim revision 2's materialization.
    clock.set(31_000);
    let _reconcile = metadata
        .claim_task(Uuid::from_u128(99))
        .await?
        .ok_or("missing reconciliation")?;
    let second = metadata
        .claim_task(worker)
        .await?
        .ok_or("missing second task")?;

    let second_file = sample_data_file("data/second.parquet");
    let valid = MaterializationResult {
        data_files: vec![second_file.clone()],
        deletion_vectors: vec![DeletionVectorReplacement {
            previous: Some(first_vector.clone()),
            current: sample_deletion_vector("deletes/second.puffin", &first_file.path, 5),
        }],
        ..common::empty_materialization_result(&second)?
    };

    let mut missing_previous = valid.clone();
    missing_previous.deletion_vectors[0].previous = None;

    let mut wrong_previous = valid.clone();
    wrong_previous.deletion_vectors[0].previous = Some(sample_deletion_vector(
        "deletes/stale.puffin",
        &first_file.path,
        2,
    ));

    let mut unknown_target = valid.clone();
    unknown_target.deletion_vectors[0].current.target_file = "missing.parquet".into();
    unknown_target.deletion_vectors[0].previous = None;

    let mut duplicate_data_file = valid.clone();
    duplicate_data_file.data_files.push(second_file);

    let mut duplicate_vector_target = valid.clone();
    duplicate_vector_target
        .deletion_vectors
        .push(duplicate_vector_target.deletion_vectors[0].clone());

    let mut reused_existing_path = valid.clone();
    reused_existing_path.data_files.push(first_file);

    let mut cardinality_exceeds_rows = valid.clone();
    cardinality_exceeds_rows.deletion_vectors[0]
        .current
        .cardinality = 18;

    let mut cardinality_decreases = valid.clone();
    cardinality_decreases.deletion_vectors[0]
        .current
        .cardinality = 1;

    let before = serde_json::to_value(metadata.debug_table(&catalog, table).await?)?;
    let mut outcomes = Vec::new();

    for (label, invalid) in [
        ("missing_previous", missing_previous),
        ("wrong_previous", wrong_previous),
        ("unknown_target", unknown_target),
        ("duplicate_data_file", duplicate_data_file),
        ("duplicate_vector_target", duplicate_vector_target),
        ("reused_existing_path", reused_existing_path),
        ("cardinality_exceeds_rows", cardinality_exceeds_rows),
        ("cardinality_decreases", cardinality_decreases),
    ] {
        let err = metadata
            .complete_materialization(&second.token, invalid)
            .await
            .err();
        outcomes.push((label, format!("{err:?}")));
        assert_eq!(
            serde_json::to_value(metadata.debug_table(&catalog, table).await?)?,
            before
        );
    }

    insta::assert_debug_snapshot!(outcomes, @r#"
    [
        (
            "missing_previous",
            "Some(InvalidTaskResult)",
        ),
        (
            "wrong_previous",
            "Some(InvalidTaskResult)",
        ),
        (
            "unknown_target",
            "Some(InvalidTaskResult)",
        ),
        (
            "duplicate_data_file",
            "Some(InvalidTaskResult)",
        ),
        (
            "duplicate_vector_target",
            "Some(InvalidTaskResult)",
        ),
        (
            "reused_existing_path",
            "Some(InvalidTaskResult)",
        ),
        (
            "cardinality_exceeds_rows",
            "Some(InvalidTaskResult)",
        ),
        (
            "cardinality_decreases",
            "Some(InvalidTaskResult)",
        ),
    ]
    "#);

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn isolates_materialized_files_and_vectors_across_catalogs_and_tables()
-> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        metadata,
        server,
        clock,
        ..
    } = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);
    let worker = Uuid::from_u128(1);

    let file = sample_data_file("data/shared.parquet");
    let vector = sample_deletion_vector("deletes/shared.puffin", &file.path, 2);

    common::resolve(&metadata, &server, &catalog, table).await?;
    append_wal(&metadata, &catalog, table, 3, "wal/0").await?;
    clock.set(16_000);
    let first = metadata
        .claim_task(worker)
        .await?
        .ok_or("missing primary materialization")?;
    metadata
        .complete_materialization(
            &first.token,
            MaterializationResult {
                data_files: vec![file.clone()],
                deletion_vectors: vec![DeletionVectorReplacement {
                    previous: None,
                    current: vector.clone(),
                }],
                ..common::empty_materialization_result(&first)?
            },
        )
        .await?;

    // Hold primary table's reconciliation so it doesn't interfere with later claims.
    clock.set(21_000);
    let publication = metadata
        .claim_task(Uuid::from_u128(99))
        .await?
        .ok_or("missing publication")?;

    // Identical object paths in another catalog or another table have independent state.
    for (other_catalog, other_table) in [
        (CatalogId::from(Uuid::from_u128(2)), table),
        (catalog.clone(), Uuid::from_u128(43)),
    ] {
        metadata.heartbeat_task(&publication.token).await?;
        let primary_before = serde_json::to_value(metadata.debug_table(&catalog, table).await?)?;
        common::resolve(&metadata, &server, &other_catalog, other_table).await?;
        append_wal(&metadata, &other_catalog, other_table, 3, "wal").await?;
        clock.set(clock.now().timestamp_millis() + 15_000);

        let task = claim_materialization(&metadata, worker).await?;
        metadata
            .complete_materialization(
                &task.token,
                MaterializationResult {
                    data_files: vec![file.clone()],
                    deletion_vectors: vec![DeletionVectorReplacement {
                        previous: None,
                        current: vector.clone(),
                    }],
                    ..common::empty_materialization_result(&task)?
                },
            )
            .await?;

        let isolated = metadata.debug_table(&other_catalog, other_table).await?;
        insta::allow_duplicates! {
            insta::assert_compact_debug_snapshot!(
                (isolated.files.len(), isolated.deletion_vectors.len(), isolated.deletion_vectors[0].removed_revision),
                @"(1, 1, None)"
            );
        }
        assert_eq!(
            serde_json::to_value(metadata.debug_table(&catalog, table).await?)?,
            primary_before
        );
    }

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn fences_expired_attempts_renews_heartbeats_and_retries_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        metadata,
        server,
        clock,
        ..
    } = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);
    let worker_a = Uuid::from_u128(1);
    let worker_b = Uuid::from_u128(2);

    common::resolve(&metadata, &server, &catalog, table).await?;
    append_wal(&metadata, &catalog, table, 3, "wal").await?;

    clock.set(16_000);
    let first = metadata.claim_task(worker_a).await?.ok_or("missing task")?;
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

    // Heartbeat 1ms before the 30s lease expiry keeps the assignment alive.
    clock.set(45_999);
    let renewed = metadata.heartbeat_task(&first.token).await?;
    assert_eq!(renewed.token, first.token);

    clock.set(46_000);
    insta::assert_debug_snapshot!(metadata.claim_task(worker_b).await?, @"None");

    // Once the renewed lease expires at 75_999, all calls from the stale attempt are fenced.
    clock.set(75_999);
    insta::assert_compact_debug_snapshot!(
        metadata.heartbeat_task(&first.token).await.err(),
        @"Some(TaskFenced)"
    );
    insta::assert_compact_debug_snapshot!(
        metadata
            .complete_materialization(&first.token, common::empty_materialization_result(&first)?)
            .await
            .err(),
        @"Some(TaskFenced)"
    );

    let second = metadata
        .claim_task(worker_b)
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
    insta::assert_compact_debug_snapshot!(
        metadata.fail_task(&first.token).await.err(),
        @"Some(TaskFenced)"
    );

    // Explicit failure fences the attempt and enforces the 5s retry delay.
    metadata.fail_task(&second.token).await?;
    insta::assert_compact_debug_snapshot!(
        metadata
            .complete_materialization(&second.token, common::empty_materialization_result(&second)?)
            .await
            .err(),
        @"Some(TaskFenced)"
    );

    clock.set(80_998);
    insta::assert_debug_snapshot!(metadata.claim_task(worker_a).await?, @"None");

    clock.set(80_999);
    let third = metadata
        .claim_task(worker_a)
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
        .complete_materialization(&third.token, common::empty_materialization_result(&third)?)
        .await?;
    insta::assert_debug_snapshot!(metadata.claim_task(worker_a).await?, @"None");

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn concurrent_claims_enforce_worker_and_table_exclusivity()
-> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        metadata,
        server,
        clock,
        ..
    } = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));

    for table in [42, 43] {
        common::resolve(&metadata, &server, &catalog, Uuid::from_u128(table)).await?;
        append_wal(&metadata, &catalog, Uuid::from_u128(table), 1, "wal").await?;
    }

    // Concurrent claims from the same worker return the same single assignment.
    clock.set(16_000);
    let same_worker = try_join_all((0..8).map(|_| metadata.claim_task(Uuid::from_u128(1)))).await?;
    for assignment in &same_worker {
        assert_eq!(assignment, &same_worker[0]);
    }
    let first = same_worker[0].as_ref().ok_or("missing first assignment")?;

    // Concurrent claims from other workers can only claim the one remaining table.
    let claims = try_join_all((2..10).map(|id| metadata.claim_task(Uuid::from_u128(id)))).await?;
    insta::assert_debug_snapshot!(claims.iter().flatten().count(), @"1");

    let other = claims
        .into_iter()
        .flatten()
        .next()
        .ok_or("missing other table")?;
    assert_ne!(first.token.table_uuid, other.token.table_uuid);

    // Recovering an expired table must not erase its former worker's newer assignment.
    clock.set(17_000);
    let third_table = Uuid::from_u128(44);
    common::resolve(&metadata, &server, &catalog, third_table).await?;
    append_wal(&metadata, &catalog, third_table, 1, "new-table").await?;

    clock.set(46_000);
    let newer = metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing newer work")?;
    assert_eq!(newer.token.table_uuid, third_table);

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
    let TestEnvironment {
        metadata,
        server,
        clock,
        ..
    } = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);

    common::resolve(&metadata, &server, &catalog, table).await?;
    append_wal(&metadata, &catalog, table, 3, "wal").await?;

    clock.set(16_000);
    let task = metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing task")?;
    let valid = common::empty_materialization_result(&task)?;

    let mut wrong_range = valid.clone();
    wrong_range.range.end += 1;
    insta::assert_compact_debug_snapshot!(
        metadata
            .complete_materialization(&task.token, wrong_range)
            .await
            .err(),
        @"Some(InvalidTaskResult)"
    );

    let mut wrong_schema = valid.clone();
    wrong_schema.schema_id += 1;
    insta::assert_compact_debug_snapshot!(
        metadata
            .complete_materialization(&task.token, wrong_schema)
            .await
            .err(),
        @"Some(InvalidTaskResult)"
    );

    let mut wrong_worker = task.token.clone();
    wrong_worker.worker_id = Uuid::from_u128(99);
    insta::assert_compact_debug_snapshot!(
        metadata
            .complete_materialization(&wrong_worker, valid.clone())
            .await
            .err(),
        @"Some(TaskFenced)"
    );

    insta::assert_debug_snapshot!(
        metadata.wal_read_after(&catalog, table, None, 10).await?.len(),
        @"1"
    );

    metadata
        .complete_materialization(&task.token, valid.clone())
        .await?;

    // Retrying with an altered result after completion is rejected.
    let mut altered = valid;
    altered.schema_id += 1;
    insta::assert_compact_debug_snapshot!(
        metadata
            .complete_materialization(&task.token, altered)
            .await
            .err(),
        @"Some(InvalidTaskResult)"
    );
    insta::assert_debug_snapshot!(metadata.claim_task(Uuid::from_u128(1)).await?, @"None");

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn index_deltas_are_ordered_and_validated_by_cursor() -> Result<(), Box<dyn std::error::Error>>
{
    let env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);
    let worker = Uuid::from_u128(1);

    env.resolve(&catalog, table).await?;
    // Different schemas produce three assignments: [0..2], [2..3], [3..4].
    for schema in [3, 3, 4, 5] {
        append_wal(&env.metadata, &catalog, table, schema, "wal").await?;
    }

    env.clock.set(16_000);
    let first = env
        .metadata
        .claim_task(worker)
        .await?
        .ok_or("missing first task")?;
    insta::assert_compact_debug_snapshot!(
        env.metadata.materialization_input(&first.token, 0).await?,
        @"MaterializationInput { deltas: [] }"
    );

    // Index delta ID must match the assignment's attempt_id.
    let mut first_result = common::empty_materialization_result(&first)?;
    first_result.index_delta = Some(Uuid::nil());
    insta::assert_compact_debug_snapshot!(
        env.metadata
            .complete_materialization(&first.token, first_result.clone())
            .await
            .err(),
        @"Some(InvalidTaskResult)"
    );
    insta::assert_debug_snapshot!(
        env.metadata.debug_table(&catalog, table).await?.index_deltas,
        @"[]"
    );

    first_result.index_delta = Some(first.token.attempt_id);
    env.metadata
        .complete_materialization(&first.token, first_result.clone())
        .await?;

    env.clock.set(31_000);
    let reconcile = env
        .metadata
        .claim_task(Uuid::from_u128(2))
        .await?
        .ok_or("missing reconciliation")?;
    let second = env
        .metadata
        .claim_task(worker)
        .await?
        .ok_or("missing second task")?;

    let first_delta = IndexDelta {
        id: first.token.attempt_id,
        range: first_result.range,
    };
    assert_eq!(
        env.metadata
            .materialization_input(&second.token, 0)
            .await?
            .deltas,
        vec![first_delta.clone()]
    );

    // Cursors must align to delta boundaries and active materialization tokens.
    insta::assert_compact_debug_snapshot!(
        env.metadata.materialization_input(&second.token, 1).await.err(),
        @"Some(InvalidTaskResult)"
    );
    insta::assert_compact_debug_snapshot!(
        env.metadata.materialization_input(&second.token, 3).await.err(),
        @"Some(InvalidTaskResult)"
    );
    insta::assert_debug_snapshot!(
        env.metadata.materialization_input(&second.token, 2).await?.deltas,
        @"[]"
    );
    insta::assert_compact_debug_snapshot!(
        env.metadata.materialization_input(&first.token, 0).await.err(),
        @"Some(TaskFenced)"
    );

    let mut forged = second.token.clone();
    forged.attempt_id = first.token.attempt_id;
    insta::assert_compact_debug_snapshot!(
        env.metadata.materialization_input(&forged, 0).await.err(),
        @"Some(TaskFenced)"
    );

    // Reconciliation assignments cannot read materialization input.
    insta::assert_compact_debug_snapshot!(
        env.metadata.materialization_input(&reconcile.token, 0).await.err(),
        @"Some(InvalidTaskResult)"
    );

    let mut second_result = common::empty_materialization_result(&second)?;
    second_result.index_delta = Some(second.token.attempt_id);
    env.metadata
        .complete_materialization(&second.token, second_result.clone())
        .await?;

    env.clock.set(46_000);
    let third = env
        .metadata
        .claim_task(worker)
        .await?
        .ok_or("missing third task")?;
    let expected = vec![
        first_delta,
        IndexDelta {
            id: second.token.attempt_id,
            range: second_result.range,
        },
    ];
    assert_eq!(
        env.metadata
            .materialization_input(&third.token, 0)
            .await?
            .deltas,
        expected
    );
    assert_eq!(
        env.metadata
            .materialization_input(&third.token, 2)
            .await?
            .deltas,
        expected[1..]
    );

    env.metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn index_delta_receipts_survive_restarts_and_fence_stale_attempts()
-> Result<(), Box<dyn std::error::Error>> {
    let mut env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);
    let worker = Uuid::from_u128(1);

    env.resolve(&catalog, table).await?;
    for schema in [3, 3, 4] {
        append_wal(&env.metadata, &catalog, table, schema, "wal").await?;
    }

    env.clock.set(16_000);
    let first = env
        .metadata
        .claim_task(worker)
        .await?
        .ok_or("missing first task")?;
    let mut first_result = common::empty_materialization_result(&first)?;
    first_result.index_delta = Some(first.token.attempt_id);
    env.metadata
        .complete_materialization(&first.token, first_result.clone())
        .await?;

    let expected = vec![IndexDelta {
        id: first.token.attempt_id,
        range: first_result.range,
    }];

    env.clock.set(31_000);
    let _reconcile = env
        .metadata
        .claim_task(Uuid::from_u128(2))
        .await?
        .ok_or("missing reconciliation")?;
    let second = env
        .metadata
        .claim_task(worker)
        .await?
        .ok_or("missing second task")?;

    // Retrying an old completion is a no-op; mismatches on the receipt are rejected.
    let before = serde_json::to_value(env.metadata.debug_table(&catalog, table).await?)?;
    env.metadata
        .complete_materialization(&first.token, first_result.clone())
        .await?;
    assert_eq!(
        serde_json::to_value(env.metadata.debug_table(&catalog, table).await?)?,
        before
    );

    let mut mismatch = first_result.clone();
    mismatch.index_delta = None;
    insta::assert_compact_debug_snapshot!(
        env.metadata
            .complete_materialization(&first.token, mismatch)
            .await
            .err(),
        @"Some(InvalidTaskResult)"
    );

    let mut wrong_token = first.token.clone();
    wrong_token.worker_id = Uuid::nil();
    insta::assert_compact_debug_snapshot!(
        env.metadata
            .complete_materialization(&wrong_token, first_result.clone())
            .await
            .err(),
        @"Some(InvalidTaskResult)"
    );

    // Accepted deltas and completion receipts survive restart; expired attempts are fenced.
    env.clock.set(61_000);
    env.reopen().await?;
    env.metadata
        .complete_materialization(&first.token, first_result)
        .await?;
    assert_eq!(
        env.metadata
            .debug_table(&catalog, table)
            .await?
            .index_deltas,
        expected
    );
    insta::assert_compact_debug_snapshot!(
        env.metadata.materialization_input(&second.token, 0).await.err(),
        @"Some(TaskFenced)"
    );

    // Recovering the expired task still sees prior deltas; completing without a delta is idempotent.
    let recovered = env
        .metadata
        .claim_task(worker)
        .await?
        .ok_or("missing recovered task")?;
    assert_eq!(
        env.metadata
            .materialization_input(&recovered.token, 0)
            .await?
            .deltas,
        expected
    );

    let append_result = common::empty_materialization_result(&recovered)?;
    env.metadata
        .complete_materialization(&recovered.token, append_result.clone())
        .await?;
    env.metadata
        .complete_materialization(&recovered.token, append_result)
        .await?;
    assert_eq!(
        env.metadata
            .debug_table(&catalog, table)
            .await?
            .index_deltas,
        expected
    );

    env.metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn completion_retry_waits_for_durable_acceptance() -> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        store,
        secrets,
        clock,
        server,
        metadata,
        ..
    } = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);

    common::resolve(&metadata, &server, &catalog, table).await?;
    append_wal(&metadata, &catalog, table, 0, "wal").await?;

    clock.set(16_000);
    let assignment = metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing task")?;
    metadata.close().await?;

    // Disable background flushing so a visible receipt is not necessarily durable yet.
    let db = Db::builder("metadata", store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    let metadata = Metadata::from_db(db, secrets.clone(), clock.clone());
    let mut result = common::empty_materialization_result(&assignment)?;
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

    let expected = vec![IndexDelta {
        id: assignment.token.attempt_id,
        range: result.range,
    }];
    let mut visible = false;
    for _ in 0..10_000 {
        if metadata.debug_table(&catalog, table).await?.index_deltas == expected {
            visible = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        visible && !completing.is_finished(),
        "fixture did not expose an acceptance awaiting durability"
    );

    // Retry must flush durability before returning Ok.
    metadata
        .complete_materialization(&assignment.token, result)
        .await?;

    // Snapshot raw object-store state before graceful close to simulate a crash.
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
    insta::assert_debug_snapshot!(
        recovered.wal_read_after(&catalog, table, None, 10).await?,
        @"[]"
    );

    completing.await??;
    recovered.close().await?;
    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn checkpoint_interval_batches_writes_and_survives_restarts()
-> Result<(), Box<dyn std::error::Error>> {
    for (value, seconds) in [(None, 60), (Some("15"), 15), (Some("900"), 900)] {
        let mut env = TestEnvironment::new().await?;
        let catalog = CatalogId::from(Uuid::from_u128(1));
        let table = Uuid::from_u128(42);
        let worker = Uuid::from_u128(1);
        let properties = value
            .map(|value: &str| HashMap::from([(CHECKPOINT_INTERVAL_PROPERTY.into(), value.into())]))
            .unwrap_or_default();

        env.resolve_with_properties(&catalog, table, properties)
            .await?;
        append_wal(&env.metadata, &catalog, table, 3, "first").await?;

        let first_due = 1_000 + seconds * 1_000;
        let task = env.metadata.claim_task(worker).await?;
        insta::allow_duplicates! {
            insta::assert_debug_snapshot!(task, @"None");
        }

        env.clock.set(first_due - 1);
        let task = env.metadata.claim_task(worker).await?;
        insta::allow_duplicates! {
            insta::assert_debug_snapshot!(task, @"None");
        }

        // More writes join the batch without pushing its deadline back, surviving restarts.
        append_wal(&env.metadata, &catalog, table, 3, "second").await?;
        env.reopen().await?;

        env.clock.set(first_due);
        let first = env
            .metadata
            .claim_task(worker)
            .await?
            .ok_or("missing first checkpoint")?;
        insta::allow_duplicates! {
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
        }

        // Writes arriving during materialization also wait for the next checkpoint interval.
        append_wal(&env.metadata, &catalog, table, 7, "third").await?;
        env.metadata
            .complete_materialization(&first.token, common::empty_materialization_result(&first)?)
            .await?;

        let second_due = first_due + seconds * 1_000;
        let task = env.metadata.claim_task(worker).await?;
        insta::allow_duplicates! {
            insta::assert_debug_snapshot!(task, @"None");
        }

        // Publication is independent of checkpoint pacing.
        env.clock.set(second_due - 1);
        let publication = env
            .metadata
            .claim_task(Uuid::from_u128(2))
            .await?
            .ok_or("missing publication")?;
        assert!(matches!(publication.task, Task::Reconcile { .. }));

        let task = common::claim_foreground(&env.metadata, worker).await?;
        insta::allow_duplicates! {
            insta::assert_debug_snapshot!(task, @"None");
        }

        env.clock.set(second_due);
        let second = common::claim_foreground(&env.metadata, worker)
            .await?
            .ok_or("missing second checkpoint")?;
        insta::allow_duplicates! {
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
        }

        env.metadata
            .complete_materialization(
                &second.token,
                common::empty_materialization_result(&second)?,
            )
            .await?;
        env.metadata.close().await?;
    }

    Ok(())
}

#[tokio::test]
async fn checkpoint_interval_is_validated_and_refreshed_on_resolve()
-> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);

    let mut invalid_errors = Vec::new();
    for value in [
        "-1",
        "901",
        "1.5",
        "",
        "invalid",
        "18446744073709551616",
        "18446744073709551615",
        "9223372036854775",
    ] {
        let error = env
            .resolve_with_properties(
                &catalog,
                table,
                HashMap::from([(CHECKPOINT_INTERVAL_PROPERTY.into(), value.into())]),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<Error>(),
            Some(Error::InvalidCheckpointInterval(_))
        ));
        invalid_errors.push(error.to_string());
    }

    insta::assert_debug_snapshot!(invalid_errors, @r#"
    [
        "invalid airfoil.checkpoint.interval-sec: \"-1\"; expected whole seconds between 0 and 900 inclusive",
        "invalid airfoil.checkpoint.interval-sec: \"901\"; expected whole seconds between 0 and 900 inclusive",
        "invalid airfoil.checkpoint.interval-sec: \"1.5\"; expected whole seconds between 0 and 900 inclusive",
        "invalid airfoil.checkpoint.interval-sec: \"\"; expected whole seconds between 0 and 900 inclusive",
        "invalid airfoil.checkpoint.interval-sec: \"invalid\"; expected whole seconds between 0 and 900 inclusive",
        "invalid airfoil.checkpoint.interval-sec: \"18446744073709551616\"; expected whole seconds between 0 and 900 inclusive",
        "invalid airfoil.checkpoint.interval-sec: \"18446744073709551615\"; expected whole seconds between 0 and 900 inclusive",
        "invalid airfoil.checkpoint.interval-sec: \"9223372036854775\"; expected whole seconds between 0 and 900 inclusive",
    ]
    "#);

    let mut refreshed = Vec::new();
    for value in [Some("15"), None, Some("900")] {
        let properties = value
            .map(|value: &str| HashMap::from([(CHECKPOINT_INTERVAL_PROPERTY.into(), value.into())]))
            .unwrap_or_default();
        env.resolve_with_properties(&catalog, table, properties)
            .await?;
        refreshed.push(
            env.metadata
                .debug_table(&catalog, table)
                .await?
                .state
                .checkpoint_interval_sec,
        );
    }

    insta::assert_compact_debug_snapshot!(refreshed, @"[15, 60, 900]");

    env.metadata.close().await?;

    Ok(())
}

async fn append_wal(
    metadata: &Metadata,
    catalog: &CatalogId,
    table: Uuid,
    schema_id: i32,
    location: &str,
) -> Result<(), Error> {
    metadata
        .wal_append(catalog, table, schema_id, location, DateTime::UNIX_EPOCH, 0)
        .await
}

async fn claim_materialization(
    metadata: &Metadata,
    worker: Uuid,
) -> Result<TaskAssignment, Box<dyn std::error::Error>> {
    let first = common::claim_foreground(metadata, Uuid::from_u128(98))
        .await?
        .ok_or("missing isolated task")?;
    if matches!(first.task, Task::Reconcile { .. }) {
        Ok(common::claim_foreground(metadata, worker)
            .await?
            .ok_or("missing isolated materialization")?)
    } else {
        Ok(first)
    }
}

fn sample_data_file(path: &str) -> DataFileMetadata {
    DataFileMetadata {
        path: path.into(),
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
    }
}

fn sample_deletion_vector(
    puffin_path: &str,
    target_file: &str,
    cardinality: u64,
) -> DeletionVectorMetadata {
    DeletionVectorMetadata {
        puffin_path: puffin_path.into(),
        target_file: target_file.into(),
        cardinality,
        file_size: 90,
        offset: 10,
        length: 30,
    }
}
