use std::sync::Arc;

use chrono::DateTime;
use futures::future::try_join_all;
use object_store::memory::InMemory;
use se_common::clock::MockSystemClock;
use se_metadata::{CatalogId, Error, MaterializationResult, Metadata, Task, TaskAssignment};
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

    for schema in [3, 3, 7] {
        metadata
            .wal_append(&catalog, table, schema, "wal", DateTime::UNIX_EPOCH)
            .await?;
    }

    let first = metadata
        .claim_task(worker)
        .await?
        .ok_or("missing first task")?;

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

    let first_result = new_result_from_assignment(&first)?;

    metadata
        .complete_materialization(&first.token, first_result.clone())
        .await?;

    assert_eq!(
        metadata
            .wal_read_after(&catalog, table, None, 10)
            .await?
            .iter()
            .map(|batch| batch.sequence)
            .collect::<Vec<_>>(),
        vec![2]
    );

    // Completion releases the worker for the next schema's WAL range.
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

    tokio::try_join!(
        metadata.wal_append(
            &catalog,
            table,
            7,
            "arrived-during-task",
            DateTime::UNIX_EPOCH,
        ),
        metadata.complete_materialization(&second.token, new_result_from_assignment(&second)?)
    )?;

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
    insta::assert_compact_debug_snapshot!(metadata.complete_materialization(&first.token, first_result).await.err(), @"Some(TaskFenced)");

    assert_eq!(metadata.claim_task(worker).await?, Some(third.clone()));

    metadata.close().await?;

    let metadata = Metadata::from_db(Db::open("metadata", store).await?, secrets, clock);

    assert_eq!(metadata.claim_task(worker).await?, Some(third.clone()));

    metadata
        .complete_materialization(&third.token, new_result_from_assignment(&third)?)
        .await?;

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

    metadata
        .wal_append(&catalog, table, 3, "wal", DateTime::UNIX_EPOCH)
        .await?;

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

    clock.set(30_999);

    let renewed = metadata.heartbeat_task(&first.token).await?;

    assert_eq!(renewed.token, first.token);

    clock.set(31_000);

    insta::assert_debug_snapshot!(metadata.claim_task(Uuid::from_u128(2)).await?, @"None");

    clock.set(60_999);

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

    clock.set(65_998);

    insta::assert_debug_snapshot!(metadata.claim_task(Uuid::from_u128(1)).await?, @"None");

    clock.set(65_999);

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
    clock.set(2000);

    metadata
        .wal_append(
            &catalog,
            Uuid::from_u128(44),
            1,
            "new-table",
            DateTime::UNIX_EPOCH,
        )
        .await?;

    clock.set(31_000);

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
    let metadata = Metadata::from_db(
        Db::open("metadata", Arc::new(InMemory::new())).await?,
        Arc::new(InMemorySecretStore::new()),
        Arc::new(MockSystemClock::with_time(1000)),
    );

    let catalog = CatalogId::new("catalog")?;
    let table = Uuid::from_u128(42);

    metadata
        .wal_append(&catalog, table, 3, "wal", DateTime::UNIX_EPOCH)
        .await?;

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
    insta::assert_compact_debug_snapshot!(metadata.complete_materialization(&task.token, altered).await.err(), @"Some(TaskFenced)");

    insta::assert_debug_snapshot!(metadata.claim_task(Uuid::from_u128(1)).await?, @"None");

    metadata.close().await?;

    Ok(())
}

fn new_result_from_assignment(assignment: &TaskAssignment) -> Result<MaterializationResult, Error> {
    let Task::Materialize {
        range, schema_id, ..
    } = assignment.task;
    Ok(MaterializationResult {
        range,
        schema_id,
        data_files: Vec::new(),
        deletion_vectors: Vec::new(),
    })
}
