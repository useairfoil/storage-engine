mod common;

use chrono::DateTime;
use common::TestEnvironment;
use se_metadata::CatalogId;
use uuid::Uuid;

#[tokio::test]
async fn delete_table_cancels_work_waits_for_active_leases_and_wipes_records()
-> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);
    let materializer = Uuid::from_u128(1);
    let reconciler = Uuid::from_u128(2);
    let deleter = Uuid::from_u128(3);

    let ident = env.resolve(&catalog, table).await?;
    env.metadata
        .wal_append(&catalog, table, 1, "wal/0", DateTime::UNIX_EPOCH, 10)
        .await?;
    env.metadata
        .wal_append(&catalog, table, 2, "wal/1", DateTime::UNIX_EPOCH, 20)
        .await?;

    // Complete first materialization so reconciliation and WAL cleanup are scheduled.
    env.clock.set(16_000);
    let first_mat = env
        .metadata
        .claim_task(materializer)
        .await?
        .ok_or("missing first materialization")?;
    common::complete_empty_materialization(&env.metadata, &first_mat).await?;

    // Claim reconciliation at 21_000 (expires 51_000) and second materialization at 31_000 (expires 61_000).
    env.clock.set(21_000);
    let reconcile = env
        .metadata
        .claim_task(reconciler)
        .await?
        .ok_or("missing reconciliation")?;

    env.clock.set(31_000);
    let second_mat = env
        .metadata
        .claim_task(materializer)
        .await?
        .ok_or("missing second materialization")?;

    // Delete table at 35_000: must wait until max active lease expiration (61_000).
    env.clock.set(35_000);
    env.metadata.delete_table(&catalog, table).await?;
    env.metadata.delete_table(&catalog, table).await?;

    // Resolving or appending while deleting is rejected.
    insta::assert_snapshot!(
        env.metadata.resolve_table(&catalog, &ident).await.unwrap_err(),
        @"table is being deleted: 00000000-0000-0000-0000-00000000002a"
    );
    insta::assert_snapshot!(
        env.metadata
            .wal_append(&catalog, table, 2, "wal/2", DateTime::UNIX_EPOCH, 30)
            .await
            .unwrap_err(),
        @"table is being deleted: 00000000-0000-0000-0000-00000000002a"
    );

    // Previously active workers are fenced and released.
    insta::assert_compact_debug_snapshot!(
        env.metadata.heartbeat_task(&reconcile.token).await.err(),
        @"Some(TaskFenced)"
    );
    insta::assert_compact_debug_snapshot!(
        env.metadata.heartbeat_task(&second_mat.token).await.err(),
        @"Some(TaskFenced)"
    );
    insta::assert_debug_snapshot!(env.metadata.claim_task(materializer).await?, @"None");
    insta::assert_debug_snapshot!(env.metadata.claim_task(reconciler).await?, @"None");

    // DeleteTable becomes eligible only once the longest in-flight lease expires at 61_000.
    env.clock.set(60_999);
    insta::assert_debug_snapshot!(env.metadata.claim_task(deleter).await?, @"None");

    env.clock.set(61_000);
    let delete_task = env
        .metadata
        .claim_task(deleter)
        .await?
        .ok_or("missing delete task")?;
    insta::assert_debug_snapshot!(delete_task.task, @"DeleteTable");

    env.metadata
        .complete_delete_table(&delete_task.token)
        .await?;
    // Retrying completion after deletion is idempotent.
    env.metadata
        .complete_delete_table(&delete_task.token)
        .await?;

    insta::assert_snapshot!(
        env.metadata.debug_table(&catalog, table).await.err().unwrap(),
        @"table UUID has not been resolved: 00000000-0000-0000-0000-00000000002a"
    );
    insta::assert_debug_snapshot!(
        env.metadata.wal_read_after(&catalog, table, None, 10).await?,
        @"[]"
    );
    insta::assert_debug_snapshot!(env.metadata.claim_task(deleter).await?, @"None");

    // Re-resolving the same table starts with clean state.
    env.resolve(&catalog, table).await?;
    let fresh = env.metadata.debug_table(&catalog, table).await?;
    insta::assert_compact_debug_snapshot!(
        (
            fresh.next_sequence,
            fresh.materializer_epoch,
            fresh.state.desired_revision,
            fresh.state.reconciled_revision,
        ),
        @"(0, 0, 0, 0)"
    );

    env.metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn delete_table_rejects_unresolved_table_and_fences_invalid_completions()
-> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);

    insta::assert_snapshot!(
        env.metadata.delete_table(&catalog, table).await.unwrap_err(),
        @"table UUID has not been resolved: 00000000-0000-0000-0000-00000000002a"
    );

    env.resolve(&catalog, table).await?;
    env.metadata
        .wal_append(&catalog, table, 1, "wal/0", DateTime::UNIX_EPOCH, 10)
        .await?;

    // Wrong task kind is rejected by complete_delete_table.
    env.clock.set(16_000);
    let mat = env
        .metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing materialization")?;
    insta::assert_compact_debug_snapshot!(
        env.metadata.complete_delete_table(&mat.token).await.err(),
        @"Some(InvalidTaskResult)"
    );

    env.metadata.delete_table(&catalog, table).await?;

    // Claim DeleteTable once the materialization lease expires, then expire the delete lease.
    env.clock.set(46_000);
    let first_delete = env
        .metadata
        .claim_task(Uuid::from_u128(2))
        .await?
        .ok_or("missing delete task")?;

    env.clock.set(76_000);
    insta::assert_compact_debug_snapshot!(
        env.metadata
            .complete_delete_table(&first_delete.token)
            .await
            .err(),
        @"Some(TaskFenced)"
    );

    let recovered = env
        .metadata
        .claim_task(Uuid::from_u128(3))
        .await?
        .ok_or("missing recovered delete task")?;
    assert_ne!(recovered.token.attempt_id, first_delete.token.attempt_id);

    env.metadata.complete_delete_table(&recovered.token).await?;
    insta::assert_snapshot!(
        env.metadata.debug_table(&catalog, table).await.err().unwrap(),
        @"table UUID has not been resolved: 00000000-0000-0000-0000-00000000002a"
    );

    env.metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn complete_delete_table_isolates_neighbor_tables_and_catalogs()
-> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let other_catalog = CatalogId::from(Uuid::from_u128(2));
    let target = Uuid::from_u128(42);
    let sibling = Uuid::from_u128(43);

    for (cat, tbl, loc) in [
        (&catalog, target, "wal/target"),
        (&catalog, sibling, "wal/sibling"),
        (&other_catalog, target, "wal/other-catalog"),
    ] {
        env.resolve(cat, tbl).await?;
        env.metadata
            .wal_append(cat, tbl, 1, loc, DateTime::UNIX_EPOCH, 10)
            .await?;
    }

    env.metadata.delete_table(&catalog, target).await?;
    let delete_task = env
        .metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing delete task")?;
    assert_eq!(delete_task.token.table_uuid, target);
    assert_eq!(delete_task.token.catalog_id, catalog);
    env.metadata
        .complete_delete_table(&delete_task.token)
        .await?;

    insta::assert_snapshot!(
        env.metadata.debug_table(&catalog, target).await.err().unwrap(),
        @"table UUID has not been resolved: 00000000-0000-0000-0000-00000000002a"
    );
    insta::assert_debug_snapshot!(
        env.metadata.wal_read_after(&catalog, sibling, None, 10).await?,
        @r#"
    [
        BatchDescriptor {
            sequence: 0,
            metadata: BatchMetadata {
                schema_id: 1,
                location: "wal/sibling",
                timestamp: 1970-01-01T00:00:00Z,
                wal_size_bytes: 10,
            },
        },
    ]
    "#
    );
    insta::assert_debug_snapshot!(
        env.metadata.wal_read_after(&other_catalog, target, None, 10).await?,
        @r#"
    [
        BatchDescriptor {
            sequence: 0,
            metadata: BatchMetadata {
                schema_id: 1,
                location: "wal/other-catalog",
                timestamp: 1970-01-01T00:00:00Z,
                wal_size_bytes: 10,
            },
        },
    ]
    "#
    );

    env.metadata.close().await?;

    Ok(())
}
