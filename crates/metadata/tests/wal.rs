mod common;

use chrono::DateTime;
use common::TestEnvironment;
use se_metadata::{CatalogId, Task};
use uuid::Uuid;

#[tokio::test]
async fn concurrent_wal_appends_assign_contiguous_sequences_and_preserve_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        metadata,
        server,
        clock,
        ..
    } = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_bytes([17; 16]);
    let timestamp = DateTime::from_timestamp(-2, 16_909_060).ok_or("invalid time")?;

    common::resolve(&metadata, &server, &catalog, table).await?;

    futures::future::try_join_all((0..16).map(|i| {
        let metadata = &metadata;
        let catalog = &catalog;

        async move {
            metadata
                .wal_append(catalog, table, -17, &format!("wal/{i}"), timestamp, 1234)
                .await
        }
    }))
    .await?;

    let batches = metadata.wal_read_after(&catalog, table, None, 32).await?;
    let sequences: Vec<u64> = batches.iter().map(|batch| batch.sequence).collect();
    insta::assert_compact_debug_snapshot!(
        sequences,
        @"[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]"
    );

    for batch in &batches {
        assert_eq!(batch.metadata.schema_id, -17);
        assert_eq!(batch.metadata.timestamp, timestamp);
        assert_eq!(batch.metadata.wal_size_bytes, 1234);
    }

    let mut locations: Vec<String> = batches
        .into_iter()
        .map(|batch| batch.metadata.location)
        .collect();
    locations.sort();
    insta::assert_debug_snapshot!(locations, @r#"
    [
        "wal/0",
        "wal/1",
        "wal/10",
        "wal/11",
        "wal/12",
        "wal/13",
        "wal/14",
        "wal/15",
        "wal/2",
        "wal/3",
        "wal/4",
        "wal/5",
        "wal/6",
        "wal/7",
        "wal/8",
        "wal/9",
    ]
    "#);

    // Appending after claiming a materialization preserves the reader's epoch.
    clock.set(16_000);
    let _task = metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing materialization task")?;
    let epoch_before = metadata
        .debug_table(&catalog, table)
        .await?
        .materializer_epoch;
    insta::assert_debug_snapshot!(epoch_before, @"1");

    metadata
        .wal_append(&catalog, table, 5, "new/\0\x01é", timestamp, 5678)
        .await?;

    let debug = metadata.debug_table(&catalog, table).await?;
    insta::assert_debug_snapshot!(debug.next_sequence, @"17");
    assert_eq!(debug.materializer_epoch, epoch_before);

    let tail = metadata
        .wal_read_after(&catalog, table, Some(15), 2)
        .await?;
    insta::assert_debug_snapshot!(tail, @r#"
    [
        BatchDescriptor {
            sequence: 16,
            metadata: BatchMetadata {
                schema_id: 5,
                location: "new/\0\u{1}é",
                timestamp: 1969-12-31T23:59:58.016909060Z,
                wal_size_bytes: 5678,
            },
        },
    ]
    "#);

    // Respects the read limit.
    let limited = metadata.wal_read_after(&catalog, table, None, 3).await?;
    insta::assert_compact_debug_snapshot!(
        limited.iter().map(|batch| batch.sequence).collect::<Vec<_>>(),
        @"[0, 1, 2]"
    );

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn wal_appends_are_isolated_across_catalogs_and_tables()
-> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        metadata, server, ..
    } = TestEnvironment::new().await?;

    let catalog = CatalogId::from(Uuid::from_u128(1));
    let other_catalog = CatalogId::from(Uuid::from_u128(2));
    let table = Uuid::from_bytes([17; 16]);
    let other_table = Uuid::from_bytes([18; 16]);

    common::resolve(&metadata, &server, &catalog, table).await?;
    common::resolve(&metadata, &server, &other_catalog, table).await?;
    common::resolve(&metadata, &server, &catalog, other_table).await?;

    metadata
        .wal_append(&catalog, table, 1, "primary", DateTime::UNIX_EPOCH, 10)
        .await?;
    metadata
        .wal_append(
            &other_catalog,
            table,
            2,
            "other-catalog",
            DateTime::UNIX_EPOCH,
            20,
        )
        .await?;
    metadata
        .wal_append(
            &catalog,
            other_table,
            3,
            "other-table",
            DateTime::UNIX_EPOCH,
            30,
        )
        .await?;

    insta::assert_debug_snapshot!(metadata.wal_read_after(&catalog, table, None, 10).await?, @r#"
    [
        BatchDescriptor {
            sequence: 0,
            metadata: BatchMetadata {
                schema_id: 1,
                location: "primary",
                timestamp: 1970-01-01T00:00:00Z,
                wal_size_bytes: 10,
            },
        },
    ]
    "#);

    insta::assert_debug_snapshot!(
        metadata.wal_read_after(&other_catalog, table, None, 10).await?,
        @r#"
    [
        BatchDescriptor {
            sequence: 0,
            metadata: BatchMetadata {
                schema_id: 2,
                location: "other-catalog",
                timestamp: 1970-01-01T00:00:00Z,
                wal_size_bytes: 20,
            },
        },
    ]
    "#
    );

    insta::assert_debug_snapshot!(
        metadata.wal_read_after(&catalog, other_table, None, 10).await?,
        @r#"
    [
        BatchDescriptor {
            sequence: 0,
            metadata: BatchMetadata {
                schema_id: 3,
                location: "other-table",
                timestamp: 1970-01-01T00:00:00Z,
                wal_size_bytes: 30,
            },
        },
    ]
    "#
    );

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn wal_append_rejects_unresolved_table() -> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment { metadata, .. } = TestEnvironment::new().await?;

    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(99);

    let error = metadata
        .wal_append(&catalog, table, 1, "wal/0", DateTime::UNIX_EPOCH, 0)
        .await
        .unwrap_err();
    insta::assert_snapshot!(
        error,
        @"table UUID has not been resolved: 00000000-0000-0000-0000-000000000063"
    );

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn wal_append_rejects_corrupted_or_exhausted_wal_state()
-> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        metadata,
        db,
        server,
        ..
    } = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_bytes([17; 16]);
    let state_key = [
        b"\x01".as_slice(),
        catalog.as_uuid().as_bytes(),
        table.as_bytes(),
        b"\x01\x01",
    ]
    .concat();

    common::resolve(&metadata, &server, &catalog, table).await?;

    let mut errors = Vec::new();
    for raw_state in [
        vec![0],
        [u64::MAX.to_le_bytes(), 7_u64.to_le_bytes()].concat(),
    ] {
        db.put(&state_key, raw_state).await?;
        let error = metadata
            .wal_append(&catalog, table, 9, "rejected", DateTime::UNIX_EPOCH, 0)
            .await
            .unwrap_err();
        errors.push(error.to_string());
        assert!(
            metadata
                .wal_read_after(&catalog, table, None, 10)
                .await?
                .is_empty()
        );
    }

    insta::assert_debug_snapshot!(errors, @r#"
    [
        "invalid metadata encoding: truncated value",
        "WAL sequence numbers exhausted",
    ]
    "#);

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn wal_read_after_paginates_and_handles_boundary_inputs()
-> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        metadata, server, ..
    } = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);

    // Unresolved and empty tables return no batches rather than an error.
    insta::assert_debug_snapshot!(metadata.wal_read_after(&catalog, table, None, 10).await?, @"[]");

    common::resolve(&metadata, &server, &catalog, table).await?;
    insta::assert_debug_snapshot!(metadata.wal_read_after(&catalog, table, None, 10).await?, @"[]");

    for i in 0..5 {
        metadata
            .wal_append(
                &catalog,
                table,
                1,
                &format!("wal/{i}"),
                DateTime::UNIX_EPOCH,
                10,
            )
            .await?;
    }

    insta::assert_debug_snapshot!(metadata.wal_read_after(&catalog, table, None, 0).await?, @"[]");
    insta::assert_debug_snapshot!(
        metadata.wal_read_after(&catalog, table, Some(u64::MAX), 10).await?,
        @"[]"
    );
    insta::assert_debug_snapshot!(metadata.wal_read_after(&catalog, table, Some(4), 10).await?, @"[]");
    insta::assert_debug_snapshot!(metadata.wal_read_after(&catalog, table, Some(100), 10).await?, @"[]");

    let mut cursor = None;
    let mut pages = Vec::new();
    loop {
        let page = metadata.wal_read_after(&catalog, table, cursor, 2).await?;
        if page.is_empty() {
            break;
        }
        cursor = page.last().map(|batch| batch.sequence);
        pages.push(
            page.into_iter()
                .map(|batch| batch.sequence)
                .collect::<Vec<_>>(),
        );
    }

    insta::assert_compact_debug_snapshot!(pages, @"[[0, 1], [2, 3], [4]]");

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn wal_append_rejects_deleting_table() -> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        metadata, server, ..
    } = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(99);

    common::resolve(&metadata, &server, &catalog, table).await?;
    metadata
        .wal_append(&catalog, table, 1, "wal/0", DateTime::UNIX_EPOCH, 10)
        .await?;

    metadata.delete_table(&catalog, table).await?;

    let error = metadata
        .wal_append(&catalog, table, 1, "wal/1", DateTime::UNIX_EPOCH, 20)
        .await
        .unwrap_err();
    insta::assert_snapshot!(
        error,
        @"table is being deleted: 00000000-0000-0000-0000-000000000063"
    );

    let remaining = metadata.wal_read_after(&catalog, table, None, 10).await?;
    insta::assert_compact_debug_snapshot!(
        remaining.iter().map(|batch| batch.sequence).collect::<Vec<_>>(),
        @"[0]"
    );

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn wal_cleanup_drains_materialized_batches_reschedules_and_is_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        metadata,
        server,
        clock,
        ..
    } = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);
    let materializer = Uuid::from_u128(1);
    let reconciler = Uuid::from_u128(2);
    let cleaner = Uuid::from_u128(3);

    common::resolve(&metadata, &server, &catalog, table).await?;

    for (schema, location) in [(1, "wal/0"), (1, "wal/1"), (2, "wal/2")] {
        metadata
            .wal_append(&catalog, table, schema, location, DateTime::UNIX_EPOCH, 10)
            .await?;
    }

    // Materialize the first two batches (schema 1) at t = 16_000.
    clock.set(16_000);
    let first_mat = metadata
        .claim_task(materializer)
        .await?
        .ok_or("missing first materialization")?;
    common::complete_empty_materialization(&metadata, &first_mat).await?;

    // Hold reconciliation and confirm WAL cleanup respects its 60s grace delay.
    clock.set(21_000);
    let reconcile = metadata
        .claim_task(reconciler)
        .await?
        .ok_or("missing reconciliation")?;
    assert!(matches!(reconcile.task, Task::Reconcile { .. }));

    // Hold the second materialization on the materializer worker so only cleanup can be claimed.
    clock.set(31_000);
    let second_mat = metadata
        .claim_task(materializer)
        .await?
        .ok_or("missing second materialization")?;
    assert!(matches!(second_mat.task, Task::Materialize { .. }));

    // Keep active leases alive across the 60s WAL cleanup delay.
    clock.set(50_000);
    metadata.heartbeat_task(&reconcile.token).await?;
    metadata.heartbeat_task(&second_mat.token).await?;

    clock.set(75_999);
    metadata.heartbeat_task(&reconcile.token).await?;
    metadata.heartbeat_task(&second_mat.token).await?;
    insta::assert_debug_snapshot!(metadata.claim_task(cleaner).await?, @"None");

    clock.set(76_000);
    let first_cleanup = metadata
        .claim_task(cleaner)
        .await?
        .ok_or("missing first wal cleanup")?;
    insta::assert_debug_snapshot!(first_cleanup.task, @r#"
    GarbageCollectWal {
        batches: [
            WalDeletion {
                sequence: 0,
                metadata: BatchMetadata {
                    schema_id: 1,
                    location: "wal/0",
                    timestamp: 1970-01-01T00:00:00Z,
                    wal_size_bytes: 10,
                },
                queued_at: 1970-01-01T00:00:16Z,
            },
            WalDeletion {
                sequence: 1,
                metadata: BatchMetadata {
                    schema_id: 1,
                    location: "wal/1",
                    timestamp: 1970-01-01T00:00:00Z,
                    wal_size_bytes: 10,
                },
                queued_at: 1970-01-01T00:00:16Z,
            },
        ],
    }
    "#);

    // Queue another WAL deletion while the first cleanup is still in flight.
    common::complete_empty_materialization(&metadata, &second_mat).await?;

    metadata.complete_wal_cleanup(&first_cleanup.token).await?;

    // Retrying with the exact token is idempotent; a mismatched worker token is rejected.
    metadata.complete_wal_cleanup(&first_cleanup.token).await?;
    let mut forged = first_cleanup.token.clone();
    forged.worker_id = Uuid::from_u128(99);
    insta::assert_compact_debug_snapshot!(
        metadata.complete_wal_cleanup(&forged).await.err(),
        @"Some(InvalidTaskResult)"
    );

    // The remaining deletion is immediately rescheduled for cleanup.
    let second_cleanup = metadata
        .claim_task(cleaner)
        .await?
        .ok_or("missing rescheduled wal cleanup")?;
    insta::assert_debug_snapshot!(second_cleanup.task, @r#"
    GarbageCollectWal {
        batches: [
            WalDeletion {
                sequence: 2,
                metadata: BatchMetadata {
                    schema_id: 2,
                    location: "wal/2",
                    timestamp: 1970-01-01T00:00:00Z,
                    wal_size_bytes: 10,
                },
                queued_at: 1970-01-01T00:01:16Z,
            },
        ],
    }
    "#);

    metadata.complete_wal_cleanup(&second_cleanup.token).await?;
    insta::assert_debug_snapshot!(
        metadata.debug_table(&catalog, table).await?.wal_deletions,
        @"[]"
    );
    insta::assert_debug_snapshot!(metadata.claim_task(cleaner).await?, @"None");

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn wal_cleanup_fences_expired_and_wrong_kind_tokens() -> Result<(), Box<dyn std::error::Error>>
{
    let TestEnvironment {
        metadata,
        server,
        clock,
        ..
    } = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);

    common::resolve(&metadata, &server, &catalog, table).await?;
    metadata
        .wal_append(&catalog, table, 1, "wal/0", DateTime::UNIX_EPOCH, 10)
        .await?;

    clock.set(16_000);
    let mat = metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing materialization")?;
    insta::assert_compact_debug_snapshot!(
        metadata.complete_wal_cleanup(&mat.token).await.err(),
        @"Some(InvalidTaskResult)"
    );
    common::complete_empty_materialization(&metadata, &mat).await?;

    clock.set(21_000);
    let reconcile = metadata
        .claim_task(Uuid::from_u128(2))
        .await?
        .ok_or("missing reconciliation")?;

    clock.set(50_000);
    metadata.heartbeat_task(&reconcile.token).await?;

    clock.set(76_000);
    let expired_cleanup = metadata
        .claim_task(Uuid::from_u128(3))
        .await?
        .ok_or("missing wal cleanup")?;

    // Renew reconciliation before advancing past cleanup's 30s lease expiration (106_000).
    clock.set(79_000);
    metadata.heartbeat_task(&reconcile.token).await?;

    clock.set(106_000);
    insta::assert_compact_debug_snapshot!(
        metadata.complete_wal_cleanup(&expired_cleanup.token).await.err(),
        @"Some(TaskFenced)"
    );

    // Another worker recovers the expired cleanup and drains the queue.
    let recovered = metadata
        .claim_task(Uuid::from_u128(4))
        .await?
        .ok_or("missing recovered wal cleanup")?;
    assert_ne!(recovered.token.attempt_id, expired_cleanup.token.attempt_id);
    metadata.complete_wal_cleanup(&recovered.token).await?;

    insta::assert_debug_snapshot!(
        metadata.debug_table(&catalog, table).await?.wal_deletions,
        @"[]"
    );

    metadata.close().await?;

    Ok(())
}
