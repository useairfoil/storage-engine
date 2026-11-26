mod common;

use chrono::DateTime;
use common::TestEnvironment;
use se_metadata::CatalogId;
use uuid::Uuid;

#[tokio::test]
async fn table_status_rejects_unresolved_table() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);

    insta::assert_snapshot!(
        env.metadata.table_status(&catalog, table).await.err().unwrap(),
        @"table UUID has not been resolved: 00000000-0000-0000-0000-00000000002a"
    );

    env.metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn table_status_tracks_materialization_reconciliation_and_wal_cleanup()
-> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);

    env.resolve(&catalog, table).await?;

    insta::assert_yaml_snapshot!(env.metadata.table_status(&catalog, table).await?, @r#"
    tableUuid: 00000000-0000-0000-0000-00000000002a
    observedAt: "1970-01-01T00:00:01Z"
    conditions:
      - type: Materialized
        status: "True"
        reason: CaughtUp
        message: All pending changes have been processed.
      - type: Reconciled
        status: "True"
        reason: CaughtUp
        message: All pending changes have been processed.
      - type: Deleting
        status: "False"
        reason: NotRequested
        message: Table deletion has not been requested.
      - type: WalGarbageCollected
        status: "True"
        reason: CaughtUp
        message: All pending changes have been processed.
    wal:
      acceptedNextSequence: 0
      materializedNextSequence: 0
      pendingBatches: 0
      pendingBytes: 0
      oldestPendingAt: ~
      cleanupPendingBatches: 0
      cleanupPendingBytes: 0
      oldestCleanupPendingAt: ~
    iceberg:
      desiredRevision: 0
      reconciledRevision: 0
      reconciledSnapshotId: ~
    work:
      materialization:
        state: Idle
        eligibleAt: ~
        leaseExpiresAt: ~
      reconciliation:
        state: Idle
        eligibleAt: ~
        leaseExpiresAt: ~
      deletion:
        state: Idle
        eligibleAt: ~
        leaseExpiresAt: ~
      walCleanup:
        state: Idle
        eligibleAt: ~
        leaseExpiresAt: ~
    "#);

    // Appending batches moves materialization into Scheduled with summed bytes and oldest timestamp.
    let first_ts = DateTime::from_timestamp(1, 0).unwrap();
    let second_ts = DateTime::from_timestamp(2, 0).unwrap();
    env.metadata
        .wal_append(&catalog, table, 1, "wal/0", first_ts, 100)
        .await?;
    env.metadata
        .wal_append(&catalog, table, 1, "wal/1", second_ts, 250)
        .await?;

    env.clock.set(5_000);
    insta::assert_yaml_snapshot!(env.metadata.table_status(&catalog, table).await?, @r#"
    tableUuid: 00000000-0000-0000-0000-00000000002a
    observedAt: "1970-01-01T00:00:05Z"
    conditions:
      - type: Materialized
        status: "False"
        reason: Scheduled
        message: Pending changes are scheduled for processing.
      - type: Reconciled
        status: "True"
        reason: CaughtUp
        message: All pending changes have been processed.
      - type: Deleting
        status: "False"
        reason: NotRequested
        message: Table deletion has not been requested.
      - type: WalGarbageCollected
        status: "True"
        reason: CaughtUp
        message: All pending changes have been processed.
    wal:
      acceptedNextSequence: 2
      materializedNextSequence: 0
      pendingBatches: 2
      pendingBytes: 350
      oldestPendingAt: "1970-01-01T00:00:01Z"
      cleanupPendingBatches: 0
      cleanupPendingBytes: 0
      oldestCleanupPendingAt: ~
    iceberg:
      desiredRevision: 0
      reconciledRevision: 0
      reconciledSnapshotId: ~
    work:
      materialization:
        state: Scheduled
        eligibleAt: "1970-01-01T00:00:16Z"
        leaseExpiresAt: ~
      reconciliation:
        state: Idle
        eligibleAt: ~
        leaseExpiresAt: ~
      deletion:
        state: Idle
        eligibleAt: ~
        leaseExpiresAt: ~
      walCleanup:
        state: Idle
        eligibleAt: ~
        leaseExpiresAt: ~
    "#);

    // At eligibility time materialization transitions to Queued, then Assigned when claimed.
    env.clock.set(16_000);
    insta::assert_yaml_snapshot!(env.metadata.table_status(&catalog, table).await?.work.materialization, @r#"
    state: Queued
    eligibleAt: "1970-01-01T00:00:16Z"
    leaseExpiresAt: ~
    "#);

    let mat = env
        .metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing materialization")?;
    insta::assert_yaml_snapshot!(env.metadata.table_status(&catalog, table).await?.work.materialization, @r#"
    state: Assigned
    eligibleAt: "1970-01-01T00:00:46Z"
    leaseExpiresAt: "1970-01-01T00:00:46Z"
    "#);

    // Completing materialization catches up WAL and schedules reconciliation and WAL cleanup.
    common::complete_empty_materialization(&env.metadata, &mat).await?;

    insta::assert_yaml_snapshot!(env.metadata.table_status(&catalog, table).await?, @r#"
    tableUuid: 00000000-0000-0000-0000-00000000002a
    observedAt: "1970-01-01T00:00:16Z"
    conditions:
      - type: Materialized
        status: "True"
        reason: CaughtUp
        message: All pending changes have been processed.
      - type: Reconciled
        status: "False"
        reason: Scheduled
        message: Pending changes are scheduled for processing.
      - type: Deleting
        status: "False"
        reason: NotRequested
        message: Table deletion has not been requested.
      - type: WalGarbageCollected
        status: "False"
        reason: Scheduled
        message: Pending changes are scheduled for processing.
    wal:
      acceptedNextSequence: 2
      materializedNextSequence: 2
      pendingBatches: 0
      pendingBytes: 0
      oldestPendingAt: ~
      cleanupPendingBatches: 2
      cleanupPendingBytes: 350
      oldestCleanupPendingAt: "1970-01-01T00:00:16Z"
    iceberg:
      desiredRevision: 1
      reconciledRevision: 0
      reconciledSnapshotId: ~
    work:
      materialization:
        state: Idle
        eligibleAt: ~
        leaseExpiresAt: ~
      reconciliation:
        state: Scheduled
        eligibleAt: "1970-01-01T00:00:21Z"
        leaseExpiresAt: ~
      deletion:
        state: Idle
        eligibleAt: ~
        leaseExpiresAt: ~
      walCleanup:
        state: Scheduled
        eligibleAt: "1970-01-01T00:01:16Z"
        leaseExpiresAt: ~
    "#);

    env.metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn table_status_tracks_deletion_lifecycle_states() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);

    env.resolve(&catalog, table).await?;
    env.metadata
        .wal_append(&catalog, table, 1, "wal/0", DateTime::UNIX_EPOCH, 40)
        .await?;

    // Hold an active materialization lease (expires at 46_000) so deletion starts Scheduled.
    env.clock.set(16_000);
    let _mat = env
        .metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing materialization")?;

    env.clock.set(20_000);
    env.metadata.delete_table(&catalog, table).await?;

    insta::assert_yaml_snapshot!(env.metadata.table_status(&catalog, table).await?, @r#"
    tableUuid: 00000000-0000-0000-0000-00000000002a
    observedAt: "1970-01-01T00:00:20Z"
    conditions:
      - type: Materialized
        status: "False"
        reason: Deleting
        message: Pending changes will not be processed because the table is being deleted.
      - type: Reconciled
        status: "True"
        reason: CaughtUp
        message: All pending changes have been processed.
      - type: Deleting
        status: "True"
        reason: Scheduled
        message: Table cleanup is scheduled for a later time.
      - type: WalGarbageCollected
        status: "True"
        reason: CaughtUp
        message: All pending changes have been processed.
    wal:
      acceptedNextSequence: 1
      materializedNextSequence: 0
      pendingBatches: 1
      pendingBytes: 40
      oldestPendingAt: "1970-01-01T00:00:00Z"
      cleanupPendingBatches: 0
      cleanupPendingBytes: 0
      oldestCleanupPendingAt: ~
    iceberg:
      desiredRevision: 0
      reconciledRevision: 0
      reconciledSnapshotId: ~
    work:
      materialization:
        state: Idle
        eligibleAt: ~
        leaseExpiresAt: ~
      reconciliation:
        state: Idle
        eligibleAt: ~
        leaseExpiresAt: ~
      deletion:
        state: Scheduled
        eligibleAt: "1970-01-01T00:00:46Z"
        leaseExpiresAt: ~
      walCleanup:
        state: Idle
        eligibleAt: ~
        leaseExpiresAt: ~
    "#);

    // At 46_000 deletion becomes Queued, and once claimed becomes Assigned (CleanupRunning).
    env.clock.set(46_000);
    let queued = env.metadata.table_status(&catalog, table).await?;
    insta::assert_yaml_snapshot!((&queued.conditions[2], &queued.work.deletion), @r#"
    - type: Deleting
      status: "True"
      reason: DeletionRequested
      message: Table cleanup is queued.
    - state: Queued
      eligibleAt: "1970-01-01T00:00:46Z"
      leaseExpiresAt: ~
    "#);

    let _delete = env
        .metadata
        .claim_task(Uuid::from_u128(2))
        .await?
        .ok_or("missing delete task")?;
    let assigned = env.metadata.table_status(&catalog, table).await?;
    insta::assert_yaml_snapshot!((&assigned.conditions[2], &assigned.work.deletion), @r#"
    - type: Deleting
      status: "True"
      reason: CleanupRunning
      message: Table cleanup is assigned to a worker.
    - state: Assigned
      eligibleAt: "1970-01-01T00:01:16Z"
      leaseExpiresAt: "1970-01-01T00:01:16Z"
    "#);

    env.metadata.close().await?;

    Ok(())
}
