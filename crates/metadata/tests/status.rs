mod common;

use chrono::DateTime;
use common::TestEnvironment;
use se_metadata::{CatalogId, TaskFailureCode};
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
async fn table_status_tracks_materialization_reconciliation_and_wal_gc()
-> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);

    env.resolve(&catalog, table).await?;

    insta::assert_yaml_snapshot!(env.metadata.table_status(&catalog, table).await?, @r#"
    table_uuid: 00000000-0000-0000-0000-00000000002a
    table_ident:
      namespace:
        - test
      name: 00000000-0000-0000-0000-00000000002a
    checkpoint_interval_sec: 15
    observed_at: "1970-01-01T00:00:01Z"
    lifecycle: active
    health: healthy
    ingestion:
      wal:
        accepted_next_sequence: 0
        materialized_next_sequence: 0
        pending_batches: 0
        pending_bytes: 0
        oldest_pending_at: ~
      iceberg:
        desired_revision: 0
        reconciled_revision: 0
        reconciled_snapshot_id: ~
      materialization:
        state: idle
        eligible_at: ~
        lease_expires_at: ~
        failure: ~
      reconciliation:
        state: idle
        eligible_at: ~
        lease_expires_at: ~
        failure: ~
    maintenance:
      wal_gc:
        state: idle
        eligible_at: ~
        lease_expires_at: ~
        failure: ~
        pending_batches: 0
        pending_bytes: 0
        oldest_pending_at: ~
      deletion:
        state: idle
        eligible_at: ~
        lease_expires_at: ~
        failure: ~
    "#);

    // Appending batches moves materialization into scheduled with summed bytes and oldest timestamp.
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
    table_uuid: 00000000-0000-0000-0000-00000000002a
    table_ident:
      namespace:
        - test
      name: 00000000-0000-0000-0000-00000000002a
    checkpoint_interval_sec: 15
    observed_at: "1970-01-01T00:00:05Z"
    lifecycle: active
    health: healthy
    ingestion:
      wal:
        accepted_next_sequence: 2
        materialized_next_sequence: 0
        pending_batches: 2
        pending_bytes: 350
        oldest_pending_at: "1970-01-01T00:00:01Z"
      iceberg:
        desired_revision: 0
        reconciled_revision: 0
        reconciled_snapshot_id: ~
      materialization:
        state: scheduled
        eligible_at: "1970-01-01T00:00:16Z"
        lease_expires_at: ~
        failure: ~
      reconciliation:
        state: idle
        eligible_at: ~
        lease_expires_at: ~
        failure: ~
    maintenance:
      wal_gc:
        state: idle
        eligible_at: ~
        lease_expires_at: ~
        failure: ~
        pending_batches: 0
        pending_bytes: 0
        oldest_pending_at: ~
      deletion:
        state: idle
        eligible_at: ~
        lease_expires_at: ~
        failure: ~
    "#);

    // At eligibility time materialization transitions to queued, then running when claimed.
    env.clock.set(16_000);
    insta::assert_yaml_snapshot!(env.metadata.table_status(&catalog, table).await?.ingestion.materialization, @r#"
    state: queued
    eligible_at: "1970-01-01T00:00:16Z"
    lease_expires_at: ~
    failure: ~
    "#);

    let mat = env
        .metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing materialization")?;
    insta::assert_yaml_snapshot!(env.metadata.table_status(&catalog, table).await?.ingestion.materialization, @r#"
    state: running
    eligible_at: ~
    lease_expires_at: "1970-01-01T00:00:46Z"
    failure: ~
    "#);

    // Completing materialization catches up WAL and schedules reconciliation and WAL GC.
    common::complete_empty_materialization(&env.metadata, &mat).await?;

    insta::assert_yaml_snapshot!(env.metadata.table_status(&catalog, table).await?, @r#"
    table_uuid: 00000000-0000-0000-0000-00000000002a
    table_ident:
      namespace:
        - test
      name: 00000000-0000-0000-0000-00000000002a
    checkpoint_interval_sec: 15
    observed_at: "1970-01-01T00:00:16Z"
    lifecycle: active
    health: healthy
    ingestion:
      wal:
        accepted_next_sequence: 2
        materialized_next_sequence: 2
        pending_batches: 0
        pending_bytes: 0
        oldest_pending_at: ~
      iceberg:
        desired_revision: 1
        reconciled_revision: 0
        reconciled_snapshot_id: ~
      materialization:
        state: idle
        eligible_at: ~
        lease_expires_at: ~
        failure: ~
      reconciliation:
        state: scheduled
        eligible_at: "1970-01-01T00:00:21Z"
        lease_expires_at: ~
        failure: ~
    maintenance:
      wal_gc:
        state: scheduled
        eligible_at: "1970-01-01T00:01:16Z"
        lease_expires_at: ~
        failure: ~
        pending_batches: 2
        pending_bytes: 350
        oldest_pending_at: "1970-01-01T00:00:16Z"
      deletion:
        state: idle
        eligible_at: ~
        lease_expires_at: ~
        failure: ~
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

    // Hold an active materialization lease (expires at 46_000) so deletion starts scheduled.
    env.clock.set(16_000);
    let _mat = env
        .metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing materialization")?;

    env.clock.set(20_000);
    env.metadata.delete_table(&catalog, table).await?;

    insta::assert_yaml_snapshot!(env.metadata.table_status(&catalog, table).await?, @r#"
    table_uuid: 00000000-0000-0000-0000-00000000002a
    table_ident:
      namespace:
        - test
      name: 00000000-0000-0000-0000-00000000002a
    checkpoint_interval_sec: 15
    observed_at: "1970-01-01T00:00:20Z"
    lifecycle: deleting
    health: healthy
    ingestion:
      wal:
        accepted_next_sequence: 1
        materialized_next_sequence: 0
        pending_batches: 1
        pending_bytes: 40
        oldest_pending_at: "1970-01-01T00:00:00Z"
      iceberg:
        desired_revision: 0
        reconciled_revision: 0
        reconciled_snapshot_id: ~
      materialization:
        state: idle
        eligible_at: ~
        lease_expires_at: ~
        failure: ~
      reconciliation:
        state: idle
        eligible_at: ~
        lease_expires_at: ~
        failure: ~
    maintenance:
      wal_gc:
        state: idle
        eligible_at: ~
        lease_expires_at: ~
        failure: ~
        pending_batches: 0
        pending_bytes: 0
        oldest_pending_at: ~
      deletion:
        state: scheduled
        eligible_at: "1970-01-01T00:00:46Z"
        lease_expires_at: ~
        failure: ~
    "#);

    // At 46_000 deletion becomes queued, and once claimed becomes running.
    env.clock.set(46_000);
    let queued = env.metadata.table_status(&catalog, table).await?;
    insta::assert_yaml_snapshot!((&queued.lifecycle, &queued.health, &queued.maintenance.deletion), @r#"
    - deleting
    - healthy
    - state: queued
      eligible_at: "1970-01-01T00:00:46Z"
      lease_expires_at: ~
      failure: ~
    "#);

    let _delete = env
        .metadata
        .claim_task(Uuid::from_u128(2))
        .await?
        .ok_or("missing delete task")?;
    let assigned = env.metadata.table_status(&catalog, table).await?;
    insta::assert_yaml_snapshot!((&assigned.lifecycle, &assigned.health, &assigned.maintenance.deletion), @r#"
    - deleting
    - healthy
    - state: running
      eligible_at: ~
      lease_expires_at: "1970-01-01T00:01:16Z"
      failure: ~
    "#);

    env.metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn table_status_tracks_transient_failures_escalation_and_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);
    let worker = Uuid::from_u128(1);

    env.resolve(&catalog, table).await?;
    env.metadata
        .wal_append(&catalog, table, 1, "wal/0", DateTime::UNIX_EPOCH, 100)
        .await?;

    let mut now_ms = 16_000;
    let delays_ms = [5_000, 10_000, 20_000, 40_000, 80_000];
    let mut observed = Vec::new();

    for (index, delay_ms) in delays_ms.into_iter().enumerate() {
        env.clock.set(now_ms);
        let task = env
            .metadata
            .claim_task(worker)
            .await?
            .ok_or("missing materialization")?;

        if index == 1 {
            // Prior failure stays visible while the retry attempt is running.
            let running = env.metadata.table_status(&catalog, table).await?;
            insta::assert_yaml_snapshot!((&running.health, &running.ingestion.materialization), @r#"
            - degraded
            - state: running
              eligible_at: ~
              lease_expires_at: "1970-01-01T00:00:51Z"
              failure:
                code: storage_error
                message: Failed to access object storage.
                failed_at: "1970-01-01T00:00:16Z"
                consecutive_failures: 1
            "#);
        }

        env.metadata
            .fail_task(&task.token, TaskFailureCode::StorageError)
            .await?;

        let status = env.metadata.table_status(&catalog, table).await?;
        observed.push((
            status.health,
            status.ingestion.materialization.state,
            status.ingestion.materialization.eligible_at,
            status
                .ingestion
                .materialization
                .failure
                .as_ref()
                .map(|failure| failure.consecutive_failures),
        ));

        now_ms += delay_ms;
    }

    insta::assert_yaml_snapshot!(observed, @r#"
    - - degraded
      - retrying
      - "1970-01-01T00:00:21Z"
      - 1
    - - degraded
      - retrying
      - "1970-01-01T00:00:31Z"
      - 2
    - - degraded
      - retrying
      - "1970-01-01T00:00:51Z"
      - 3
    - - degraded
      - retrying
      - "1970-01-01T00:01:31Z"
      - 4
    - - failing
      - retrying
      - "1970-01-01T00:02:51Z"
      - 5
    "#);

    // Completing the task clears its failure and restores overall table health.
    env.clock.set(now_ms);
    let recovered = env
        .metadata
        .claim_task(worker)
        .await?
        .ok_or("missing recovery attempt")?;
    common::complete_empty_materialization(&env.metadata, &recovered).await?;

    let status = env.metadata.table_status(&catalog, table).await?;
    insta::assert_yaml_snapshot!((&status.health, &status.ingestion.materialization), @r#"
    - healthy
    - state: idle
      eligible_at: ~
      lease_expires_at: ~
      failure: ~
    "#);

    env.metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn table_status_marks_non_transient_failure_as_failing_immediately()
-> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);
    let worker = Uuid::from_u128(1);

    env.resolve(&catalog, table).await?;
    env.metadata
        .wal_append(&catalog, table, 1, "wal/0", DateTime::UNIX_EPOCH, 100)
        .await?;

    env.clock.set(16_000);
    let mat = env
        .metadata
        .claim_task(worker)
        .await?
        .ok_or("missing materialization")?;
    common::complete_empty_materialization(&env.metadata, &mat).await?;

    env.clock.set(21_000);
    let rec = env
        .metadata
        .claim_task(worker)
        .await?
        .ok_or("missing reconciliation")?;
    env.metadata
        .fail_task(&rec.token, TaskFailureCode::ReconciliationConflict)
        .await?;

    let status = env.metadata.table_status(&catalog, table).await?;
    insta::assert_yaml_snapshot!((&status.health, &status.ingestion.reconciliation), @r#"
    - failing
    - state: retrying
      eligible_at: "1970-01-01T00:00:26Z"
      lease_expires_at: ~
      failure:
        code: reconciliation_conflict
        message: Iceberg table metadata changed concurrently outside Airfoil.
        failed_at: "1970-01-01T00:00:21Z"
        consecutive_failures: 1
    "#);

    env.metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn table_status_reports_expired_lease_before_and_after_reclaim()
-> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);

    env.resolve(&catalog, table).await?;
    env.metadata
        .wal_append(&catalog, table, 1, "wal/0", DateTime::UNIX_EPOCH, 100)
        .await?;

    env.clock.set(16_000);
    let _first = env
        .metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing materialization")?;

    // Once the 30s lease expires at 46_000, status reports lease_expired before reclaim.
    env.clock.set(46_000);
    let before_reclaim = env.metadata.table_status(&catalog, table).await?;
    insta::assert_yaml_snapshot!((&before_reclaim.health, &before_reclaim.ingestion.materialization), @r#"
    - degraded
    - state: queued
      eligible_at: "1970-01-01T00:00:46Z"
      lease_expires_at: ~
      failure:
        code: lease_expired
        message: Worker lease expired before the task completed.
        failed_at: "1970-01-01T00:00:46Z"
        consecutive_failures: 1
    "#);

    // Reclaiming persists the lease_expired failure while the replacement attempt runs.
    env.clock.set(50_000);
    let _second = env
        .metadata
        .claim_task(Uuid::from_u128(2))
        .await?
        .ok_or("missing reclaimed materialization")?;

    let after_reclaim = env.metadata.table_status(&catalog, table).await?;
    insta::assert_yaml_snapshot!((&after_reclaim.health, &after_reclaim.ingestion.materialization), @r#"
    - degraded
    - state: running
      eligible_at: ~
      lease_expires_at: "1970-01-01T00:01:20Z"
      failure:
        code: lease_expired
        message: Worker lease expired before the task completed.
        failed_at: "1970-01-01T00:00:46Z"
        consecutive_failures: 1
    "#);

    env.metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn table_status_clears_ingestion_failures_when_deletion_is_requested()
-> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);
    let worker = Uuid::from_u128(1);

    env.resolve(&catalog, table).await?;
    env.metadata
        .wal_append(&catalog, table, 1, "wal/0", DateTime::UNIX_EPOCH, 100)
        .await?;

    env.clock.set(16_000);
    let mat = env
        .metadata
        .claim_task(worker)
        .await?
        .ok_or("missing materialization")?;
    env.metadata
        .fail_task(&mat.token, TaskFailureCode::SchemaNotFound)
        .await?;

    assert_eq!(
        env.metadata.table_status(&catalog, table).await?.health,
        se_metadata::TableHealth::Failing
    );

    env.metadata.delete_table(&catalog, table).await?;

    let status = env.metadata.table_status(&catalog, table).await?;
    insta::assert_yaml_snapshot!(
        (
            &status.lifecycle,
            &status.health,
            &status.ingestion.materialization,
            &status.maintenance.deletion,
        ),
        @r#"
    - deleting
    - healthy
    - state: idle
      eligible_at: ~
      lease_expires_at: ~
      failure: ~
    - state: queued
      eligible_at: "1970-01-01T00:00:16Z"
      lease_expires_at: ~
      failure: ~
    "#
    );

    env.metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn table_status_tracks_wal_gc_failure_and_recovery() -> Result<(), Box<dyn std::error::Error>>
{
    let env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);

    env.resolve(&catalog, table).await?;
    env.metadata
        .wal_append(&catalog, table, 1, "wal/0", DateTime::UNIX_EPOCH, 120)
        .await?;

    env.clock.set(16_000);
    let mat = env
        .metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing materialization")?;
    common::complete_empty_materialization(&env.metadata, &mat).await?;

    // Hold reconciliation on worker 1 so worker 2 claims WAL GC at 76_000.
    env.clock.set(76_000);
    let _rec = env
        .metadata
        .claim_task(Uuid::from_u128(1))
        .await?
        .ok_or("missing reconciliation")?;
    let gc = env
        .metadata
        .claim_task(Uuid::from_u128(2))
        .await?
        .ok_or("missing wal gc")?;

    env.metadata
        .fail_task(&gc.token, TaskFailureCode::StorageError)
        .await?;

    let degraded = env.metadata.table_status(&catalog, table).await?;
    insta::assert_yaml_snapshot!((&degraded.health, &degraded.maintenance.wal_gc), @r#"
    - degraded
    - state: retrying
      eligible_at: "1970-01-01T00:01:21Z"
      lease_expires_at: ~
      failure:
        code: storage_error
        message: Failed to access object storage.
        failed_at: "1970-01-01T00:01:16Z"
        consecutive_failures: 1
      pending_batches: 1
      pending_bytes: 120
      oldest_pending_at: "1970-01-01T00:00:16Z"
    "#);

    env.clock.set(81_000);
    let gc_retry = env
        .metadata
        .claim_task(Uuid::from_u128(2))
        .await?
        .ok_or("missing wal gc retry")?;
    env.metadata.complete_wal_cleanup(&gc_retry.token).await?;

    let recovered = env.metadata.table_status(&catalog, table).await?;
    insta::assert_yaml_snapshot!((&recovered.health, &recovered.maintenance.wal_gc), @r#"
    - healthy
    - state: idle
      eligible_at: ~
      lease_expires_at: ~
      failure: ~
      pending_batches: 0
      pending_bytes: 0
      oldest_pending_at: ~
    "#);

    env.metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn table_status_tracks_deletion_failure() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let catalog = CatalogId::from(Uuid::from_u128(1));
    let table = Uuid::from_u128(42);
    let worker = Uuid::from_u128(1);

    env.resolve(&catalog, table).await?;
    env.clock.set(10_000);
    env.metadata.delete_table(&catalog, table).await?;

    let first = env
        .metadata
        .claim_task(worker)
        .await?
        .ok_or("missing deletion task")?;
    env.metadata
        .fail_task(&first.token, TaskFailureCode::StorageError)
        .await?;

    let degraded = env.metadata.table_status(&catalog, table).await?;
    insta::assert_yaml_snapshot!((&degraded.lifecycle, &degraded.health, &degraded.maintenance.deletion), @r#"
    - deleting
    - degraded
    - state: retrying
      eligible_at: "1970-01-01T00:00:15Z"
      lease_expires_at: ~
      failure:
        code: storage_error
        message: Failed to access object storage.
        failed_at: "1970-01-01T00:00:10Z"
        consecutive_failures: 1
    "#);

    env.clock.set(15_000);
    let second = env
        .metadata
        .claim_task(worker)
        .await?
        .ok_or("missing deletion retry")?;
    env.metadata
        .fail_task(&second.token, TaskFailureCode::CatalogNotFound)
        .await?;

    let failing = env.metadata.table_status(&catalog, table).await?;
    insta::assert_yaml_snapshot!((&failing.lifecycle, &failing.health, &failing.maintenance.deletion), @r#"
    - deleting
    - failing
    - state: retrying
      eligible_at: "1970-01-01T00:00:25Z"
      lease_expires_at: ~
      failure:
        code: catalog_not_found
        message: Catalog configuration was not found.
        failed_at: "1970-01-01T00:00:15Z"
        consecutive_failures: 2
    "#);

    env.metadata.close().await?;

    Ok(())
}
