mod common;

use std::{
    fmt,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use common::{document, record};
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
    memory::InMemory,
    path::Path,
    throttle::{ThrottleConfig, ThrottledStore},
};
use se_common::clock::{DefaultSystemClock, SystemClock};
use se_meta_store::catalog::{CatalogName, NamespaceIdent, TableIdent};
use se_scheduler::{
    Error, RetryPolicy, Task, TaskFailure, TaskOutcome, TaskScheduler,
    object_store::{DEFAULT_LOCATION, ObjectStoreBroker, ObjectStoreSchedulerOptions},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
struct FaultStore {
    inner: InMemory,
    fail_writes: AtomicBool,
    lose_response: AtomicBool,
    attempted_writes: AtomicUsize,
}

fn task(name: &str) -> Task {
    task_with_deadline(name, 0)
}

fn task_with_deadline(name: &str, deadline: i64) -> Task {
    Task::Reconcile {
        catalog_name: CatalogName::new("warehouse").unwrap(),
        table_ident: TableIdent::new(NamespaceIdent::new("analytics".into()), name.into()),
        // Deliberately overdue: freshness deadlines must not expire work.
        deadline: DateTime::from_timestamp(deadline, 0).unwrap(),
    }
}

async fn open(
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn SystemClock>,
    options: ObjectStoreSchedulerOptions,
    address: &str,
) -> (
    tokio::task::JoinHandle<se_scheduler::Result<()>>,
    Arc<dyn TaskScheduler>,
) {
    ObjectStoreBroker::new(
        store,
        Path::from(DEFAULT_LOCATION),
        address.into(),
        options,
        clock,
    )
    .await
    .unwrap()
    .run(CancellationToken::new())
}

#[tokio::test(start_paused = true)]
async fn batches_acknowledgments_and_excludes_requests_received_during_upload() {
    let mut trace = String::new();

    let clock = Arc::new(DefaultSystemClock::new());
    let store = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig::default(),
    ));

    let (_broker, scheduler) = open(
        store.clone(),
        clock.clone(),
        ObjectStoreSchedulerOptions::default(),
        "broker:1",
    )
    .await;

    store.config_mut(|config| config.wait_put_per_call = Duration::from_secs(2));

    let first = tokio::spawn({
        let scheduler = scheduler.clone();
        async move { scheduler.enqueue(task("first")).await }
    });

    let second = tokio::spawn({
        let scheduler = scheduler.clone();
        async move { scheduler.enqueue(task("second")).await }
    });

    tokio::task::yield_now().await;

    clock.sleep(Duration::from_millis(500)).await;

    assert!(!first.is_finished());
    record(
        &mut trace,
        "Before the first flush: enqueue is not acknowledged",
        store.as_ref(),
    )
    .await;
    clock.sleep(Duration::from_secs(1)).await;

    let later = tokio::spawn({
        let scheduler = scheduler.clone();
        async move { scheduler.enqueue(task("later")).await }
    });

    tokio::task::yield_now().await;

    let first_id = first.await.unwrap().unwrap();
    let second_id = second.await.unwrap().unwrap();

    assert!(!later.is_finished());
    record(
        &mut trace,
        "First batch committed; later enqueue is still unacknowledged",
        store.as_ref(),
    )
    .await;

    let durable = document(store.as_ref()).await;
    let ids: std::collections::BTreeSet<_> = durable["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|task| task["id"].as_str().unwrap().to_owned())
        .collect();

    assert_eq!(
        ids,
        [first_id.to_string(), second_id.to_string()]
            .into_iter()
            .collect()
    );

    let later_id = later.await.unwrap().unwrap();

    record(
        &mut trace,
        "Second batch committed; later enqueue is acknowledged",
        store.as_ref(),
    )
    .await;

    let durable = document(store.as_ref()).await;

    assert_eq!(durable["tasks"].as_array().unwrap().len(), 3);
    assert!(
        durable["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|task| task["id"] == later_id.to_string())
    );

    insta::assert_snapshot!(trace, @"
    Before the first flush: enqueue is not acknowledged
      broker: broker:1
      tasks: []
      failures (newest first): []

    First batch committed; later enqueue is still unacknowledged
      broker: broker:1
      tasks:
        queued: reconcile catalogs/warehouse/analytics.first deadline=0 attempts=0 next_deadline=-
        queued: reconcile catalogs/warehouse/analytics.second deadline=0 attempts=0 next_deadline=-
      failures (newest first): []

    Second batch committed; later enqueue is acknowledged
      broker: broker:1
      tasks:
        queued: reconcile catalogs/warehouse/analytics.first deadline=0 attempts=0 next_deadline=-
        queued: reconcile catalogs/warehouse/analytics.later deadline=0 attempts=0 next_deadline=-
        queued: reconcile catalogs/warehouse/analytics.second deadline=0 attempts=0 next_deadline=-
      failures (newest first): []
    ");
}

#[tokio::test(start_paused = true)]
async fn enqueues_coalesce_by_target_and_claims_use_earliest_deadline() {
    let mut trace = String::new();

    let clock = Arc::new(DefaultSystemClock::new());
    let store = Arc::new(InMemory::new());

    let (_broker, scheduler) = open(
        store.clone(),
        clock,
        ObjectStoreSchedulerOptions::default(),
        "broker:1",
    )
    .await;

    let first_id = scheduler
        .enqueue(task_with_deadline("orders", 30))
        .await
        .unwrap();

    let other_id = scheduler
        .enqueue(task_with_deadline("other", 20))
        .await
        .unwrap();

    record(&mut trace, "Initially queued", store.as_ref()).await;

    for deadline in [60, 30, 10, 25] {
        assert_eq!(
            scheduler
                .enqueue(task_with_deadline("orders", deadline))
                .await
                .unwrap(),
            first_id
        );

        record(
            &mut trace,
            &format!("Enqueue orders with deadline {deadline}"),
            store.as_ref(),
        )
        .await;
    }

    // The table name alone is not the coalescing key.
    let other_catalog = Task::Reconcile {
        catalog_name: CatalogName::new("another").unwrap(),
        table_ident: TableIdent::new(NamespaceIdent::new("analytics".into()), "orders".into()),
        deadline: DateTime::from_timestamp(40, 0).unwrap(),
    };

    let catalog_id = scheduler.enqueue(other_catalog.clone()).await.unwrap();
    let other_namespace = Task::Reconcile {
        catalog_name: CatalogName::new("warehouse").unwrap(),
        table_ident: TableIdent::new(NamespaceIdent::new("different".into()), "orders".into()),
        deadline: DateTime::from_timestamp(50, 0).unwrap(),
    };

    let namespace_id = scheduler.enqueue(other_namespace.clone()).await.unwrap();
    record(
        &mut trace,
        "Different catalogs and namespaces remain independent",
        store.as_ref(),
    )
    .await;

    for (id, expected) in [
        (first_id, task_with_deadline("orders", 10)),
        (other_id, task_with_deadline("other", 20)),
        (catalog_id, other_catalog),
        (namespace_id, other_namespace),
    ] {
        let claim = scheduler.claim().await.unwrap().unwrap();

        assert_eq!(claim.task_id, id);
        assert_eq!(claim.task, expected);

        record(&mut trace, "Claim the earliest deadline", store.as_ref()).await;

        scheduler
            .complete(id, claim.attempt_id, TaskOutcome::Success)
            .await
            .unwrap();
    }

    assert_eq!(scheduler.claim().await.unwrap(), None);
    record(&mut trace, "All tasks completed", store.as_ref()).await;

    insta::assert_snapshot!(trace, @"
    Initially queued
      broker: broker:1
      tasks:
        queued: reconcile catalogs/warehouse/analytics.orders deadline=30 attempts=0 next_deadline=-
        queued: reconcile catalogs/warehouse/analytics.other deadline=20 attempts=0 next_deadline=-
      failures (newest first): []

    Enqueue orders with deadline 60
      broker: broker:1
      tasks:
        queued: reconcile catalogs/warehouse/analytics.orders deadline=30 attempts=0 next_deadline=-
        queued: reconcile catalogs/warehouse/analytics.other deadline=20 attempts=0 next_deadline=-
      failures (newest first): []

    Enqueue orders with deadline 30
      broker: broker:1
      tasks:
        queued: reconcile catalogs/warehouse/analytics.orders deadline=30 attempts=0 next_deadline=-
        queued: reconcile catalogs/warehouse/analytics.other deadline=20 attempts=0 next_deadline=-
      failures (newest first): []

    Enqueue orders with deadline 10
      broker: broker:1
      tasks:
        queued: reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=0 next_deadline=-
        queued: reconcile catalogs/warehouse/analytics.other deadline=20 attempts=0 next_deadline=-
      failures (newest first): []

    Enqueue orders with deadline 25
      broker: broker:1
      tasks:
        queued: reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=0 next_deadline=-
        queued: reconcile catalogs/warehouse/analytics.other deadline=20 attempts=0 next_deadline=-
      failures (newest first): []

    Different catalogs and namespaces remain independent
      broker: broker:1
      tasks:
        queued: reconcile catalogs/another/analytics.orders deadline=40 attempts=0 next_deadline=-
        queued: reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=0 next_deadline=-
        queued: reconcile catalogs/warehouse/analytics.other deadline=20 attempts=0 next_deadline=-
        queued: reconcile catalogs/warehouse/different.orders deadline=50 attempts=0 next_deadline=-
      failures (newest first): []

    Claim the earliest deadline
      broker: broker:1
      tasks:
        queued: reconcile catalogs/another/analytics.orders deadline=40 attempts=0 next_deadline=-
        queued: reconcile catalogs/warehouse/analytics.other deadline=20 attempts=0 next_deadline=-
        queued: reconcile catalogs/warehouse/different.orders deadline=50 attempts=0 next_deadline=-
        running: reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=1 next_deadline=-
      failures (newest first): []

    Claim the earliest deadline
      broker: broker:1
      tasks:
        queued: reconcile catalogs/another/analytics.orders deadline=40 attempts=0 next_deadline=-
        queued: reconcile catalogs/warehouse/different.orders deadline=50 attempts=0 next_deadline=-
        running: reconcile catalogs/warehouse/analytics.other deadline=20 attempts=1 next_deadline=-
      failures (newest first): []

    Claim the earliest deadline
      broker: broker:1
      tasks:
        queued: reconcile catalogs/warehouse/different.orders deadline=50 attempts=0 next_deadline=-
        running: reconcile catalogs/another/analytics.orders deadline=40 attempts=1 next_deadline=-
      failures (newest first): []

    Claim the earliest deadline
      broker: broker:1
      tasks:
        running: reconcile catalogs/warehouse/different.orders deadline=50 attempts=1 next_deadline=-
      failures (newest first): []

    All tasks completed
      broker: broker:1
      tasks: []
      failures (newest first): []
    ");
}

#[tokio::test(start_paused = true)]
async fn running_tasks_preserve_the_earliest_follow_up_across_takeover() {
    let mut trace = String::new();

    let clock = Arc::new(DefaultSystemClock::new());
    let store = Arc::new(InMemory::new());

    let (broker, scheduler) = open(
        store.clone(),
        clock.clone(),
        ObjectStoreSchedulerOptions::default(),
        "broker:1",
    )
    .await;

    let id = scheduler
        .enqueue(task_with_deadline("orders", 10))
        .await
        .unwrap();

    let first = scheduler.claim().await.unwrap().unwrap();
    record(&mut trace, "Original run claimed", store.as_ref()).await;

    // Even a deadline later than the active task represents new work.
    for deadline in [50, 70, 40, 40] {
        assert_eq!(
            scheduler
                .enqueue(task_with_deadline("orders", deadline))
                .await
                .unwrap(),
            id
        );

        record(
            &mut trace,
            &format!("Enqueue while running: deadline {deadline}"),
            store.as_ref(),
        )
        .await;
    }

    assert_eq!(scheduler.claim().await.unwrap(), None);
    drop(scheduler);

    broker.await.unwrap().unwrap();

    let (_broker, scheduler) = open(
        store.clone(),
        clock,
        ObjectStoreSchedulerOptions::default(),
        "broker:2",
    )
    .await;

    record(
        &mut trace,
        "Replacement broker recovers the claim and follow-up",
        store.as_ref(),
    )
    .await;

    scheduler.heartbeat(id, first.attempt_id).await.unwrap();

    scheduler
        .complete(id, first.attempt_id, TaskOutcome::Success)
        .await
        .unwrap();

    record(
        &mut trace,
        "Completion promotes the follow-up immediately",
        store.as_ref(),
    )
    .await;

    let second = scheduler.claim().await.unwrap().unwrap();

    assert_eq!(second.task_id, id);
    assert_eq!(second.task, task_with_deadline("orders", 40));
    assert_ne!(second.attempt_id, first.attempt_id);
    assert!(matches!(
        scheduler
            .complete(id, first.attempt_id, TaskOutcome::Success)
            .await,
        Err(Error::StaleAttempt { .. })
    ));

    record(
        &mut trace,
        "Follow-up claimed; stale completion rejected",
        store.as_ref(),
    )
    .await;

    // A follow-up earlier than the current deadline is tracked independently too.
    scheduler
        .enqueue(task_with_deadline("orders", 20))
        .await
        .unwrap();

    scheduler
        .enqueue(task_with_deadline("orders", 30))
        .await
        .unwrap();

    record(
        &mut trace,
        "A second follow-up keeps deadline 20, not 30",
        store.as_ref(),
    )
    .await;

    scheduler
        .complete(id, second.attempt_id, TaskOutcome::Success)
        .await
        .unwrap();

    let third = scheduler.claim().await.unwrap().unwrap();

    assert_eq!(third.task, task_with_deadline("orders", 20));
    record(
        &mut trace,
        "Next run starts with a fresh attempt budget",
        store.as_ref(),
    )
    .await;

    scheduler
        .complete(id, third.attempt_id, TaskOutcome::Success)
        .await
        .unwrap();
    assert_eq!(scheduler.claim().await.unwrap(), None);
    record(&mut trace, "No follow-up remains", store.as_ref()).await;

    insta::assert_snapshot!(trace, @"
    Original run claimed
      broker: broker:1
      tasks:
        running: reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=1 next_deadline=-
      failures (newest first): []

    Enqueue while running: deadline 50
      broker: broker:1
      tasks:
        running: reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=1 next_deadline=50
      failures (newest first): []

    Enqueue while running: deadline 70
      broker: broker:1
      tasks:
        running: reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=1 next_deadline=50
      failures (newest first): []

    Enqueue while running: deadline 40
      broker: broker:1
      tasks:
        running: reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=1 next_deadline=40
      failures (newest first): []

    Enqueue while running: deadline 40
      broker: broker:1
      tasks:
        running: reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=1 next_deadline=40
      failures (newest first): []

    Replacement broker recovers the claim and follow-up
      broker: broker:2
      tasks:
        running: reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=1 next_deadline=40
      failures (newest first): []

    Completion promotes the follow-up immediately
      broker: broker:2
      tasks:
        queued: reconcile catalogs/warehouse/analytics.orders deadline=40 attempts=0 next_deadline=-
      failures (newest first): []

    Follow-up claimed; stale completion rejected
      broker: broker:2
      tasks:
        running: reconcile catalogs/warehouse/analytics.orders deadline=40 attempts=1 next_deadline=-
      failures (newest first): []

    A second follow-up keeps deadline 20, not 30
      broker: broker:2
      tasks:
        running: reconcile catalogs/warehouse/analytics.orders deadline=40 attempts=1 next_deadline=20
      failures (newest first): []

    Next run starts with a fresh attempt budget
      broker: broker:2
      tasks:
        running: reconcile catalogs/warehouse/analytics.orders deadline=20 attempts=1 next_deadline=-
      failures (newest first): []

    No follow-up remains
      broker: broker:2
      tasks: []
      failures (newest first): []
    ");
}

#[tokio::test(start_paused = true)]
async fn terminal_failure_preserves_follow_up_and_manual_requeue_coalesces() {
    let mut trace = String::new();

    for failure in [
        TaskFailure::Permanent {
            reason: "invalid configuration".into(),
        },
        TaskFailure::Retryable {
            reason: "attempts exhausted".into(),
        },
    ] {
        let clock = Arc::new(DefaultSystemClock::new());
        let store = Arc::new(InMemory::new());
        let mut options = ObjectStoreSchedulerOptions::default();
        options.retry_policy.max_attempts = NonZeroU32::MIN;

        let (_broker, scheduler) = open(store.clone(), clock, options, "broker:1").await;

        let id = scheduler
            .enqueue(task_with_deadline("orders", 10))
            .await
            .unwrap();

        let first = scheduler.claim().await.unwrap().unwrap();

        scheduler
            .enqueue(task_with_deadline("orders", 40))
            .await
            .unwrap();

        scheduler
            .complete(id, first.attempt_id, TaskOutcome::Failure(failure.clone()))
            .await
            .unwrap();

        let failed = scheduler.failed_tasks(10).await.unwrap();

        assert_eq!(failed[0].task_id, id);
        assert_eq!(failed[0].task, task_with_deadline("orders", 10));
        assert_eq!(failed[0].failure, failure);
        record(
            &mut trace,
            "Terminal failure recorded; follow-up immediately ready",
            store.as_ref(),
        )
        .await;

        let follow_up = scheduler.claim().await.unwrap().unwrap();
        assert_eq!(follow_up.task_id, id);
        assert_eq!(follow_up.task, task_with_deadline("orders", 40));

        scheduler
            .complete(id, follow_up.attempt_id, TaskOutcome::Success)
            .await
            .unwrap();

        let new_id = scheduler
            .enqueue(task_with_deadline("orders", 50))
            .await
            .unwrap();

        assert_eq!(scheduler.requeue(id).await.unwrap(), new_id);
        record(
            &mut trace,
            "Manual requeue coalesces with the outstanding task",
            store.as_ref(),
        )
        .await;

        let requeued = scheduler.claim().await.unwrap().unwrap();
        assert_eq!(requeued.task, task_with_deadline("orders", 10));
        scheduler
            .complete(new_id, requeued.attempt_id, TaskOutcome::Success)
            .await
            .unwrap();

        assert_eq!(scheduler.claim().await.unwrap(), None);
        record(
            &mut trace,
            "Requeued task completed; failure history retained",
            store.as_ref(),
        )
        .await;
    }

    insta::assert_snapshot!(trace, @r#"
    Terminal failure recorded; follow-up immediately ready
      broker: broker:1
      tasks:
        queued: reconcile catalogs/warehouse/analytics.orders deadline=40 attempts=0 next_deadline=-
      failures (newest first):
        reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=1 failure={"Permanent":{"reason":"invalid configuration"}}

    Manual requeue coalesces with the outstanding task
      broker: broker:1
      tasks:
        queued: reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=0 next_deadline=-
      failures (newest first):
        reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=1 failure={"Permanent":{"reason":"invalid configuration"}}

    Requeued task completed; failure history retained
      broker: broker:1
      tasks: []
      failures (newest first):
        reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=1 failure={"Permanent":{"reason":"invalid configuration"}}

    Terminal failure recorded; follow-up immediately ready
      broker: broker:1
      tasks:
        queued: reconcile catalogs/warehouse/analytics.orders deadline=40 attempts=0 next_deadline=-
      failures (newest first):
        reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=1 failure={"Retryable":{"reason":"attempts exhausted"}}

    Manual requeue coalesces with the outstanding task
      broker: broker:1
      tasks:
        queued: reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=0 next_deadline=-
      failures (newest first):
        reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=1 failure={"Retryable":{"reason":"attempts exhausted"}}

    Requeued task completed; failure history retained
      broker: broker:1
      tasks: []
      failures (newest first):
        reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=1 failure={"Retryable":{"reason":"attempts exhausted"}}
    "#);
}

#[tokio::test(start_paused = true)]
async fn follow_ups_survive_retries_without_bypassing_backoff() {
    let mut trace = String::new();

    let clock = Arc::new(DefaultSystemClock::new());
    let store = Arc::new(InMemory::new());

    let options = ObjectStoreSchedulerOptions {
        retry_policy: RetryPolicy {
            max_attempts: NonZeroU32::new(2).unwrap(),
            initial_backoff: Duration::from_secs(10),
            max_backoff: Duration::from_secs(10),
        },
        ..Default::default()
    };
    let (_broker, scheduler) = open(store.clone(), clock.clone(), options, "broker:1").await;

    let id = scheduler
        .enqueue(task_with_deadline("orders", 10))
        .await
        .unwrap();

    let first = scheduler.claim().await.unwrap().unwrap();

    scheduler
        .enqueue(task_with_deadline("orders", 40))
        .await
        .unwrap();

    let failure = TaskOutcome::Failure(TaskFailure::Retryable {
        reason: "temporary".into(),
    });

    scheduler
        .complete(id, first.attempt_id, failure.clone())
        .await
        .unwrap();

    assert_eq!(
        scheduler
            .enqueue(task_with_deadline("orders", 30))
            .await
            .unwrap(),
        id
    );

    assert_eq!(scheduler.claim().await.unwrap(), None);
    record(
        &mut trace,
        "Retry backoff retained; follow-up deadline tightened to 30",
        store.as_ref(),
    )
    .await;

    clock.sleep(Duration::from_secs(10)).await;

    let retry = scheduler.claim().await.unwrap().unwrap();

    assert_eq!(retry.task, task_with_deadline("orders", 10));
    record(
        &mut trace,
        "Original work retried before its follow-up",
        store.as_ref(),
    )
    .await;

    scheduler
        .complete(id, retry.attempt_id, TaskOutcome::Success)
        .await
        .unwrap();

    let follow_up = scheduler.claim().await.unwrap().unwrap();

    assert_eq!(follow_up.task, task_with_deadline("orders", 30));
    record(
        &mut trace,
        "Follow-up starts with its own budget",
        store.as_ref(),
    )
    .await;

    // The preceding run used its full budget; this run must still be retryable.
    scheduler
        .complete(id, follow_up.attempt_id, failure)
        .await
        .unwrap();

    assert_eq!(scheduler.failed_tasks(10).await.unwrap(), vec![]);
    assert_eq!(scheduler.claim().await.unwrap(), None);
    record(
        &mut trace,
        "Follow-up failure can still retry",
        store.as_ref(),
    )
    .await;

    clock.sleep(Duration::from_secs(10)).await;

    let retry = scheduler.claim().await.unwrap().unwrap();
    assert_eq!(retry.task, task_with_deadline("orders", 30));

    scheduler
        .complete(id, retry.attempt_id, TaskOutcome::Success)
        .await
        .unwrap();

    assert_eq!(scheduler.claim().await.unwrap(), None);
    record(&mut trace, "Retry completed successfully", store.as_ref()).await;

    insta::assert_snapshot!(trace, @r#"
    Retry backoff retained; follow-up deadline tightened to 30
      broker: broker:1
      tasks:
        retry: reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=1 next_deadline=30 failure={"Retryable":{"reason":"temporary"}}
      failures (newest first): []

    Original work retried before its follow-up
      broker: broker:1
      tasks:
        running: reconcile catalogs/warehouse/analytics.orders deadline=10 attempts=2 next_deadline=30 failure={"Retryable":{"reason":"temporary"}}
      failures (newest first): []

    Follow-up starts with its own budget
      broker: broker:1
      tasks:
        running: reconcile catalogs/warehouse/analytics.orders deadline=30 attempts=1 next_deadline=-
      failures (newest first): []

    Follow-up failure can still retry
      broker: broker:1
      tasks:
        retry: reconcile catalogs/warehouse/analytics.orders deadline=30 attempts=1 next_deadline=- failure={"Retryable":{"reason":"temporary"}}
      failures (newest first): []

    Retry completed successfully
      broker: broker:1
      tasks: []
      failures (newest first): []
    "#);
}

#[tokio::test(start_paused = true)]
async fn takeover_preserves_claims_and_fences_the_old_broker() {
    let mut trace = String::new();

    let clock = Arc::new(DefaultSystemClock::new());
    let store = Arc::new(InMemory::new());

    let (first_broker, first) = open(
        store.clone(),
        clock.clone(),
        ObjectStoreSchedulerOptions::default(),
        "first:1",
    )
    .await;

    let id = first.enqueue(task("orders")).await.unwrap();
    let claim = first.claim().await.unwrap().unwrap();

    assert_eq!(claim.task_id, id);
    assert_eq!(claim.task, task("orders"));

    let waiting_id = first.enqueue(task("waiting")).await.unwrap();
    record(
        &mut trace,
        "First broker has one running and one queued task",
        store.as_ref(),
    )
    .await;

    let (_second_broker, second) = open(
        store.clone(),
        clock.clone(),
        ObjectStoreSchedulerOptions::default(),
        "second:2",
    )
    .await;

    let broker = ObjectStoreBroker::read_broker(store.as_ref(), &Path::from(DEFAULT_LOCATION))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(broker.address, "second:2");
    assert!(matches!(
        first.enqueue(task("must-not-persist")).await,
        Err(Error::NotBroker | Error::Closed)
    ));

    assert!(matches!(first_broker.await.unwrap(), Err(Error::NotBroker)));
    assert!(matches!(first.claim().await, Err(Error::Closed)));

    record(
        &mut trace,
        "Replacement owns the queue; old broker's enqueue rejected",
        store.as_ref(),
    )
    .await;

    let waiting = second.claim().await.unwrap().unwrap();

    assert_eq!(waiting.task_id, waiting_id);

    second
        .complete(waiting_id, waiting.attempt_id, TaskOutcome::Success)
        .await
        .unwrap();

    assert_eq!(second.claim().await.unwrap(), None);

    second.heartbeat(id, claim.attempt_id).await.unwrap();
    second
        .complete(id, claim.attempt_id, TaskOutcome::Success)
        .await
        .unwrap();

    assert_eq!(second.claim().await.unwrap(), None);
    record(
        &mut trace,
        "Replacement completes both recovered tasks",
        store.as_ref(),
    )
    .await;

    insta::assert_snapshot!(trace, @"
    First broker has one running and one queued task
      broker: first:1
      tasks:
        queued: reconcile catalogs/warehouse/analytics.waiting deadline=0 attempts=0 next_deadline=-
        running: reconcile catalogs/warehouse/analytics.orders deadline=0 attempts=1 next_deadline=-
      failures (newest first): []

    Replacement owns the queue; old broker's enqueue rejected
      broker: second:2
      tasks:
        queued: reconcile catalogs/warehouse/analytics.waiting deadline=0 attempts=0 next_deadline=-
        running: reconcile catalogs/warehouse/analytics.orders deadline=0 attempts=1 next_deadline=-
      failures (newest first): []

    Replacement completes both recovered tasks
      broker: second:2
      tasks: []
      failures (newest first): []
    ");
}

#[tokio::test(start_paused = true)]
async fn idle_broker_exits_after_takeover() {
    let clock = Arc::new(DefaultSystemClock::new());
    let store = Arc::new(InMemory::new());
    let (first_broker, first) = open(
        store.clone(),
        clock.clone(),
        ObjectStoreSchedulerOptions::default(),
        "first:1",
    )
    .await;
    let (second_broker, second) = open(
        store,
        clock,
        ObjectStoreSchedulerOptions::default(),
        "second:2",
    )
    .await;

    // No requests to the first broker: its periodic flush must detect takeover.
    insta::assert_debug_snapshot!(first_broker.await.unwrap(), @"
    Err(
        NotBroker,
    )
    ");
    assert!(matches!(
        first.enqueue(task("orders")).await,
        Err(Error::Closed)
    ));
    assert_eq!(second.claim().await.unwrap(), None);
    drop(second);
    second_broker.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn cancellation_stops_an_idle_broker_with_live_clients() {
    let ct = CancellationToken::new();
    let broker = ObjectStoreBroker::new(
        Arc::new(InMemory::new()),
        Path::from(DEFAULT_LOCATION),
        "broker:1".into(),
        ObjectStoreSchedulerOptions::default(),
        Arc::new(DefaultSystemClock::new()),
    )
    .await
    .unwrap();
    let (handle, scheduler) = broker.run(ct.clone());
    assert_eq!(scheduler.claim().await.unwrap(), None);

    ct.cancel();

    let stopped = tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .unwrap()
        .unwrap();
    insta::assert_compact_debug_snapshot!(
        (stopped, scheduler.claim().await),
        @"(Ok(()), Err(Closed))"
    );
}

#[tokio::test(start_paused = true)]
async fn cancellation_interrupts_uploads_and_closes_pending_requests() {
    let clock = Arc::new(DefaultSystemClock::new());
    let store = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig::default(),
    ));
    let ct = CancellationToken::new();
    let broker = ObjectStoreBroker::new(
        store.clone(),
        Path::from(DEFAULT_LOCATION),
        "broker:1".into(),
        ObjectStoreSchedulerOptions::default(),
        clock.clone(),
    )
    .await
    .unwrap();
    let (handle, scheduler) = broker.run(ct.clone());
    store.config_mut(|config| config.wait_put_per_call = Duration::from_secs(3600));
    let first = tokio::spawn({
        let scheduler = scheduler.clone();
        async move { scheduler.enqueue(task("first")).await }
    });
    tokio::task::yield_now().await;
    clock.sleep(Duration::from_secs(2)).await;
    let second = tokio::spawn({
        let scheduler = scheduler.clone();
        async move { scheduler.enqueue(task("second")).await }
    });
    tokio::task::yield_now().await;
    assert!(!first.is_finished());
    assert!(!second.is_finished());

    ct.cancel();

    let stopped = tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .unwrap()
        .unwrap();
    insta::assert_compact_debug_snapshot!(
        (stopped, first.await.unwrap(), second.await.unwrap(), scheduler.claim().await),
        @"(Ok(()), Err(Closed), Err(Closed), Err(Closed))"
    );
    let mut trace = String::new();
    record(
        &mut trace,
        "Cancelled upload leaves durable state unchanged",
        store.as_ref(),
    )
    .await;
    insta::assert_snapshot!(trace, @"
    Cancelled upload leaves durable state unchanged
      broker: broker:1
      tasks: []
      failures (newest first): []
    ");
}

#[tokio::test(start_paused = true)]
async fn broker_handle_can_abort_with_live_clients() {
    let broker = ObjectStoreBroker::new(
        Arc::new(InMemory::new()),
        Path::from(DEFAULT_LOCATION),
        "broker:1".into(),
        ObjectStoreSchedulerOptions::default(),
        Arc::new(DefaultSystemClock::new()),
    )
    .await
    .unwrap();
    let (handle, scheduler) = broker.run(CancellationToken::new());
    let client = scheduler.clone();
    drop(scheduler);
    assert_eq!(client.claim().await.unwrap(), None);
    assert!(!handle.is_finished());

    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
    assert!(matches!(
        client.enqueue(task("orders")).await,
        Err(Error::Closed)
    ));
}

#[tokio::test(start_paused = true)]
async fn retry_backoff_exhaustion_and_stale_attempts() {
    let mut trace = String::new();

    let clock = Arc::new(DefaultSystemClock::new());
    let store = Arc::new(InMemory::new());

    let options = ObjectStoreSchedulerOptions {
        retry_policy: RetryPolicy {
            max_attempts: NonZeroU32::new(2).unwrap(),
            initial_backoff: Duration::from_secs(10),
            max_backoff: Duration::from_secs(10),
        },
        ..Default::default()
    };
    let (_broker, scheduler) = open(store.clone(), clock.clone(), options, "broker:1").await;

    let id = scheduler.enqueue(task("retry")).await.unwrap();

    let first = scheduler.claim().await.unwrap().unwrap();
    let failure = TaskFailure::Retryable {
        reason: "catalog unavailable".into(),
    };

    scheduler
        .complete(id, first.attempt_id, TaskOutcome::Failure(failure.clone()))
        .await
        .unwrap();

    assert_eq!(scheduler.claim().await.unwrap(), None);
    record(
        &mut trace,
        "First failure schedules a retry, not a terminal failure",
        store.as_ref(),
    )
    .await;

    clock.sleep(Duration::from_secs(10)).await;

    let second = scheduler.claim().await.unwrap().unwrap();

    assert_eq!(second.task_id, id);
    assert_ne!(second.attempt_id, first.attempt_id);
    assert!(matches!(
        scheduler.heartbeat(id, first.attempt_id).await,
        Err(Error::StaleAttempt { .. })
    ));
    assert!(matches!(
        scheduler
            .complete(id, first.attempt_id, TaskOutcome::Success)
            .await,
        Err(Error::StaleAttempt { .. })
    ));
    record(
        &mut trace,
        "Second attempt active; stale heartbeat and completion rejected",
        store.as_ref(),
    )
    .await;

    scheduler
        .complete(id, second.attempt_id, TaskOutcome::Failure(failure.clone()))
        .await
        .unwrap();

    let failed = scheduler.failed_tasks(10).await.unwrap();

    assert_eq!(failed[0].task_id, id);
    assert_eq!(failed[0].attempts, 2);
    assert_eq!(failed[0].failure, failure);
    record(
        &mut trace,
        "Second failure exhausts the two-attempt budget",
        store.as_ref(),
    )
    .await;

    clock.sleep(Duration::from_secs(20)).await;

    assert_eq!(scheduler.claim().await.unwrap(), None);
    record(
        &mut trace,
        "No automatic retry after exhaustion",
        store.as_ref(),
    )
    .await;

    insta::assert_snapshot!(trace, @r#"
    First failure schedules a retry, not a terminal failure
      broker: broker:1
      tasks:
        retry: reconcile catalogs/warehouse/analytics.retry deadline=0 attempts=1 next_deadline=- failure={"Retryable":{"reason":"catalog unavailable"}}
      failures (newest first): []

    Second attempt active; stale heartbeat and completion rejected
      broker: broker:1
      tasks:
        running: reconcile catalogs/warehouse/analytics.retry deadline=0 attempts=2 next_deadline=- failure={"Retryable":{"reason":"catalog unavailable"}}
      failures (newest first): []

    Second failure exhausts the two-attempt budget
      broker: broker:1
      tasks: []
      failures (newest first):
        reconcile catalogs/warehouse/analytics.retry deadline=0 attempts=2 failure={"Retryable":{"reason":"catalog unavailable"}}

    No automatic retry after exhaustion
      broker: broker:1
      tasks: []
      failures (newest first):
        reconcile catalogs/warehouse/analytics.retry deadline=0 attempts=2 failure={"Retryable":{"reason":"catalog unavailable"}}
    "#);
}

#[tokio::test(start_paused = true)]
async fn retries_use_exponential_backoff_with_capped_jitter() {
    let mut trace = String::new();

    let clock = Arc::new(DefaultSystemClock::new());
    let store = Arc::new(InMemory::new());

    let options = ObjectStoreSchedulerOptions {
        retry_policy: RetryPolicy {
            max_attempts: NonZeroU32::new(4).unwrap(),
            initial_backoff: Duration::from_secs(4),
            max_backoff: Duration::from_secs(10),
        },
        ..Default::default()
    };
    let (_broker, scheduler) = open(store.clone(), clock.clone(), options, "broker:1").await;

    let id = scheduler.enqueue(task("backoff")).await.unwrap();

    for cap_seconds in [4, 8, 10] {
        let claim = scheduler.claim().await.unwrap().unwrap();
        let reported_at = clock.now();
        scheduler
            .complete(
                id,
                claim.attempt_id,
                TaskOutcome::Failure(TaskFailure::Retryable {
                    reason: "temporary".into(),
                }),
            )
            .await
            .unwrap();
        let durable = document(store.as_ref()).await;

        record(
            &mut trace,
            &format!(
                "Retry delay is between {} and {cap_seconds} seconds",
                cap_seconds / 2
            ),
            store.as_ref(),
        )
        .await;

        let retry_at = durable["tasks"][0]["next_attempt_at"]
            .as_str()
            .unwrap()
            .parse::<DateTime<Utc>>()
            .unwrap();
        let delay = retry_at
            .signed_duration_since(reported_at)
            .to_std()
            .unwrap();

        assert!(delay >= Duration::from_secs(cap_seconds / 2));
        assert!(delay <= Duration::from_secs(cap_seconds));
        clock.sleep(Duration::from_secs(cap_seconds)).await;
    }

    let last = scheduler.claim().await.unwrap().unwrap();
    scheduler
        .complete(id, last.attempt_id, TaskOutcome::Success)
        .await
        .unwrap();

    assert_eq!(scheduler.failed_tasks(10).await.unwrap(), vec![]);
    record(&mut trace, "Fourth attempt succeeds", store.as_ref()).await;

    insta::assert_snapshot!(trace, @r#"
    Retry delay is between 2 and 4 seconds
      broker: broker:1
      tasks:
        retry: reconcile catalogs/warehouse/analytics.backoff deadline=0 attempts=1 next_deadline=- failure={"Retryable":{"reason":"temporary"}}
      failures (newest first): []

    Retry delay is between 4 and 8 seconds
      broker: broker:1
      tasks:
        retry: reconcile catalogs/warehouse/analytics.backoff deadline=0 attempts=2 next_deadline=- failure={"Retryable":{"reason":"temporary"}}
      failures (newest first): []

    Retry delay is between 5 and 10 seconds
      broker: broker:1
      tasks:
        retry: reconcile catalogs/warehouse/analytics.backoff deadline=0 attempts=3 next_deadline=- failure={"Retryable":{"reason":"temporary"}}
      failures (newest first): []

    Fourth attempt succeeds
      broker: broker:1
      tasks: []
      failures (newest first): []
    "#);
}

#[tokio::test(start_paused = true)]
async fn heartbeats_extend_claims_and_expiry_consumes_an_attempt() {
    let mut trace = String::new();

    let clock = Arc::new(DefaultSystemClock::new());
    let store = Arc::new(InMemory::new());

    let options = ObjectStoreSchedulerOptions {
        heartbeat_timeout: Duration::from_secs(10),
        ..Default::default()
    };
    let (_broker, scheduler) = open(store.clone(), clock.clone(), options, "broker:1").await;

    let id = scheduler.enqueue(task("heartbeat")).await.unwrap();
    let first = scheduler.claim().await.unwrap().unwrap();

    record(&mut trace, "Initial claim", store.as_ref()).await;

    clock.sleep(Duration::from_secs(6)).await;

    scheduler.heartbeat(id, first.attempt_id).await.unwrap();
    record(
        &mut trace,
        "Heartbeat renews the claim after six seconds",
        store.as_ref(),
    )
    .await;

    clock.sleep(Duration::from_secs(4)).await;

    assert_eq!(scheduler.claim().await.unwrap(), None);
    record(
        &mut trace,
        "Original timeout passed, but renewed claim is still active",
        store.as_ref(),
    )
    .await;

    clock.sleep(Duration::from_secs(8)).await;

    let next = scheduler.claim().await.unwrap().unwrap();

    assert_eq!(next.task_id, id);
    assert_ne!(next.attempt_id, first.attempt_id);
    record(
        &mut trace,
        "Missing heartbeat expires the claim; second attempt starts",
        store.as_ref(),
    )
    .await;

    insta::assert_snapshot!(trace, @r#"
    Initial claim
      broker: broker:1
      tasks:
        running: reconcile catalogs/warehouse/analytics.heartbeat deadline=0 attempts=1 next_deadline=-
      failures (newest first): []

    Heartbeat renews the claim after six seconds
      broker: broker:1
      tasks:
        running: reconcile catalogs/warehouse/analytics.heartbeat deadline=0 attempts=1 next_deadline=-
      failures (newest first): []

    Original timeout passed, but renewed claim is still active
      broker: broker:1
      tasks:
        running: reconcile catalogs/warehouse/analytics.heartbeat deadline=0 attempts=1 next_deadline=-
      failures (newest first): []

    Missing heartbeat expires the claim; second attempt starts
      broker: broker:1
      tasks:
        running: reconcile catalogs/warehouse/analytics.heartbeat deadline=0 attempts=2 next_deadline=- failure={"Retryable":{"reason":"worker heartbeat expired"}}
      failures (newest first): []
    "#);
}

#[tokio::test(start_paused = true)]
async fn permanent_failures_are_bounded_and_can_be_requeued() {
    let mut trace = String::new();

    let clock = Arc::new(DefaultSystemClock::new());
    let store = Arc::new(InMemory::new());

    let options = ObjectStoreSchedulerOptions {
        max_failed_tasks: 1,
        ..Default::default()
    };
    let (_broker, scheduler) =
        open(store.clone(), clock.clone(), options.clone(), "broker:1").await;

    let mut ids = Vec::new();

    for name in ["first", "second"] {
        let id = scheduler.enqueue(task(name)).await.unwrap();
        let claim = scheduler.claim().await.unwrap().unwrap();

        scheduler
            .complete(
                id,
                claim.attempt_id,
                TaskOutcome::Failure(TaskFailure::Permanent {
                    reason: name.into(),
                }),
            )
            .await
            .unwrap();
        ids.push(id);

        record(
            &mut trace,
            &format!("Permanent failure for {name}; retain only the newest failure"),
            store.as_ref(),
        )
        .await;
    }

    let (_replacement_broker, scheduler) =
        open(store.clone(), clock, options, "replacement:2").await;
    let failed = scheduler.failed_tasks(10).await.unwrap();

    assert_eq!(failed[0].task_id, ids[1]);
    assert_eq!(failed[0].attempts, 1);
    record(
        &mut trace,
        "Replacement recovers bounded failure history",
        store.as_ref(),
    )
    .await;

    assert_eq!(scheduler.failed_tasks(0).await.unwrap(), vec![]);
    assert!(matches!(
        scheduler.requeue(ids[0]).await,
        Err(Error::TaskNotFound(_))
    ));

    let new_id = scheduler.requeue(ids[1]).await.unwrap();
    assert_ne!(new_id, ids[1]);
    record(
        &mut trace,
        "Explicit requeue creates ready work and preserves history",
        store.as_ref(),
    )
    .await;

    let claim = scheduler.claim().await.unwrap().unwrap();

    assert_eq!(claim.task_id, new_id);
    assert_eq!(claim.task, task("second"));
    assert_eq!(scheduler.failed_tasks(10).await.unwrap(), failed);

    scheduler
        .complete(new_id, claim.attempt_id, TaskOutcome::Success)
        .await
        .unwrap();

    assert_eq!(scheduler.claim().await.unwrap(), None);

    // A terminal failure must not disable future notifications for this table.
    let id = scheduler.enqueue(task("second")).await.unwrap();

    assert_eq!(scheduler.claim().await.unwrap().unwrap().task_id, id);
    record(
        &mut trace,
        "Failure history does not disable new notifications",
        store.as_ref(),
    )
    .await;

    insta::assert_snapshot!(trace, @r#"
    Permanent failure for first; retain only the newest failure
      broker: broker:1
      tasks: []
      failures (newest first):
        reconcile catalogs/warehouse/analytics.first deadline=0 attempts=1 failure={"Permanent":{"reason":"first"}}

    Permanent failure for second; retain only the newest failure
      broker: broker:1
      tasks: []
      failures (newest first):
        reconcile catalogs/warehouse/analytics.second deadline=0 attempts=1 failure={"Permanent":{"reason":"second"}}

    Replacement recovers bounded failure history
      broker: replacement:2
      tasks: []
      failures (newest first):
        reconcile catalogs/warehouse/analytics.second deadline=0 attempts=1 failure={"Permanent":{"reason":"second"}}

    Explicit requeue creates ready work and preserves history
      broker: replacement:2
      tasks:
        queued: reconcile catalogs/warehouse/analytics.second deadline=0 attempts=0 next_deadline=-
      failures (newest first):
        reconcile catalogs/warehouse/analytics.second deadline=0 attempts=1 failure={"Permanent":{"reason":"second"}}

    Failure history does not disable new notifications
      broker: replacement:2
      tasks:
        running: reconcile catalogs/warehouse/analytics.second deadline=0 attempts=1 next_deadline=-
      failures (newest first):
        reconcile catalogs/warehouse/analytics.second deadline=0 attempts=1 failure={"Permanent":{"reason":"second"}}
    "#);
}

#[tokio::test(start_paused = true)]
async fn failed_uploads_wait_for_durability_and_lost_responses_are_resolved() {
    let mut trace = String::new();

    let clock = Arc::new(DefaultSystemClock::new());
    let store = Arc::new(FaultStore::default());

    let options = ObjectStoreSchedulerOptions {
        max_pending_requests: 1,
        ..Default::default()
    };
    let (_broker, scheduler) = open(store.clone(), clock.clone(), options, "broker:1").await;
    store.fail_writes.store(true, Ordering::SeqCst);

    let request = tokio::spawn({
        let scheduler = scheduler.clone();
        async move { scheduler.enqueue(task("outage")).await }
    });

    tokio::task::yield_now().await;
    clock.sleep(Duration::from_secs(3)).await;

    assert!(!request.is_finished());
    assert!(store.attempted_writes.load(Ordering::SeqCst) > 1);
    record(
        &mut trace,
        "Upload outage: request pending and durable queue unchanged",
        store.as_ref(),
    )
    .await;

    store.fail_writes.store(false, Ordering::SeqCst);
    store.lose_response.store(true, Ordering::SeqCst);

    let id = request.await.unwrap().unwrap();
    assert_eq!(
        document(store.as_ref()).await["tasks"][0]["id"],
        id.to_string()
    );
    assert!(!store.lose_response.load(Ordering::SeqCst));
    record(
        &mut trace,
        "Successful upload with lost response recovered by reading the object",
        store.as_ref(),
    )
    .await;

    assert_eq!(scheduler.claim().await.unwrap().unwrap().task_id, id);
    assert_eq!(scheduler.claim().await.unwrap(), None);
    record(
        &mut trace,
        "Exactly one task is claimed after recovery",
        store.as_ref(),
    )
    .await;

    insta::assert_snapshot!(trace, @"
    Upload outage: request pending and durable queue unchanged
      broker: broker:1
      tasks: []
      failures (newest first): []

    Successful upload with lost response recovered by reading the object
      broker: broker:1
      tasks:
        queued: reconcile catalogs/warehouse/analytics.outage deadline=0 attempts=0 next_deadline=-
      failures (newest first): []

    Exactly one task is claimed after recovery
      broker: broker:1
      tasks:
        running: reconcile catalogs/warehouse/analytics.outage deadline=0 attempts=1 next_deadline=-
      failures (newest first): []
    ");
}

#[tokio::test(start_paused = true)]
async fn dropping_all_handles_stops_upload_retries() {
    let mut trace = String::new();

    let clock = Arc::new(DefaultSystemClock::new());
    let store = Arc::new(FaultStore::default());
    let options = ObjectStoreSchedulerOptions {
        max_pending_requests: 1,
        ..Default::default()
    };

    let (broker, scheduler) = open(store.clone(), clock.clone(), options, "broker:1").await;
    store.fail_writes.store(true, Ordering::SeqCst);

    let request = tokio::spawn({
        let scheduler = scheduler.clone();
        async move { scheduler.enqueue(task("cancelled")).await }
    });

    tokio::task::yield_now().await;
    clock.sleep(Duration::from_secs(2)).await;

    record(
        &mut trace,
        "Before shutdown: upload failing, no task acknowledged",
        store.as_ref(),
    )
    .await;

    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    drop(scheduler);

    broker.await.unwrap().unwrap();
    tokio::task::yield_now().await;

    let writes = store.attempted_writes.load(Ordering::SeqCst);

    clock.sleep(Duration::from_secs(5)).await;

    assert_eq!(store.attempted_writes.load(Ordering::SeqCst), writes);
    record(
        &mut trace,
        "After shutdown: uploads stopped and unacknowledged task is absent",
        store.as_ref(),
    )
    .await;

    insta::assert_snapshot!(trace, @"
    Before shutdown: upload failing, no task acknowledged
      broker: broker:1
      tasks: []
      failures (newest first): []

    After shutdown: uploads stopped and unacknowledged task is absent
      broker: broker:1
      tasks: []
      failures (newest first): []
    ");
}

impl fmt::Display for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FaultStore")
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.attempted_writes.fetch_add(1, Ordering::SeqCst);
        if self.fail_writes.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "fault",
                source: "injected upload failure".into(),
            });
        }
        let result = self.inner.put_opts(path, payload, opts).await?;
        if self.lose_response.swap(false, Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "fault",
                source: "injected lost response".into(),
            });
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }

    async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, opts).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}
