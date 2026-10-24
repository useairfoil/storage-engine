mod common;

use std::{num::NonZeroUsize, sync::atomic::Ordering, time::Duration};

use common::{Environment, GatedSecrets};
use se_common::clock::SystemClock;
use se_scheduler::Task;
use se_worker::{WorkerPool, WorkerPoolOptions};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn bounds_concurrency_and_fills_free_slots() {
    let env = Environment::new().await;
    let (secrets, mut started) = GatedSecrets::new();
    for (catalog, table, deadline) in [
        ("alpha", "orders", 10),
        ("beta", "events", 20),
        ("gamma", "users", 30),
    ] {
        env.scheduler
            .enqueue(env.task(catalog, table, deadline))
            .await
            .unwrap();
    }
    let pool = WorkerPool::new(
        env.scheduler.clone(),
        env.reconciler(secrets.clone()),
        WorkerPoolOptions {
            concurrency: NonZeroUsize::new(2).unwrap(),
        },
    );
    let ct = CancellationToken::new();
    let pool = tokio::spawn(pool.run(ct.clone()));

    let mut running = vec![started.recv().await.unwrap(), started.recv().await.unwrap()];
    running.sort();
    insta::assert_debug_snapshot!(running, @r#"
    [
        "catalog/alpha",
        "catalog/beta",
    ]
    "#);
    // The third task waits for a free worker.
    env.clock.sleep(Duration::from_millis(200)).await;
    assert!(matches!(
        started.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert!(env.scheduler.failed_tasks(10).await.unwrap().is_empty());

    // Free only one slot: gamma must start without waiting for the other job.
    secrets.permits.add_permits(1);
    let next = tokio::select! {
        next = started.recv() => next.unwrap(),
        () = env.clock.sleep(Duration::from_secs(2)) => panic!("free slot was not filled"),
    };
    assert_eq!(next, "catalog/gamma");
    secrets.permits.add_permits(2);
    let failures = async {
        loop {
            let failed = env.scheduler.failed_tasks(10).await.unwrap();
            if failed.len() == 3 {
                break failed;
            }
            env.clock.sleep(Duration::from_millis(100)).await;
        }
    };
    let failures = tokio::select! {
        failures = failures => failures,
        () = env.clock.sleep(Duration::from_secs(20)) => panic!("workers did not finish"),
    };
    let mut outcomes = failures
        .into_iter()
        .map(|failed| {
            let Task::Reconcile {
                catalog_name,
                table_ident,
                ..
            } = failed.task;
            (
                catalog_name.to_string(),
                table_ident.to_string(),
                failed.attempts,
                failed.failure,
            )
        })
        .collect::<Vec<_>>();
    outcomes.sort_by(|left, right| left.0.cmp(&right.0));
    insta::assert_debug_snapshot!(outcomes, @r#"
    [
        (
            "catalogs/alpha",
            "analytics.orders",
            1,
            Permanent {
                reason: "catalog not found: catalogs/alpha",
            },
        ),
        (
            "catalogs/beta",
            "analytics.events",
            1,
            Permanent {
                reason: "catalog not found: catalogs/beta",
            },
        ),
        (
            "catalogs/gamma",
            "analytics.users",
            1,
            Permanent {
                reason: "catalog not found: catalogs/gamma",
            },
        ),
    ]
    "#);

    ct.cancel();
    pool.await.unwrap().unwrap();
    drop(env.scheduler);
    env.broker.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn reports_transient_failures_for_scheduler_retry() {
    let env = Environment::new().await;
    let (secrets, mut started) = GatedSecrets::new();
    secrets.fail_once.store(true, Ordering::SeqCst);
    secrets.permits.add_permits(1);
    env.scheduler
        .enqueue(env.task("warehouse", "orders", 0))
        .await
        .unwrap();
    let pool = WorkerPool::new(
        env.scheduler.clone(),
        env.reconciler(secrets.clone()),
        WorkerPoolOptions::default(),
    );
    let pool = tokio::spawn(pool.run(CancellationToken::new()));

    assert_eq!(started.recv().await.unwrap(), "catalog/warehouse");
    // The retry must come from the scheduler: the worker never re-enqueues.
    assert_eq!(started.recv().await.unwrap(), "catalog/warehouse");
    secrets.permits.add_permits(1);
    let failure = tokio::select! {
        failure = async {
            loop {
                if let Some(failure) = env.scheduler.failed_tasks(1).await.unwrap().pop() {
                    break failure;
                }
                env.clock.sleep(Duration::from_millis(100)).await;
            }
        } => failure,
        () = env.clock.sleep(Duration::from_secs(20)) => panic!("retry did not finish"),
    };
    insta::assert_debug_snapshot!((failure.attempts, failure.failure), @r#"
    (
        2,
        Permanent {
            reason: "catalog not found: catalogs/warehouse",
        },
    )
    "#);

    pool.abort();
    assert!(pool.await.unwrap_err().is_cancelled());
    drop(env.scheduler);
    env.broker.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn aborting_the_pool_leaves_unfinished_work_for_lease_recovery() {
    let env = Environment::new().await;
    let (secrets, mut started) = GatedSecrets::new();
    let task = env.task("warehouse", "orders", 0);
    let task_id = env.scheduler.enqueue(task.clone()).await.unwrap();
    let pool = WorkerPool::new(
        env.scheduler.clone(),
        env.reconciler(secrets),
        WorkerPoolOptions::default(),
    );
    let pool = tokio::spawn(pool.run(CancellationToken::new()));
    started.recv().await.unwrap();
    pool.abort();
    assert!(pool.await.unwrap_err().is_cancelled());

    // The worker must not report a completion after abort.
    env.clock.sleep(Duration::from_secs(5)).await;
    let recovered = env.scheduler.claim().await.unwrap().unwrap();
    assert_eq!(recovered.task_id, task_id);
    assert_eq!(recovered.task, task);
    assert!(env.scheduler.failed_tasks(1).await.unwrap().is_empty());
    drop(env.scheduler);
    env.broker.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn cancellation_stops_claiming_and_drains_running_jobs() {
    let env = Environment::new().await;
    let (secrets, mut started) = GatedSecrets::new();
    let first_id = env
        .scheduler
        .enqueue(env.task("first", "orders", 10))
        .await
        .unwrap();
    let second_task = env.task("second", "events", 20);
    let second_id = env.scheduler.enqueue(second_task.clone()).await.unwrap();
    let pool = WorkerPool::new(
        env.scheduler.clone(),
        env.reconciler(secrets.clone()),
        WorkerPoolOptions {
            concurrency: NonZeroUsize::MIN,
        },
    );
    let ct = CancellationToken::new();
    let pool = tokio::spawn(pool.run(ct.clone()));
    assert_eq!(started.recv().await.unwrap(), "catalog/first");

    ct.cancel();
    tokio::task::yield_now().await;
    assert!(!pool.is_finished());
    secrets.permits.add_permits(1);
    tokio::select! {
        result = pool => result.unwrap().unwrap(),
        () = env.clock.sleep(Duration::from_secs(2)) => panic!("running job did not drain"),
    }

    let failures = env.scheduler.failed_tasks(10).await.unwrap();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].task_id, first_id);
    let unclaimed = env.scheduler.claim().await.unwrap().unwrap();
    assert_eq!(unclaimed.task_id, second_id);
    assert_eq!(unclaimed.task, second_task);
    drop(env.scheduler);
    env.broker.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn returns_scheduler_errors_to_the_supervisor() {
    let env = Environment::new().await;
    let (secrets, mut started) = GatedSecrets::new();
    env.scheduler
        .enqueue(env.task("warehouse", "orders", 0))
        .await
        .unwrap();
    let pool = WorkerPool::new(
        env.scheduler.clone(),
        env.reconciler(secrets),
        WorkerPoolOptions {
            concurrency: NonZeroUsize::new(2).unwrap(),
        },
    );
    let pool = tokio::spawn(pool.run(CancellationToken::new()));
    started.recv().await.unwrap();
    env.broker.abort();
    assert!(env.broker.await.unwrap_err().is_cancelled());
    let result = tokio::select! {
        result = pool => result.unwrap(),
        () = env.clock.sleep(Duration::from_secs(10)) => panic!("pool did not stop"),
    };
    insta::assert_debug_snapshot!(result, @"
    Err(
        Scheduler(
            Closed,
        ),
    )
    ");
}
