use std::{sync::Arc, time::Duration};

use object_store::{memory::InMemory, path::Path};
use se_common::clock::DefaultSystemClock;
use se_ingestion::Ingestor;
use se_meta_store::catalog::CatalogStore;
use se_scheduler::{
    ClaimedTask, TaskScheduler,
    object_store::{ObjectStoreBroker, ObjectStoreSchedulerOptions},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

pub async fn claim_task(scheduler: &dyn TaskScheduler) -> ClaimedTask {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(task) = scheduler.claim().await.unwrap() {
                return task;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

pub async fn test_ingestor(
    store: CatalogStore,
) -> (
    Ingestor,
    Arc<dyn TaskScheduler>,
    AbortOnDropHandle<se_scheduler::Result<()>>,
) {
    let clock = Arc::new(DefaultSystemClock::new());
    let (broker, scheduler) = ObjectStoreBroker::new(
        Arc::new(InMemory::new()),
        Path::from("queue.json"),
        "test-broker".into(),
        ObjectStoreSchedulerOptions {
            flush_interval: Duration::from_millis(1),
            ..Default::default()
        },
        clock.clone(),
    )
    .await
    .unwrap()
    .run(CancellationToken::new());
    (
        Ingestor::new(store, clock, scheduler.clone()),
        scheduler,
        AbortOnDropHandle::new(broker),
    )
}
