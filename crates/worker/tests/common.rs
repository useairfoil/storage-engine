use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use object_store::{memory::InMemory, path::Path};
use se_common::clock::{DefaultSystemClock, SystemClock};
use se_meta_store::catalog::{CatalogName, CatalogStore, NamespaceIdent, TableIdent};
use se_reconciliation::Reconciler;
use se_scheduler::{
    Task, TaskScheduler,
    object_store::{ObjectStoreBroker, ObjectStoreSchedulerOptions},
};
use se_secret_store::{Secret, SecretName, SecretStore};
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinHandle,
};

pub struct Environment {
    pub clock: Arc<DefaultSystemClock>,
    pub store: Arc<InMemory>,
    pub scheduler: Arc<dyn TaskScheduler>,
    pub broker: JoinHandle<se_scheduler::Result<()>>,
}

#[derive(Debug)]
pub struct GatedSecrets {
    pub permits: Semaphore,
    pub fail_once: AtomicBool,
    started: mpsc::UnboundedSender<String>,
}

impl Environment {
    pub async fn new() -> Self {
        let clock = Arc::new(DefaultSystemClock::new());
        let store = Arc::new(InMemory::new());
        let (broker, scheduler) = ObjectStoreBroker::new(
            store.clone(),
            Path::from("queue.json"),
            "broker:1".into(),
            ObjectStoreSchedulerOptions {
                flush_interval: Duration::from_millis(100),
                heartbeat_timeout: Duration::from_secs(3),
                ..Default::default()
            },
            clock.clone(),
        )
        .await
        .unwrap()
        .run();
        Self {
            clock,
            store,
            scheduler,
            broker,
        }
    }

    pub fn reconciler(&self, secrets: Arc<dyn SecretStore>) -> Reconciler {
        Reconciler::new(
            CatalogStore::new(secrets, self.store.clone()),
            self.clock.clone(),
        )
    }

    pub fn task(&self, catalog: &str, table: &str, deadline: u64) -> Task {
        Task::Reconcile {
            catalog_name: CatalogName::new(catalog).unwrap(),
            table_ident: TableIdent::new(NamespaceIdent::new("analytics".into()), table.into()),
            deadline: self.clock.now() + Duration::from_secs(deadline),
        }
    }
}

impl GatedSecrets {
    pub fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<String>) {
        let (started, receiver) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                permits: Semaphore::new(0),
                fail_once: AtomicBool::new(false),
                started,
            }),
            receiver,
        )
    }
}

#[async_trait]
impl SecretStore for GatedSecrets {
    async fn get(&self, name: &SecretName) -> se_secret_store::Result<Secret> {
        self.started.send(name.to_string()).unwrap();
        self.permits.acquire().await.unwrap().forget();
        if self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(se_secret_store::Error::Generic {
                store: "test",
                source: "temporarily unavailable".into(),
            });
        }
        Err(se_secret_store::Error::NotFound(name.clone()))
    }

    async fn put(&self, _name: &SecretName, _value: String) -> se_secret_store::Result<()> {
        // PANIC: reconciliation must not write catalog credentials.
        unreachable!("workers only read catalog credentials")
    }

    async fn delete(&self, _name: &SecretName) -> se_secret_store::Result<()> {
        // PANIC: reconciliation must not delete catalog credentials.
        unreachable!("workers only read catalog credentials")
    }
}
