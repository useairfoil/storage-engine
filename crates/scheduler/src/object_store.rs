//! A single-object queue with an in-memory broker and CAS-protected group commits.
//!
//! Constructing a broker explicitly takes over ownership. Discovery clients must use
//! [`ObjectStoreBroker::read_broker`] instead of constructing another broker.
//! The store must support conditional puts and expose ETags or object versions.
//! Tasks coalesce by type, catalog, and table. Execution is at least once, so
//! reconciliation workers must still tolerate duplicate work.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    num::NonZeroU32,
    ops::ControlFlow,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, UpdateVersion, path::Path};
use rand::RngExt;
use se_common::{clock::SystemClock, id::IdGenerator};
use se_meta_store::catalog::{CatalogName, TableIdent};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

use crate::{
    AttemptId, ClaimedTask, Error, FailedTask, Result, RetryPolicy, Task, TaskFailure, TaskId,
    TaskOutcome, TaskScheduler,
};

pub const DEFAULT_LOCATION: &str = "queue.json";

/// The broker advertised in the queue object. Discovery is advisory: a client
/// must rediscover after connection failures or a broker's rejection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub id: Uuid,
    pub address: String,
}

/// Broker configuration. All durations must be positive, and the initial retry
/// backoff must not exceed the maximum backoff.
#[derive(Debug, Clone)]
pub struct ObjectStoreSchedulerOptions {
    pub flush_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub retry_policy: RetryPolicy,
    /// Maximum number of terminal failures retained in the snapshot.
    pub max_failed_tasks: usize,
    /// Bounds both queued requests and pending acknowledgments during outages.
    pub max_pending_requests: usize,
}

/// An acquired queue broker. Call [`Self::run`] to start processing requests.
/// Constructing it does not spawn a background task.
pub struct ObjectStoreBroker {
    store: Arc<dyn ObjectStore>,
    location: Path,
    clock: Arc<dyn SystemClock>,
    options: ObjectStoreSchedulerOptions,
    snapshot: Snapshot,
    tasks: BTreeMap<TaskId, QueuedTask>,
    targets: HashMap<TaskKey, TaskId>,
    ready: BTreeSet<(DateTime<Utc>, TaskId)>,
    timers: BTreeSet<(DateTime<Utc>, TaskId)>,
    version: UpdateVersion,
    pending: Vec<PendingReply>,
}

#[derive(Debug)]
struct ObjectStoreTaskScheduler {
    commands: mpsc::Sender<Command>,
    // Closing this channel cancels even a blocked or indefinitely retrying upload.
    _lifetime: watch::Sender<()>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Snapshot {
    broker: BrokerInfo,
    commit_id: Uuid,
    updated_at: DateTime<Utc>,
    tasks: VecDeque<QueuedTask>,
    failed_tasks: VecDeque<FailedTask>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueuedTask {
    id: TaskId,
    task: Task,
    attempts: u32,
    next_attempt_at: DateTime<Utc>,
    attempt: Option<Attempt>,
    last_failure: Option<TaskFailure>,
    next_deadline: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Attempt {
    id: AttemptId,
    last_heartbeat: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum TaskKey {
    Reconcile(CatalogName, TableIdent),
}

#[derive(Debug)]
struct Command {
    operation: Operation,
    reply: oneshot::Sender<Result<Response>>,
}

#[derive(Debug)]
enum Operation {
    Enqueue(Task),
    Claim,
    Heartbeat(TaskId, AttemptId),
    Complete(TaskId, AttemptId, TaskOutcome),
    FailedTasks(usize),
    Requeue(TaskId),
}

#[derive(Debug)]
enum Response {
    TaskId(TaskId),
    Claim(Option<ClaimedTask>),
    Done,
    FailedTasks(Vec<FailedTask>),
}

struct PendingReply {
    reply: oneshot::Sender<Result<Response>>,
    response: Response,
}

impl Default for ObjectStoreSchedulerOptions {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_secs(1),
            heartbeat_timeout: Duration::from_secs(30),
            retry_policy: RetryPolicy {
                // PANIC: 5 is nonzero.
                max_attempts: NonZeroU32::new(5).unwrap(),
                initial_backoff: Duration::from_secs(1),
                max_backoff: Duration::from_secs(60),
            },
            max_failed_tasks: 1_000,
            max_pending_requests: 1_024,
        }
    }
}

impl ObjectStoreBroker {
    /// Takes over the queue using CAS, preserving its durable tasks and claims.
    /// Concurrent takeover attempts may return [`Error::NotBroker`]; callers must
    /// rediscover rather than repeatedly opening writers against a healthy broker.
    ///
    /// Ownership is published before this returns. Call [`Self::run`] promptly
    /// and serve its scheduler client at the advertised endpoint.
    pub async fn new(
        store: Arc<dyn ObjectStore>,
        location: Path,
        broker_address: String,
        options: ObjectStoreSchedulerOptions,
        clock: Arc<dyn SystemClock>,
    ) -> Result<Self> {
        options.validate()?;
        let loaded = load(store.as_ref(), &location).await?;
        let broker = BrokerInfo {
            id: rand::rng().gen_uuid(),
            address: broker_address,
        };
        let (mut snapshot, version) = match loaded {
            Some((snapshot, version)) => (snapshot, Some(version)),
            None => (
                Snapshot {
                    broker: broker.clone(),
                    commit_id: rand::rng().gen_uuid(),
                    updated_at: clock.now(),
                    tasks: VecDeque::new(),
                    failed_tasks: VecDeque::new(),
                },
                None,
            ),
        };
        snapshot.broker = broker;
        snapshot.commit_id = rand::rng().gen_uuid();
        snapshot.updated_at = clock.now();
        snapshot.failed_tasks.truncate(options.max_failed_tasks);
        let payload = serde_json::to_vec(&snapshot).map_err(box_error)?;
        let mode = version.map_or(PutMode::Create, PutMode::Update);
        let result = store
            .put_opts(&location, payload.into(), mode.into())
            .await
            .map_err(|error| match error {
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. } => Error::NotBroker,
                error => box_error(error),
            })?;
        let version = checked_version(result.e_tag, result.version)?;
        let tasks: BTreeMap<_, _> = std::mem::take(&mut snapshot.tasks)
            .into_iter()
            .map(|task| (task.id, task))
            .collect();
        let targets = tasks
            .values()
            .map(|task| (task.task.key(), task.id))
            .collect();
        let mut broker = Self {
            store,
            location,
            clock,
            options,
            snapshot,
            tasks,
            targets,
            ready: BTreeSet::new(),
            timers: BTreeSet::new(),
            version,
            pending: Vec::new(),
        };
        for id in broker.tasks.keys().copied().collect::<Vec<_>>() {
            broker.index_task(id);
        }
        Ok(broker)
    }

    /// Starts the broker and returns its owned task handle and shared client.
    ///
    /// The task returns [`Error::NotBroker`] on ownership loss and `Ok(())` when
    /// cancelled or all clients are dropped. Cancellation stops pending uploads
    /// without draining requests; unacknowledged calls return [`Error::Closed`].
    /// Aborting the handle also stops the broker and closes its clients.
    /// Dropping the handle alone does not stop the task.
    pub fn run(self, ct: CancellationToken) -> (JoinHandle<Result<()>>, Arc<dyn TaskScheduler>) {
        let (commands, receiver) = mpsc::channel(self.options.max_pending_requests);
        let (lifetime, lifetime_receiver) = watch::channel(());
        let scheduler = Arc::new(ObjectStoreTaskScheduler {
            commands,
            _lifetime: lifetime,
        });
        let handle = tokio::spawn(self.run_loop(receiver, lifetime_receiver, ct));
        (handle, scheduler)
    }

    /// Reads discovery information with a GET; does not acquire writer ownership.
    pub async fn read_broker(
        store: &dyn ObjectStore,
        location: &Path,
    ) -> Result<Option<BrokerInfo>> {
        Ok(load(store, location)
            .await?
            .map(|(snapshot, _)| snapshot.broker))
    }
}

impl ObjectStoreTaskScheduler {
    async fn request(&self, operation: Operation) -> Result<Response> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command { operation, reply })
            .await
            .map_err(|_| Error::Closed)?;
        response.await.map_err(|_| Error::Closed)?
    }
}

#[async_trait]
impl TaskScheduler for ObjectStoreTaskScheduler {
    async fn enqueue(&self, task: Task) -> Result<TaskId> {
        match self.request(Operation::Enqueue(task)).await? {
            Response::TaskId(id) => Ok(id),
            // PANIC: enqueue always produces a task ID.
            _ => unreachable!("enqueue response"),
        }
    }

    async fn claim(&self) -> Result<Option<ClaimedTask>> {
        match self.request(Operation::Claim).await? {
            Response::Claim(task) => Ok(task),
            // PANIC: claim always produces a claim response.
            _ => unreachable!("claim response"),
        }
    }

    async fn heartbeat(&self, task_id: TaskId, attempt_id: AttemptId) -> Result<()> {
        self.request(Operation::Heartbeat(task_id, attempt_id))
            .await?;
        Ok(())
    }

    async fn complete(
        &self,
        task_id: TaskId,
        attempt_id: AttemptId,
        outcome: TaskOutcome,
    ) -> Result<()> {
        self.request(Operation::Complete(task_id, attempt_id, outcome))
            .await?;
        Ok(())
    }

    async fn failed_tasks(&self, limit: usize) -> Result<Vec<FailedTask>> {
        match self.request(Operation::FailedTasks(limit)).await? {
            Response::FailedTasks(tasks) => Ok(tasks),
            // PANIC: failed_tasks always produces failure records.
            _ => unreachable!("failed_tasks response"),
        }
    }

    async fn requeue(&self, task_id: TaskId) -> Result<TaskId> {
        match self.request(Operation::Requeue(task_id)).await? {
            Response::TaskId(id) => Ok(id),
            // PANIC: requeue always produces a task ID.
            _ => unreachable!("requeue response"),
        }
    }
}

impl Task {
    fn key(&self) -> TaskKey {
        match self {
            Self::Reconcile {
                catalog_name,
                table_ident,
                ..
            } => TaskKey::Reconcile(catalog_name.clone(), table_ident.clone()),
        }
    }

    fn deadline(&self) -> DateTime<Utc> {
        match self {
            Self::Reconcile { deadline, .. } => *deadline,
        }
    }
}

fn box_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Generic(Box::new(error))
}

fn checked_version(e_tag: Option<String>, version: Option<String>) -> Result<UpdateVersion> {
    if e_tag.is_none() && version.is_none() {
        return Err(Error::InvalidConfiguration(
            "object store must expose ETags or object versions".into(),
        ));
    }
    Ok(UpdateVersion { e_tag, version })
}

async fn load(
    store: &dyn ObjectStore,
    location: &Path,
) -> Result<Option<(Snapshot, UpdateVersion)>> {
    let response = match store.get(location).await {
        Ok(response) => response,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(box_error(error)),
    };
    let version = checked_version(response.meta.e_tag.clone(), response.meta.version.clone())?;
    let bytes = response.bytes().await.map_err(box_error)?;
    let snapshot = serde_json::from_slice(&bytes).map_err(box_error)?;
    Ok(Some((snapshot, version)))
}

/// Retries the *same* frozen batch on transport failures. Reading after a failed
/// PUT resolves a successful upload whose response was lost without acknowledging
/// later in-memory mutations. A changed object belonging to another commit fences us.
async fn persist(
    store: &dyn ObjectStore,
    location: &Path,
    snapshot: &Snapshot,
    version: &UpdateVersion,
    clock: &dyn SystemClock,
    retry_interval: Duration,
) -> Result<UpdateVersion> {
    let payload = serde_json::to_vec(snapshot).map_err(box_error)?;
    loop {
        match store
            .put_opts(
                location,
                payload.clone().into(),
                PutMode::Update(version.clone()).into(),
            )
            .await
        {
            Ok(result) => return checked_version(result.e_tag, result.version),
            Err(error) => {
                warn!(%error, "queue snapshot write failed; checking durable state");
                match load(store, location).await {
                    Ok(Some((durable, current_version))) => {
                        if durable.broker == snapshot.broker
                            && durable.commit_id == snapshot.commit_id
                        {
                            return Ok(current_version);
                        }
                        if &current_version != version || durable.broker != snapshot.broker {
                            return Err(Error::NotBroker);
                        }
                    }
                    Ok(None) => return Err(Error::NotBroker),
                    Err(error) => warn!(%error, "could not check durable queue state"),
                }
                clock.sleep(retry_interval).await;
            }
        }
    }
}

impl ObjectStoreSchedulerOptions {
    fn validate(&self) -> Result<()> {
        if self.flush_interval.is_zero()
            || self.heartbeat_timeout.is_zero()
            || self.retry_policy.initial_backoff.is_zero()
            || self.retry_policy.initial_backoff > self.retry_policy.max_backoff
            || self.max_pending_requests == 0
            || chrono::Duration::from_std(self.flush_interval).is_err()
            || chrono::Duration::from_std(self.heartbeat_timeout).is_err()
            || chrono::Duration::from_std(self.retry_policy.max_backoff).is_err()
        {
            return Err(Error::InvalidConfiguration(
                "durations and request capacity must be positive and representable; initial_backoff must not exceed max_backoff".into(),
            ));
        }
        Ok(())
    }
}

impl ObjectStoreBroker {
    async fn run_loop(
        mut self,
        mut commands: mpsc::Receiver<Command>,
        mut lifetime: watch::Receiver<()>,
        ct: CancellationToken,
    ) -> Result<()> {
        let mut next_flush = self.clock.now() + self.options.flush_interval;
        loop {
            if self
                .tick(&mut commands, &mut lifetime, &ct, &mut next_flush)
                .await?
                .is_break()
            {
                return Ok(());
            }
        }
    }

    #[tracing::instrument(name = "scheduler.tick", skip_all)]
    async fn tick(
        &mut self,
        commands: &mut mpsc::Receiver<Command>,
        lifetime: &mut watch::Receiver<()>,
        ct: &CancellationToken,
        next_flush: &mut DateTime<Utc>,
    ) -> Result<ControlFlow<()>> {
        let next_wake = self
            .timers
            .first()
            .map_or(*next_flush, |(at, _)| (*at).min(*next_flush));

        let wait = (next_wake - self.clock.now()).to_std().unwrap_or_default();

        tokio::select! {
            () = ct.cancelled() => return Ok(ControlFlow::Break(())),
            _ = lifetime.changed() => return Ok(ControlFlow::Break(())),
            command = commands.recv(), if self.pending.len() < self.options.max_pending_requests => {
                match command {
                    Some(command) => self.apply(command),
                    None => return Ok(ControlFlow::Break(())),
                }
                return Ok(ControlFlow::Continue(()));
            }
            () = self.clock.sleep(wait) => {}
        }

        self.expire_timers();
        if self.clock.now() < *next_flush {
            return Ok(ControlFlow::Continue(()));
        }

        self.snapshot.commit_id = rand::rng().gen_uuid();
        self.snapshot.updated_at = self.clock.now();

        let mut frozen = self.snapshot.clone();
        frozen.tasks = self.tasks.values().cloned().collect();
        let batch = std::mem::take(&mut self.pending);
        let store = self.store.clone();
        let location = self.location.clone();
        let clock = self.clock.clone();
        let version = self.version.clone();
        *next_flush = self.clock.now() + self.options.flush_interval;
        let write = persist(
            store.as_ref(),
            &location,
            &frozen,
            &version,
            clock.as_ref(),
            self.options.flush_interval,
        );

        tokio::pin!(write);

        let result = loop {
            let timer_wait = self
                .timers
                .first()
                .map_or(self.options.flush_interval, |(at, _)| {
                    (*at - self.clock.now()).to_std().unwrap_or_default()
                });

            tokio::select! {
                () = ct.cancelled() => return Ok(ControlFlow::Break(())),
                _ = lifetime.changed() => return Ok(ControlFlow::Break(())),
                result = &mut write => break result,
                () = self.clock.sleep(timer_wait), if !self.timers.is_empty() => self.expire_timers(),
                command = commands.recv(), if self.pending.len() + batch.len() < self.options.max_pending_requests => {
                    match command {
                        Some(command) => self.apply(command),
                        None => return Ok(ControlFlow::Break(())),
                    }
                }
            }
        };

        match result {
            Ok(version) => {
                self.version = version;
                for pending in batch {
                    let _ = pending.reply.send(Ok(pending.response));
                }
            }
            Err(error) => {
                warn!(%error, "stopping queue broker");
                for pending in batch.into_iter().chain(std::mem::take(&mut self.pending)) {
                    let error = if matches!(error, Error::NotBroker) {
                        Error::NotBroker
                    } else {
                        Error::Closed
                    };
                    let _ = pending.reply.send(Err(error));
                }
                return Err(error);
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    fn apply(&mut self, command: Command) {
        self.expire_timers();
        match self.execute(command.operation) {
            Ok(response) => self.pending.push(PendingReply {
                reply: command.reply,
                response,
            }),
            Err(error) => {
                let _ = command.reply.send(Err(error));
            }
        }
    }

    fn execute(&mut self, operation: Operation) -> Result<Response> {
        let now = self.clock.now();
        match operation {
            Operation::Enqueue(task) => Ok(Response::TaskId(self.enqueue(task))),
            Operation::Claim => {
                let claim = self.ready.pop_first().map(|(_, id)| {
                    // PANIC: the ready index only contains existing, unclaimed tasks.
                    let task = self.tasks.get_mut(&id).unwrap();
                    let attempt_id = rand::rng().gen_uuid();
                    task.attempts += 1;
                    task.attempt = Some(Attempt {
                        id: attempt_id,
                        last_heartbeat: now,
                    });
                    let claim = ClaimedTask {
                        task_id: task.id,
                        attempt_id,
                        task: task.task.clone(),
                    };
                    self.index_task(id);
                    claim
                });
                Ok(Response::Claim(claim))
            }
            Operation::Heartbeat(task_id, attempt_id) => {
                self.active_task(task_id, attempt_id)?;
                self.unindex_task(task_id);
                let task = self.active_task(task_id, attempt_id)?;
                if let Some(attempt) = &mut task.attempt {
                    attempt.last_heartbeat = now;
                }
                self.index_task(task_id);
                Ok(Response::Done)
            }
            Operation::Complete(task_id, attempt_id, outcome) => {
                self.active_task(task_id, attempt_id)?;
                match outcome {
                    TaskOutcome::Success => self.finish(task_id, None),
                    TaskOutcome::Failure(failure) => self.fail(task_id, failure),
                }
                Ok(Response::Done)
            }
            Operation::FailedTasks(limit) => Ok(Response::FailedTasks(
                self.snapshot
                    .failed_tasks
                    .iter()
                    .take(limit)
                    .cloned()
                    .collect(),
            )),
            Operation::Requeue(id) => {
                let task = self
                    .snapshot
                    .failed_tasks
                    .iter()
                    .find(|task| task.task_id == id)
                    .ok_or(Error::TaskNotFound(id))?
                    .task
                    .clone();
                Ok(Response::TaskId(self.enqueue(task)))
            }
        }
    }

    fn enqueue(&mut self, task: Task) -> TaskId {
        let key = task.key();
        if let Some(&id) = self.targets.get(&key) {
            self.unindex_task(id);
            // PANIC: the target index only contains existing tasks.
            self.tasks.get_mut(&id).unwrap().merge(&task);
            self.index_task(id);
            return id;
        }
        let id = rand::rng().gen_uuid();
        self.tasks.insert(
            id,
            QueuedTask {
                id,
                task,
                attempts: 0,
                next_attempt_at: self.clock.now(),
                attempt: None,
                last_failure: None,
                next_deadline: None,
            },
        );
        self.targets.insert(key, id);
        self.index_task(id);
        id
    }

    fn active_task(&mut self, task_id: TaskId, attempt_id: AttemptId) -> Result<&mut QueuedTask> {
        let task = self
            .tasks
            .get_mut(&task_id)
            .ok_or(Error::TaskNotFound(task_id))?;
        if task.attempt.as_ref().map(|attempt| attempt.id) != Some(attempt_id) {
            return Err(Error::StaleAttempt {
                task_id,
                attempt_id,
            });
        }
        Ok(task)
    }

    fn index_task(&mut self, id: TaskId) {
        // PANIC: callers inserted or looked up this task before indexing it.
        let task = self.tasks.get(&id).unwrap();
        if let Some(attempt) = &task.attempt {
            self.timers
                .insert((attempt.last_heartbeat + self.options.heartbeat_timeout, id));
        } else if task.next_attempt_at > self.clock.now() {
            self.timers.insert((task.next_attempt_at, id));
        } else {
            self.ready.insert((task.task.deadline(), id));
        }
    }

    fn unindex_task(&mut self, id: TaskId) {
        // PANIC: callers looked up this task before removing its index entries.
        let task = self.tasks.get(&id).unwrap();
        self.ready.remove(&(task.task.deadline(), id));
        let at = task
            .attempt
            .as_ref()
            .map_or(task.next_attempt_at, |attempt| {
                attempt.last_heartbeat + self.options.heartbeat_timeout
            });
        self.timers.remove(&(at, id));
    }

    fn expire_timers(&mut self) {
        let now = self.clock.now();
        while let Some(&(at, id)) = self.timers.first() {
            if at > now {
                break;
            }
            self.timers.pop_first();
            // PANIC: the timer index only contains existing tasks.
            if self.tasks.get(&id).unwrap().attempt.is_some() {
                self.fail(
                    id,
                    TaskFailure::Retryable {
                        reason: "worker heartbeat expired".into(),
                    },
                );
            } else {
                self.index_task(id);
            }
        }
    }

    fn fail(&mut self, id: TaskId, failure: TaskFailure) {
        self.unindex_task(id);
        let now = self.clock.now();
        // PANIC: callers verified that this task exists.
        let task = self.tasks.get_mut(&id).unwrap();
        if matches!(failure, TaskFailure::Retryable { .. })
            && task.attempts < self.options.retry_policy.max_attempts.get()
        {
            let policy = &self.options.retry_policy;
            let mut cap = policy.initial_backoff;
            for _ in 1..task.attempts {
                cap = cap.saturating_mul(2).min(policy.max_backoff);
                if cap == policy.max_backoff {
                    break;
                }
            }
            // Equal jitter: retry in [cap / 2, cap], never exceeding the configured cap.
            let delay = cap.mul_f64(rand::rng().random_range(0.5..=1.0));
            task.next_attempt_at = now + delay;
            task.attempt = None;
            task.last_failure = Some(failure);
            self.index_task(id);
        } else {
            self.finish(id, Some(failure));
        }
    }

    fn finish(&mut self, id: TaskId, failure: Option<TaskFailure>) {
        self.unindex_task(id);
        // PANIC: callers verified that this task exists.
        let mut task = self.tasks.remove(&id).unwrap();
        let now = self.clock.now();
        if let Some(failure) = failure {
            warn!(task_id = %task.id, ?failure, "task failed permanently or exhausted its attempts");
            self.snapshot.failed_tasks.push_front(FailedTask {
                task_id: task.id,
                task: task.task.clone(),
                attempts: task.attempts,
                failure,
                failed_at: now,
            });
            self.snapshot
                .failed_tasks
                .truncate(self.options.max_failed_tasks);
        }
        if let Some(next_deadline) = task.next_deadline.take() {
            match &mut task.task {
                Task::Reconcile { deadline, .. } => *deadline = next_deadline,
            }
            task.attempts = 0;
            task.next_attempt_at = now;
            task.attempt = None;
            task.last_failure = None;
            self.tasks.insert(id, task);
            self.index_task(id);
        } else {
            self.targets.remove(&task.task.key());
        }
    }
}

impl QueuedTask {
    fn merge(&mut self, incoming: &Task) {
        match (&mut self.task, incoming) {
            (
                Task::Reconcile { deadline, .. },
                Task::Reconcile {
                    deadline: incoming_deadline,
                    ..
                },
            ) => {
                let deadline = if self.attempt.is_some() || self.next_deadline.is_some() {
                    self.next_deadline.get_or_insert(*incoming_deadline)
                } else {
                    deadline
                };
                *deadline = (*deadline).min(*incoming_deadline);
            }
        }
    }
}
