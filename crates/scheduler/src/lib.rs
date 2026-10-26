//! Scheduling of table reconciliations.

pub mod object_store;

use std::{fmt, num::NonZeroU32, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use se_meta_store::catalog::{CatalogName, TableIdent};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The error type for scheduler operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("scheduler is no longer the broker")]
    NotBroker,
    #[error("scheduler is closed")]
    Closed,
    #[error("invalid scheduler configuration: {0}")]
    InvalidConfiguration(String),
    /// The task is not known to the scheduler.
    #[error("task not found: {0}")]
    TaskNotFound(TaskId),
    /// The report belongs to an attempt that is no longer active.
    #[error("attempt {attempt_id} is not active for task {task_id}")]
    StaleAttempt {
        task_id: TaskId,
        attempt_id: AttemptId,
    },
    /// An error from the scheduler implementation.
    #[error("scheduler error: {0}")]
    Generic(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Identifies a queued task.
pub type TaskId = Uuid;

/// Identifies one execution attempt of a task. Retries receive a new identifier.
pub type AttemptId = Uuid;

/// Work to be performed by a scheduler worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Task {
    /// Reconcile a table, targeting completion by the given deadline.
    ///
    /// The deadline is a freshness target, not an expiry: overdue work remains
    /// eligible for execution and retries.
    Reconcile {
        catalog_name: CatalogName,
        table_ident: TableIdent,
        deadline: DateTime<Utc>,
    },
}

/// A durably claimed task. Heartbeats and completion must use this attempt ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedTask {
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
    pub task: Task,
}

/// The outcome reported by a worker, independently of scheduler errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutcome {
    Success,
    Failure(TaskFailure),
}

/// A worker's classification of an execution failure.
///
/// Reasons are diagnostic text, not inputs to scheduling decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskFailure {
    Retryable { reason: String },
    Permanent { reason: String },
}

/// Retry limits applied by the scheduler, not by workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Maximum execution attempts, including the initial execution.
    pub max_attempts: NonZeroU32,
    /// Initial exponential-backoff delay before jitter is applied.
    pub initial_backoff: Duration,
    /// Upper bound on retry delays, including jitter.
    pub max_backoff: Duration,
}

/// A terminal failure retained for inspection and explicit requeueing.
///
/// A permanent failure terminates immediately. A retryable failure terminates
/// when the attempt limit is exhausted. A terminal failure must not prevent
/// new reconciliation tasks for the same table from being enqueued.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailedTask {
    pub task_id: TaskId,
    pub task: Task,
    /// Number of execution attempts, including attempts lost to heartbeat expiry.
    pub attempts: u32,
    pub failure: TaskFailure,
    pub failed_at: DateTime<Utc>,
}

/// A scheduler that discards tasks and never claims work or retains failures.
///
/// Unlike durable schedulers, successful calls do not persist anything.
/// Enqueue returns the nil task ID; requeue returns the supplied ID unchanged.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopScheduler;

/// Queues tasks and receives worker progress and completion reports.
///
/// Successful calls acknowledge durable persistence, not just an in-memory update.
/// Retryable failures and expired heartbeats consume an attempt and are retried
/// with capped exponential backoff and jitter, up to the configured retry limit.
/// Failure reports and their retry or terminal-state decisions persist atomically.
/// Implementations must bound terminal failure retention by count and/or age.
#[async_trait]
pub trait TaskScheduler: fmt::Debug + Send + Sync + 'static {
    /// Durably queues a task, coalescing by task type, catalog, and table.
    ///
    /// Queued tasks keep the earliest deadline. Enqueues for running tasks keep
    /// the earliest follow-up deadline without changing the active attempt.
    /// Coalesced enqueues return the existing task ID. Follow-ups retain that ID
    /// and become immediately eligible with a fresh attempt budget after success
    /// or terminal failure; retryable failures retain the normal backoff policy.
    async fn enqueue(&self, task: Task) -> Result<TaskId>;

    /// Durably claims the eligible task with the earliest deadline, or returns
    /// `None` if none is ready. Equal deadlines have no guaranteed ordering.
    async fn claim(&self) -> Result<Option<ClaimedTask>>;

    /// Records that the worker executing the task is still alive.
    ///
    /// Rejects stale attempts without changing the current attempt.
    async fn heartbeat(&self, task_id: TaskId, attempt_id: AttemptId) -> Result<()>;

    /// Records an attempt's outcome and schedules a retry or terminal failure.
    ///
    /// Rejects stale attempts without changing the current attempt. Workers must
    /// report failures here rather than independently enqueueing retries.
    async fn complete(
        &self,
        task_id: TaskId,
        attempt_id: AttemptId,
        outcome: TaskOutcome,
    ) -> Result<()>;

    /// Lists retained terminal failures, newest first, up to the given limit.
    async fn failed_tasks(&self, limit: usize) -> Result<Vec<FailedTask>>;

    /// Queues a retained failed task, coalescing with any outstanding task for
    /// the same target. A newly created task receives a fresh attempt budget.
    ///
    /// Preserves the original failure record. Returns [`Error::TaskNotFound`]
    /// if the terminal failure is unknown or has expired from retention.
    async fn requeue(&self, task_id: TaskId) -> Result<TaskId>;
}

#[async_trait]
impl TaskScheduler for NoopScheduler {
    async fn enqueue(&self, _task: Task) -> Result<TaskId> {
        Ok(TaskId::nil())
    }

    async fn claim(&self) -> Result<Option<ClaimedTask>> {
        Ok(None)
    }

    async fn heartbeat(&self, _task_id: TaskId, _attempt_id: AttemptId) -> Result<()> {
        Ok(())
    }

    async fn complete(
        &self,
        _task_id: TaskId,
        _attempt_id: AttemptId,
        _outcome: TaskOutcome,
    ) -> Result<()> {
        Ok(())
    }

    async fn failed_tasks(&self, _limit: usize) -> Result<Vec<FailedTask>> {
        Ok(Vec::new())
    }

    async fn requeue(&self, task_id: TaskId) -> Result<TaskId> {
        Ok(task_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_scheduler_discards_all_work() {
        let scheduler = NoopScheduler;
        let task = Task::Reconcile {
            catalog_name: CatalogName::new("example").unwrap(),
            table_ident: TableIdent::from_strs(["analytics", "events"]).unwrap(),
            deadline: DateTime::UNIX_EPOCH,
        };
        assert_eq!(scheduler.enqueue(task).await.unwrap(), TaskId::nil());
        let task_id = TaskId::from_u128(42);
        let attempt_id = AttemptId::from_u128(17);
        scheduler.heartbeat(task_id, attempt_id).await.unwrap();
        scheduler
            .complete(
                task_id,
                attempt_id,
                TaskOutcome::Failure(TaskFailure::Permanent {
                    reason: "ignored".into(),
                }),
            )
            .await
            .unwrap();
        assert_eq!(scheduler.requeue(task_id).await.unwrap(), task_id);
        assert_eq!(scheduler.claim().await.unwrap(), None);
        assert_eq!(scheduler.failed_tasks(10).await.unwrap(), Vec::new());
    }
}
