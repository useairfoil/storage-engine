//! Bounded execution of scheduled table reconciliations.

use std::{num::NonZeroUsize, sync::Arc};

use se_reconciliation::Reconciler;
use se_scheduler::{ClaimedTask, Task, TaskFailure, TaskOutcome, TaskScheduler};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::warn;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("scheduler error: {0}")]
    Scheduler(#[from] se_scheduler::Error),
    #[error("worker task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct WorkerPoolOptions {
    /// Maximum number of tasks claimed or executing concurrently.
    pub concurrency: NonZeroUsize,
}

/// Claims tasks and reports durable outcomes.
///
/// Retries belong to the scheduler. A lost attempt is abandoned; other scheduler
/// errors stop the pool and are returned to its supervisor.
#[derive(Debug, Clone)]
pub struct WorkerPool {
    scheduler: Arc<dyn TaskScheduler>,
    reconciler: Reconciler,
    options: WorkerPoolOptions,
}

impl Default for WorkerPoolOptions {
    fn default() -> Self {
        Self {
            // PANIC: 10 is nonzero.
            concurrency: NonZeroUsize::new(10).unwrap(),
        }
    }
}

impl WorkerPool {
    pub fn new(
        scheduler: Arc<dyn TaskScheduler>,
        reconciler: Reconciler,
        options: WorkerPoolOptions,
    ) -> Self {
        Self {
            scheduler,
            reconciler,
            options,
        }
    }

    /// Runs the pool until cancellation or an error.
    /// Cancellation stops new claims and waits for running jobs to report outcomes.
    /// Dropping the future aborts all running jobs. Any abandoned in-flight claim
    /// remains in the scheduler and recovers through heartbeat expiry.
    /// The scheduler's `claim` call is responsible for long-polling an empty queue.
    pub async fn run(self, ct: CancellationToken) -> Result<()> {
        let mut jobs = JoinSet::new();
        let claim = self.scheduler.claim();
        tokio::pin!(claim);

        loop {
            tokio::select! {
                biased;
                () = ct.cancelled() => break,
                Some(result) = jobs.join_next(), if !jobs.is_empty() => result??,
                result = &mut claim, if jobs.len() < self.options.concurrency.get() => {
                    if let Some(task) = result? {
                        jobs.spawn(self.clone().run_task(task));
                    }
                    // Preserve this future across job completions: cancelling a
                    // claim could leave durable work without an executor.
                    claim.set(self.scheduler.claim());
                }
            }
        }
        while let Some(result) = jobs.join_next().await {
            result??;
        }
        Ok(())
    }

    async fn run_task(self, claim: ClaimedTask) -> Result<()> {
        match self.execute(&claim).await {
            Ok(()) => Ok(()),
            Err(
                se_scheduler::Error::StaleAttempt { .. } | se_scheduler::Error::TaskNotFound(_),
            ) => {
                warn!(task_id = %claim.task_id, "worker lost its task attempt");
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn execute(&self, claim: &ClaimedTask) -> se_scheduler::Result<()> {
        let result = match &claim.task {
            Task::Reconcile {
                catalog_name,
                table_ident,
                ..
            } => {
                async {
                    let reconciler = self
                        .reconciler
                        .for_table(
                            catalog_name.clone(),
                            table_ident.namespace.clone(),
                            table_ident.name.clone(),
                        )
                        .await?;
                    let result = reconciler.run_reconciliation().await;
                    let close_result = reconciler.table().clone().flush_and_close().await;
                    result?;
                    close_result?;
                    Ok::<_, se_reconciliation::Error>(())
                }
                .await
            }
        };

        let outcome = match result {
            Ok(()) => TaskOutcome::Success,
            Err(error) => {
                warn!(task_id = %claim.task_id, %error, "scheduled reconciliation failed");
                let reason = error.to_string();
                let failure = match error {
                    se_reconciliation::Error::CatalogNotFound(_)
                    | se_reconciliation::Error::TableNotFound(_) => {
                        TaskFailure::Permanent { reason }
                    }
                    _ => TaskFailure::Retryable { reason },
                };
                TaskOutcome::Failure(failure)
            }
        };

        self.scheduler
            .complete(claim.task_id, claim.attempt_id, outcome)
            .await
    }
}
