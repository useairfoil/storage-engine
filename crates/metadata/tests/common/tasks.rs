use se_metadata::{Metadata, Task, TaskAssignment};
use uuid::Uuid;

/// Completes independent WAL cleanup before returning foreground work.
pub async fn claim_foreground(
    metadata: &Metadata,
    worker: Uuid,
) -> Result<Option<TaskAssignment>, se_metadata::Error> {
    loop {
        let assignment = metadata.claim_task(worker).await?;
        match assignment {
            Some(assignment) if matches!(assignment.task, Task::GarbageCollectWal { .. }) => {
                metadata.complete_wal_cleanup(&assignment.token).await?;
            }
            assignment => return Ok(assignment),
        }
    }
}
