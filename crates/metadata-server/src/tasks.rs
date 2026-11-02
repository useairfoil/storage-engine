use std::time::Duration;

use axum::{
    Extension, Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use se_metadata::{Metadata, TaskAssignment, TaskRequest};

use crate::{TaskPolling, catalog::Error};

/// Handles worker assignments, recovery inputs, completion reports, and retries.
pub(crate) async fn action(
    State(metadata): State<Metadata>,
    Extension(polling): Extension<TaskPolling>,
    Json(request): Json<TaskRequest>,
) -> Result<Response, Error> {
    match request {
        TaskRequest::CompleteDeleteTable { token } => {
            metadata.complete_delete_table(&token).await?;
        }
        TaskRequest::Claim { worker_id } => {
            let deadline = polling.clock.sleep(Duration::from_secs(5));
            tokio::pin!(deadline);

            loop {
                // Register before checking for work so a concurrent enqueue cannot be missed.
                let notified = polling.notify.notified();
                if let Some(task) = metadata.claim_task(worker_id).await? {
                    return Ok(Json(Some(task)).into_response());
                }

                tokio::select! {
                    _ = notified => {}
                    _ = &mut deadline => {
                        return Ok(Json(None::<TaskAssignment>).into_response());
                    }
                }
            }
        }
        TaskRequest::Heartbeat { token } => {
            return Ok(Json(metadata.heartbeat_task(&token).await?).into_response());
        }
        TaskRequest::CompleteMaterialization { token, result } => {
            metadata.complete_materialization(&token, result).await?;
        }
        TaskRequest::MaterializationInput {
            token,
            applied_through,
        } => {
            return Ok(Json(
                metadata
                    .materialization_input(&token, applied_through)
                    .await?,
            )
            .into_response());
        }
        TaskRequest::ReconciliationInput { token } => {
            return Ok(Json(metadata.reconciliation_input(&token).await?).into_response());
        }
        TaskRequest::CompleteReconciliation { token, commit } => {
            metadata.complete_reconciliation(&token, commit).await?;
        }
        TaskRequest::Fail { token } => metadata.fail_task(&token).await?,
    }

    polling.notify.notify_waiters();
    Ok(StatusCode::NO_CONTENT.into_response())
}
