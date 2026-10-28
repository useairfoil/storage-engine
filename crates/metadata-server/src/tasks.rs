use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use se_metadata::{Metadata, TaskRequest};

use crate::catalog::Error;

/// Handles worker assignments, recovery inputs, completion reports, and retries.
pub(crate) async fn action(
    State(metadata): State<Metadata>,
    Json(request): Json<TaskRequest>,
) -> Result<Response, Error> {
    match request {
        TaskRequest::Claim { worker_id } => {
            return Ok(Json(metadata.claim_task(worker_id).await?).into_response());
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

    Ok(StatusCode::NO_CONTENT.into_response())
}
