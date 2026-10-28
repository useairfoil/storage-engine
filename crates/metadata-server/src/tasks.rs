use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use se_metadata::{Metadata, TaskRequest};

use crate::catalog::Error;

/// Handles worker claims, heartbeats, completion reports, and retries.
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
        TaskRequest::Fail { token } => metadata.fail_task(&token).await?,
    }

    Ok(StatusCode::NO_CONTENT.into_response())
}
