use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use se_metadata::{CatalogId, Metadata};
use se_wal::WalRequest;
use uuid::Uuid;

use crate::catalog::Error;

/// Handles table-scoped WAL commits, reads, reader claims, and fenced acknowledgments.
pub(crate) async fn action(
    State(metadata): State<Metadata>,
    Path((id, table_uuid)): Path<(String, Uuid)>,
    Json(request): Json<WalRequest>,
) -> Result<Response, Error> {
    let id = CatalogId::new(id)?;

    match request {
        WalRequest::Commit { batch } => {
            metadata
                .wal_append(
                    &id,
                    table_uuid,
                    batch.schema_id,
                    &batch.location,
                    batch.timestamp,
                )
                .await?;

            Ok(StatusCode::NO_CONTENT.into_response())
        }
        WalRequest::ReadAfter { sequence, max } => {
            let batches = metadata
                .wal_read_after(&id, table_uuid, sequence, max)
                .await?;

            Ok(Json(batches).into_response())
        }
        WalRequest::ClaimReader => {
            let epoch = metadata.wal_claim_reader(&id, table_uuid).await?;

            Ok(Json(epoch).into_response())
        }
        WalRequest::AckThrough { epoch, sequence } => {
            metadata
                .wal_ack_through(&id, table_uuid, epoch, sequence)
                .await?;

            Ok(StatusCode::NO_CONTENT.into_response())
        }
    }
}
