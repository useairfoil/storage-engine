use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use se_metadata::{CatalogId, Metadata};
use se_wal::WalRequest;
use uuid::Uuid;

use crate::{TaskPolling, catalog::Error};

/// Handles table-scoped WAL appends and ownership-free reads.
pub(crate) async fn action(
    State(metadata): State<Metadata>,
    Extension(polling): Extension<TaskPolling>,
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
                    batch.wal_size_bytes,
                )
                .await?;

            polling.notify.notify_waiters();
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        WalRequest::ReadAfter { sequence, max } => {
            let batches = metadata
                .wal_read_after(&id, table_uuid, sequence, max)
                .await?;

            Ok(Json(batches).into_response())
        }
    }
}
