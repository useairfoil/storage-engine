use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
};
use iceberg::{NamespaceIdent, TableIdent, spec::TableMetadata};
use se_metadata::{CatalogId, Metadata, TableDebugState, TableStatus};
use uuid::Uuid;

use crate::{TaskPolling, catalog::Error};

/// Queues removal of a table's Airfoil files and metadata without changing Iceberg.
pub(crate) async fn delete(
    State(metadata): State<Metadata>,
    Extension(polling): Extension<TaskPolling>,
    Path((id, table_uuid)): Path<(String, Uuid)>,
) -> Result<StatusCode, Error> {
    metadata
        .delete_table(&CatalogId::new(id)?, table_uuid)
        .await?;
    polling.notify.notify_waiters();
    Ok(StatusCode::ACCEPTED)
}

/// Dumps a table's persisted WAL and materialization state without changing it.
pub(crate) async fn debug(
    State(metadata): State<Metadata>,
    Path((id, table_uuid)): Path<(String, Uuid)>,
) -> Result<Json<TableDebugState>, Error> {
    let id = CatalogId::new(id)?;

    Ok(Json(metadata.debug_table(&id, table_uuid).await?))
}

/// Returns a read-only summary of a table's persisted progress and scheduled work.
pub(crate) async fn status(
    State(metadata): State<Metadata>,
    Path((id, table_uuid)): Path<(String, Uuid)>,
) -> Result<Json<TableStatus>, Error> {
    let id = CatalogId::new(id)?;

    Ok(Json(metadata.table_status(&id, table_uuid).await?))
}

/// Resolves an Iceberg REST-encoded namespace and table name to table metadata.
pub(crate) async fn resolve(
    State(metadata): State<Metadata>,
    Path((id, namespace, table)): Path<(String, String, String)>,
) -> Result<Json<TableMetadata>, Error> {
    let id = CatalogId::new(id)?;
    let namespace =
        NamespaceIdent::from_strs(namespace.split('\u{001f}')).map_err(se_metadata::Error::from)?;
    Ok(Json(
        metadata
            .resolve_table(&id, &TableIdent::new(namespace, table))
            .await?,
    ))
}
