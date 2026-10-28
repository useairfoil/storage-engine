use axum::{
    Json,
    extract::{Path, State},
};
use iceberg::{NamespaceIdent, TableIdent, spec::TableMetadata};
use se_metadata::{CatalogId, Metadata, TableDebugState};
use uuid::Uuid;

use crate::catalog::Error;

/// Dumps a table's persisted WAL and materialization state without changing it.
pub(crate) async fn debug(
    State(metadata): State<Metadata>,
    Path((id, table_uuid)): Path<(String, Uuid)>,
) -> Result<Json<TableDebugState>, Error> {
    let id = CatalogId::new(id)?;

    Ok(Json(metadata.debug_table(&id, table_uuid).await?))
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
