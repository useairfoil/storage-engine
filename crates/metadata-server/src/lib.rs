mod catalog;
mod wal;

use axum::{
    Router,
    routing::{get, post},
};
use se_metadata::Metadata;
use tower_http::trace::TraceLayer;

/// Builds the metadata HTTP API using the supplied metadata service.
pub fn router(metadata: Metadata) -> Router {
    Router::new()
        .route("/catalogs", post(catalog::create))
        .route(
            "/catalogs/{id}",
            get(catalog::get)
                .put(catalog::update)
                .delete(catalog::delete),
        )
        .route("/catalogs/{id}/tables/{table_uuid}/wal", post(wal::action))
        .with_state(metadata)
        .layer(TraceLayer::new_for_http())
}
