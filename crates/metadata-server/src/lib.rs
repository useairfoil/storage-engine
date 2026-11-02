mod catalog;
mod table;
mod tasks;
mod wal;

use std::sync::Arc;

use axum::{
    Extension, Router,
    routing::{delete, get, post},
};
use se_common::clock::SystemClock;
use se_metadata::Metadata;
use tokio::sync::Notify;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
struct TaskPolling {
    notify: Arc<Notify>,
    clock: Arc<dyn SystemClock>,
}

/// Builds the metadata HTTP API using the supplied metadata service.
pub fn router(metadata: Metadata, clock: Arc<dyn SystemClock>) -> Router {
    Router::new()
        .route("/catalogs", post(catalog::create))
        .route(
            "/catalogs/{id}",
            get(catalog::get)
                .put(catalog::update)
                .delete(catalog::delete),
        )
        .route(
            "/catalogs/{id}/tables/{namespace}/{table}",
            get(table::resolve),
        )
        .route("/catalogs/{id}/by-uuid/{table_uuid}/wal", post(wal::action))
        .route("/catalogs/{id}/by-uuid/{table_uuid}", delete(table::delete))
        .route(
            "/catalogs/{id}/by-uuid/{table_uuid}/debug",
            get(table::debug),
        )
        .route("/tasks", post(tasks::action))
        .with_state(metadata)
        .layer(Extension(TaskPolling {
            notify: Arc::new(Notify::new()),
            clock,
        }))
        .layer(TraceLayer::new_for_http())
}
