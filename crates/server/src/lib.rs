//! HTTP server for Storage Engine catalog management.

mod catalog;
mod iceberg;

use axum::{
    Router,
    http::StatusCode,
    routing::{get, post},
};
use se_meta_store::catalog::CatalogStore;

/// Builds the HTTP API. Catalog configurations are persisted in the secret store;
/// there is deliberately no catalog listing endpoint.
pub fn router(catalog_store: CatalogStore) -> Router {
    Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .route("/catalogs", post(catalog::create))
        .route("/catalogs/{id}", get(catalog::get).delete(catalog::delete))
        .nest("/catalogs/{id}", iceberg::router())
        .with_state(catalog_store)
}
