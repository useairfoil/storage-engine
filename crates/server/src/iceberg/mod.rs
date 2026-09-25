//! Iceberg REST catalog endpoints, relative to `/catalogs/{id}`.

mod client;

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use iceberg_catalog_rest::ErrorModel;
use se_meta_store::catalog::{CatalogConfig, CatalogName, CatalogStore, Error as StoreError};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("invalid catalog id")]
    InvalidId,
    #[error("catalog not found")]
    NotFound,
    #[error("{0}")]
    Store(#[from] StoreError),
    #[error("failed to read the request body: {0}")]
    Body(#[from] axum::Error),
    #[error("failed to forward request to the upstream catalog: {0}")]
    Proxy(#[from] client::Error),
}

#[derive(Serialize)]
struct ErrorResponse {
    error: ErrorModel,
}

pub(super) fn router() -> Router<CatalogStore> {
    Router::new().fallback(proxy)
}

/// Proxy a request to the upstream iceberg REST catalog.
///
/// Returns an error response if the catalog specified by the `id` doesn't exist.
pub(super) async fn proxy(
    State(store): State<CatalogStore>,
    Path(id): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Error> {
    let name = CatalogName::new(id).map_err(|_| Error::InvalidId)?;
    let catalog = store.get(name).await?.ok_or(Error::NotFound)?;
    let CatalogConfig::Rest(config) = catalog.config();

    let upstream_uri = build_upstream_uri(&config.uri, &uri);

    let body = axum::body::to_bytes(body, usize::MAX).await?;
    let response = client::Client::new(config)?
        .send(method, upstream_uri, headers, body)
        .await?;

    let status = response.status();
    let headers = response.headers().to_owned();
    let body = Body::from_stream(response.bytes_stream());

    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    Ok(response)
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, error_type, message) = match self {
            Self::InvalidId => (
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "invalid catalog id",
            ),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                "NoSuchCatalogException",
                "catalog not found",
            ),
            // Never expose upstream credentials or backend details to clients.
            Self::Store(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                "internal server error",
            ),
            Self::Body(_) => (
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "failed to read the request body",
            ),
            Self::Proxy(_) => (
                StatusCode::BAD_GATEWAY,
                "BadGatewayException",
                "failed to forward request to the upstream catalog",
            ),
        };
        (
            status,
            Json(ErrorResponse {
                error: ErrorModel {
                    message: message.to_owned(),
                    r#type: error_type.to_owned(),
                    code: status.as_u16(),
                    stack: None,
                },
            }),
        )
            .into_response()
    }
}

fn build_upstream_uri(base_uri: &str, uri: &Uri) -> String {
    let base_uri = base_uri.trim_end_matches('/');
    let path = uri.path();
    match uri.query() {
        Some(query) => format!("{base_uri}{path}?{query}"),
        None => format!("{base_uri}{path}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_upstream_uri_with_single_separator() {
        let uri = "/v1/config?warehouse=s3%3A%2F%2Fmain".parse().unwrap();

        assert_eq!(
            build_upstream_uri("http://10.89.1.2:8181", &uri),
            "http://10.89.1.2:8181/v1/config?warehouse=s3%3A%2F%2Fmain"
        );
        assert_eq!(
            build_upstream_uri("http://10.89.1.2:8181/", &uri),
            "http://10.89.1.2:8181/v1/config?warehouse=s3%3A%2F%2Fmain"
        );
    }
}
