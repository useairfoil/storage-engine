use axum::{
    Json,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use se_meta_store::catalog::{
    CatalogConfig, CatalogName, CatalogStore, Error as StoreError, IcebergError,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub(crate) struct CreateCatalogRequest {
    id: String,
    #[serde(flatten)]
    config: CatalogConfig,
}

#[derive(Serialize)]
pub(crate) struct Catalog {
    name: CatalogName,
    #[serde(flatten)]
    config: CatalogConfig,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invalid catalog id")]
    InvalidId,
    #[error("catalog not found")]
    NotFound,
    #[error("invalid catalog config: {0}")]
    InvalidConfig(#[from] IcebergError),
    #[error("{0}")]
    Store(#[from] StoreError),
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

pub(crate) async fn create(
    State(store): State<CatalogStore>,
    Json(request): Json<CreateCatalogRequest>,
) -> Result<impl IntoResponse, Error> {
    let name = CatalogName::new(request.id).map_err(|_| Error::InvalidId)?;

    validate_config(&request.config, &name).await?;

    if store.get(name.clone()).await?.is_some() {
        return Err(Error::Store(StoreError::AlreadyExists(name)));
    }

    store.create(name.clone(), request.config.clone()).await?;

    Ok((
        StatusCode::CREATED,
        [(header::LOCATION, format!("/{name}"))],
        Json(Catalog {
            name,
            config: request.config,
        }),
    ))
}

pub(crate) async fn get(
    State(store): State<CatalogStore>,
    Path(id): Path<String>,
) -> Result<Json<Catalog>, Error> {
    let name = CatalogName::new(id).map_err(|_| Error::InvalidId)?;

    let catalog = store.get(name.clone()).await?.ok_or(Error::NotFound)?;

    Ok(Json(Catalog {
        name,
        config: catalog.config().clone(),
    }))
}

pub(crate) async fn delete(
    State(store): State<CatalogStore>,
    Path(id): Path<String>,
) -> Result<StatusCode, Error> {
    let name = CatalogName::new(id).map_err(|_| Error::InvalidId)?;

    store.delete(name).await?;

    Ok(StatusCode::NO_CONTENT)
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::InvalidId => StatusCode::BAD_REQUEST,
            Self::InvalidConfig(_) => StatusCode::BAD_REQUEST,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Store(StoreError::AlreadyExists(_)) => StatusCode::CONFLICT,
            Self::Store(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let error = if status == StatusCode::INTERNAL_SERVER_ERROR {
            // Store errors can contain credentials or backend details.
            "internal server error".to_owned()
        } else {
            self.to_string()
        };
        (status, Json(ErrorBody { error })).into_response()
    }
}

/// Checks that the catalog configuration yields a usable iceberg catalog
/// by listing its root namespaces.
async fn validate_config(config: &CatalogConfig, name: &CatalogName) -> Result<(), Error> {
    let catalog = config
        .to_catalog(&name.to_string())
        .await
        .map_err(|error| match error {
            StoreError::Iceberg(source) => source.into(),
            source => Error::Store(source),
        })?;

    catalog.list_namespaces(None).await?;

    Ok(())
}
