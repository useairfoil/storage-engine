use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use se_metadata::{CatalogConfig, CatalogId, Metadata};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub(crate) struct Catalog {
    id: String,

    #[serde(flatten)]
    config: CatalogConfig,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("catalog not found")]
    NotFound,

    #[error("{0}")]
    Metadata(#[from] se_metadata::Error),
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

pub(crate) async fn create(
    State(metadata): State<Metadata>,
    Json(request): Json<Catalog>,
) -> Result<impl IntoResponse, Error> {
    let id = CatalogId::new(request.id)?;

    metadata.create_catalog(id, request.config).await?;

    Ok(StatusCode::CREATED)
}

pub(crate) async fn get(
    State(metadata): State<Metadata>,
    Path(id): Path<String>,
) -> Result<Json<Catalog>, Error> {
    let id = CatalogId::new(id)?;
    let config = metadata.get_catalog(&id).await?.ok_or(Error::NotFound)?;

    Ok(Json(Catalog {
        id: id.to_string(),
        config,
    }))
}

pub(crate) async fn update(
    State(metadata): State<Metadata>,
    Path(id): Path<String>,
    Json(config): Json<CatalogConfig>,
) -> Result<Json<Catalog>, Error> {
    let id = CatalogId::new(id)?;

    metadata.update_catalog(&id, config.clone()).await?;

    Ok(Json(Catalog {
        id: id.to_string(),
        config,
    }))
}

pub(crate) async fn delete(
    State(metadata): State<Metadata>,
    Path(id): Path<String>,
) -> Result<StatusCode, Error> {
    let id = CatalogId::new(id)?;

    metadata.delete_catalog(&id).await?;

    Ok(StatusCode::NO_CONTENT)
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::NotFound | Self::Metadata(se_metadata::Error::CatalogNotFound(_)) => {
                StatusCode::NOT_FOUND
            }
            Self::Metadata(se_metadata::Error::InvalidCatalogId(_))
            | Self::Metadata(se_metadata::Error::WalSequenceTooHigh { .. }) => {
                StatusCode::BAD_REQUEST
            }
            Self::Metadata(se_metadata::Error::CatalogAlreadyExists(_)) => StatusCode::CONFLICT,
            Self::Metadata(se_metadata::Error::WalFenced) => StatusCode::CONFLICT,
            Self::Metadata(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let error = if status == StatusCode::INTERNAL_SERVER_ERROR {
            // Backend errors can contain credentials or storage details.
            "internal server error".to_owned()
        } else {
            self.to_string()
        };

        (status, Json(ErrorBody { error })).into_response()
    }
}
