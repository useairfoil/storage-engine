mod wal;

use std::sync::Arc;

use reqwest::{Client, StatusCode, Url};
pub use se_metadata::{CatalogConfig, CatalogId, RestCatalogConfig};
use serde::Deserialize;
use uuid::Uuid;
pub use wal::{WalClient, WalRequest};

#[derive(Clone)]
pub struct MetadataClient {
    client: Client,
    base_url: Url,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("metadata HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("metadata URL cannot be used as a base URL")]
    InvalidBaseUrl,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Deserialize)]
struct CatalogResponse {
    #[serde(flatten)]
    config: CatalogConfig,
}

impl MetadataClient {
    /// Creates an HTTP client for the metadata server at the supplied base URL.
    pub fn new(base_url: Url) -> Self {
        Self {
            client: Client::new(),
            base_url,
        }
    }

    /// Fetches a catalog configuration, returning `None` when the catalog does not exist.
    #[tracing::instrument(name = "metadata_client.get_catalog", skip_all, fields(catalog_id = %id))]
    pub async fn get_catalog(&self, id: &CatalogId) -> Result<Option<CatalogConfig>> {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|_| Error::InvalidBaseUrl)?
            .pop_if_empty()
            .push("catalogs")
            .push(id.as_str());

        let response = self.client.get(url).send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let catalog: CatalogResponse = response.error_for_status()?.json().await?;

        Ok(Some(catalog.config))
    }

    /// Creates a WAL client scoped to the supplied catalog and table.
    pub fn wal_client(&self, catalog_id: CatalogId, table_uuid: Uuid) -> WalClient {
        WalClient::new(self.clone(), catalog_id, table_uuid)
    }
}

impl se_wal::WalClientFactory for MetadataClient {
    fn wal_client(&self, catalog_id: CatalogId, table_uuid: Uuid) -> Arc<dyn se_wal::WalClient> {
        Arc::new(self.wal_client(catalog_id, table_uuid))
    }
}
