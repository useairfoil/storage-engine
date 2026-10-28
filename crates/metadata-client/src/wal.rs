use async_trait::async_trait;
use reqwest::{Response, StatusCode};
pub use se_wal::WalRequest;
use se_wal::{BatchCommit, BatchDescriptor, BatchMetadata, BatchRead, Error, Result};
use uuid::Uuid;

use crate::{CatalogId, MetadataClient};

#[derive(Clone)]
pub struct WalClient {
    metadata: MetadataClient,
    catalog_id: CatalogId,
    table_uuid: Uuid,
}

impl WalClient {
    pub(crate) fn new(metadata: MetadataClient, catalog_id: CatalogId, table_uuid: Uuid) -> Self {
        Self {
            metadata,
            catalog_id,
            table_uuid,
        }
    }

    /// Posts a WAL action to this table's endpoint and preserves fencing errors.
    #[tracing::instrument(name = "wal_client.request", skip_all, fields(catalog_id = %self.catalog_id, table_uuid = %self.table_uuid))]
    async fn request(&self, request: WalRequest) -> Result<Response> {
        let mut url = self.metadata.base_url.clone();
        url.path_segments_mut()
            .map_err(|_| Error::Commit(crate::Error::InvalidBaseUrl.to_string()))?
            .pop_if_empty()
            .push("catalogs")
            .push(self.catalog_id.as_str())
            .push("tables")
            .push(&self.table_uuid.to_string())
            .push("wal");

        let response = self
            .metadata
            .client
            .post(url)
            .json(&request)
            .send()
            .await
            .map_err(|error| Error::Commit(error.to_string()))?;

        if response.status() == StatusCode::CONFLICT {
            return Err(Error::Fenced);
        }

        response
            .error_for_status()
            .map_err(|error| Error::Commit(error.to_string()))
    }
}

#[async_trait]
impl BatchCommit for WalClient {
    async fn commit(&self, batch: BatchMetadata) -> Result<()> {
        self.request(WalRequest::Commit { batch }).await?;

        Ok(())
    }
}

#[async_trait]
impl BatchRead for WalClient {
    async fn claim_reader(&self) -> Result<u64> {
        self.request(WalRequest::ClaimReader)
            .await?
            .json()
            .await
            .map_err(|error| Error::Commit(error.to_string()))
    }

    async fn read_after(&self, sequence: Option<u64>, max: usize) -> Result<Vec<BatchDescriptor>> {
        self.request(WalRequest::ReadAfter { sequence, max })
            .await?
            .json()
            .await
            .map_err(|error| Error::Commit(error.to_string()))
    }

    async fn ack_through(&self, epoch: u64, sequence: u64) -> Result<()> {
        self.request(WalRequest::AckThrough { epoch, sequence })
            .await?;

        Ok(())
    }
}
