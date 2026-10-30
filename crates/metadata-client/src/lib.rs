mod heartbeat;
mod wal;

use async_trait::async_trait;
pub use heartbeat::Heartbeat;
use iceberg::TableIdent;
use reqwest::{Client, Response, StatusCode, Url};
pub use se_metadata::{
    CatalogConfig, CatalogId, DataFileMetadata, DeletionVectorMetadata, DeletionVectorReplacement,
    IndexDelta, MaterializationInput, MaterializationResult, PreparedCommit,
    RECONCILIATION_ID_PROPERTY, REVISION_PROPERTY, ReconciliationInput, ReconciliationPlan,
    RestCatalogConfig, Task, TaskAssignment, TaskRequest, TaskToken, VersionedDataFile,
    VersionedDeletionVector, WalRange,
};
use se_wal::{BatchDescriptor, BatchMetadata};
use serde::Deserialize;
use uuid::Uuid;
pub use wal::{WalClient, WalRequest};

#[derive(Clone)]
pub struct HttpMetadataClient {
    client: Client,
    base_url: Url,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("metadata HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("metadata URL cannot be used as a base URL")]
    InvalidBaseUrl,

    #[error("task attempt is no longer active")]
    TaskFenced,

    #[error("table not found: {0}")]
    TableNotFound(TableIdent),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait MetadataClient: Send + Sync {
    /// Fetches a catalog configuration, returning `None` if it does not exist.
    async fn get_catalog(&self, id: &CatalogId) -> Result<Option<CatalogConfig>>;

    /// Resolves table metadata and durably records its UUID/name binding.
    async fn resolve_table(
        &self,
        catalog_id: &CatalogId,
        table: &TableIdent,
    ) -> Result<iceberg::spec::TableMetadata>;

    /// Claims work or returns this worker session's existing live assignment.
    async fn claim_task(&self, worker_id: Uuid) -> Result<Option<TaskAssignment>>;

    /// Creates an owned handle that renews the task lease when progress is reported.
    fn heartbeat_task(&self, token: TaskToken) -> Heartbeat;

    /// Accepts materialization with atomic WAL acknowledgment, allowing exact completion retries.
    async fn complete_materialization(
        &self,
        token: TaskToken,
        result: MaterializationResult,
    ) -> Result<()>;

    /// Reads accepted index deltas preceding the active materialization range.
    async fn materialization_input(
        &self,
        token: TaskToken,
        applied_through: u64,
    ) -> Result<MaterializationInput>;

    /// Reads the pinned reconciliation input and any already prepared commit.
    async fn reconciliation_input(&self, token: TaskToken) -> Result<ReconciliationInput>;

    /// Submits prepared metadata for durable conditional publication and confirmation.
    async fn complete_reconciliation(&self, token: TaskToken, commit: PreparedCommit)
    -> Result<()>;

    /// Releases an assignment for delayed retry without acknowledging WAL.
    async fn fail_task(&self, token: TaskToken) -> Result<()>;

    /// Appends an uploaded WAL batch and schedules materialization.
    async fn wal_append(
        &self,
        catalog_id: &CatalogId,
        table_uuid: Uuid,
        batch: BatchMetadata,
    ) -> se_wal::Result<()>;

    /// Reads retained WAL descriptors without acquiring ownership or acknowledging them.
    async fn wal_read_after(
        &self,
        catalog_id: &CatalogId,
        table_uuid: Uuid,
        sequence: Option<u64>,
        max: usize,
    ) -> se_wal::Result<Vec<BatchDescriptor>>;
}

#[derive(Deserialize)]
struct CatalogResponse {
    #[serde(flatten)]
    config: CatalogConfig,
}

impl HttpMetadataClient {
    /// Creates an HTTP client for the metadata server at the supplied base URL.
    pub fn new(base_url: Url) -> Self {
        Self {
            client: Client::new(),
            base_url,
        }
    }

    async fn task_request(&self, request: TaskRequest) -> Result<Response> {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|_| Error::InvalidBaseUrl)?
            .pop_if_empty()
            .push("tasks");
        let response = self.client.post(url).json(&request).send().await?;
        if response.status() == StatusCode::CONFLICT {
            return Err(Error::TaskFenced);
        }
        Ok(response.error_for_status()?)
    }

    async fn wal_request(
        &self,
        catalog_id: &CatalogId,
        table_uuid: Uuid,
        request: WalRequest,
    ) -> se_wal::Result<Response> {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|_| se_wal::Error::Commit(Error::InvalidBaseUrl.to_string()))?
            .pop_if_empty()
            .push("catalogs")
            .push(catalog_id.as_str())
            .push("by-uuid")
            .push(&table_uuid.to_string())
            .push("wal");
        let response = self
            .client
            .post(url)
            .json(&request)
            .send()
            .await
            .map_err(|error| se_wal::Error::Commit(error.to_string()))?;
        if response.status() == StatusCode::CONFLICT {
            return Err(se_wal::Error::Fenced);
        }
        response
            .error_for_status()
            .map_err(|error| se_wal::Error::Commit(error.to_string()))
    }
}

#[async_trait]
impl MetadataClient for HttpMetadataClient {
    /// Fetches a catalog configuration, returning `None` when the catalog does not exist.
    #[tracing::instrument(name = "metadata_client.get_catalog", skip_all, fields(catalog.id = %id))]
    async fn get_catalog(&self, id: &CatalogId) -> Result<Option<CatalogConfig>> {
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

    /// Resolves a table through the metadata service, which records its UUID/name binding.
    #[tracing::instrument(name = "metadata_client.resolve_table", skip_all, fields(catalog.id = %catalog_id, table.ident = %table))]
    async fn resolve_table(
        &self,
        catalog_id: &CatalogId,
        table: &TableIdent,
    ) -> Result<iceberg::spec::TableMetadata> {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|_| Error::InvalidBaseUrl)?
            .pop_if_empty()
            .push("catalogs")
            .push(catalog_id.as_str())
            .push("tables")
            .push(&table.namespace().to_url_string())
            .push(table.name());

        let response = self.client.get(url).send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::TableNotFound(table.clone()));
        }

        Ok(response.error_for_status()?.json().await?)
    }

    async fn claim_task(&self, worker_id: Uuid) -> Result<Option<TaskAssignment>> {
        Ok(self
            .task_request(TaskRequest::Claim { worker_id })
            .await?
            .json()
            .await?)
    }

    fn heartbeat_task(&self, token: TaskToken) -> Heartbeat {
        let client = self.clone();
        Heartbeat::new(move || {
            let client = client.clone();
            let token = token.clone();
            async move {
                client
                    .task_request(TaskRequest::Heartbeat { token })
                    .await?;
                Ok(())
            }
        })
    }

    async fn complete_materialization(
        &self,
        token: TaskToken,
        result: MaterializationResult,
    ) -> Result<()> {
        self.task_request(TaskRequest::CompleteMaterialization { token, result })
            .await?;
        Ok(())
    }

    async fn fail_task(&self, token: TaskToken) -> Result<()> {
        self.task_request(TaskRequest::Fail { token }).await?;
        Ok(())
    }

    async fn materialization_input(
        &self,
        token: TaskToken,
        applied_through: u64,
    ) -> Result<MaterializationInput> {
        Ok(self
            .task_request(TaskRequest::MaterializationInput {
                token,
                applied_through,
            })
            .await?
            .json()
            .await?)
    }

    async fn reconciliation_input(&self, token: TaskToken) -> Result<ReconciliationInput> {
        Ok(self
            .task_request(TaskRequest::ReconciliationInput { token })
            .await?
            .json()
            .await?)
    }

    async fn complete_reconciliation(
        &self,
        token: TaskToken,
        commit: PreparedCommit,
    ) -> Result<()> {
        self.task_request(TaskRequest::CompleteReconciliation { token, commit })
            .await?;
        Ok(())
    }

    async fn wal_append(
        &self,
        catalog_id: &CatalogId,
        table_uuid: Uuid,
        batch: BatchMetadata,
    ) -> se_wal::Result<()> {
        self.wal_request(catalog_id, table_uuid, WalRequest::Commit { batch })
            .await?;
        Ok(())
    }

    async fn wal_read_after(
        &self,
        catalog_id: &CatalogId,
        table_uuid: Uuid,
        sequence: Option<u64>,
        max: usize,
    ) -> se_wal::Result<Vec<BatchDescriptor>> {
        self.wal_request(
            catalog_id,
            table_uuid,
            WalRequest::ReadAfter { sequence, max },
        )
        .await?
        .json()
        .await
        .map_err(|error| se_wal::Error::Commit(error.to_string()))
    }
}
