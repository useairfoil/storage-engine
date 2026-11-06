mod heartbeat;
mod wal;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
pub use heartbeat::Heartbeat;
use iceberg::TableIdent;
use reqwest::{Client, Response, StatusCode, Url};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_tracing::TracingMiddleware;
use se_common::clock::SystemClock;
pub use se_metadata::{
    CatalogConfig, CatalogId, DataFileMetadata, DeletionVectorMetadata, DeletionVectorReplacement,
    IndexDelta, MaterializationInput, MaterializationResult, PreparedCommit,
    RECONCILIATION_ID_PROPERTY, REVISION_PROPERTY, ReconciliationInput, ReconciliationPlan,
    RestCatalogConfig, Task, TaskAssignment, TaskAssignmentResponse, TaskRequest, TaskToken,
    VersionedDataFile, VersionedDeletionVector, WalRange,
};
use se_wal::{BatchDescriptor, BatchMetadata};
use serde::Deserialize;
use tracing::{Instrument, info_span, instrument};
use uuid::Uuid;
pub use wal::{WalClient, WalRequest};

#[derive(Clone)]
pub struct HttpMetadataClient {
    client: ClientWithMiddleware,
    base_url: Url,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("metadata HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("metadata HTTP middleware failed: {0}")]
    Middleware(#[source] reqwest_middleware::Error),

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
    async fn claim_task(&self, worker_id: Uuid) -> Result<Option<TaskAssignmentResponse>>;

    /// Confirms file deletion and removes the table's persisted metadata.
    async fn complete_delete_table(&self, token: TaskToken) -> Result<()>;

    /// Creates a progress handle that renews the lease after half its timeout has elapsed.
    fn heartbeat_task(
        &self,
        token: TaskToken,
        timeout: Duration,
        clock: Arc<dyn SystemClock>,
    ) -> Heartbeat;

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

impl From<reqwest_middleware::Error> for Error {
    fn from(error: reqwest_middleware::Error) -> Self {
        match error {
            reqwest_middleware::Error::Reqwest(error) => Self::Http(error),
            error => Self::Middleware(error),
        }
    }
}

impl HttpMetadataClient {
    /// Creates an HTTP client for the metadata server at the supplied base URL.
    pub fn new(base_url: Url) -> Self {
        Self {
            client: ClientBuilder::new(Client::new())
                .with(TracingMiddleware::default())
                .build(),
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
            .push(&catalog_id.to_string())
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
        if matches!(
            response.status(),
            StatusCode::CONFLICT | StatusCode::NOT_FOUND
        ) {
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
    #[instrument(name = "metadata_client.get_catalog", skip_all, fields(catalog.id = %id), err)]
    async fn get_catalog(&self, id: &CatalogId) -> Result<Option<CatalogConfig>> {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|_| Error::InvalidBaseUrl)?
            .pop_if_empty()
            .push("catalogs")
            .push(&id.to_string());

        let response = self.client.get(url).send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let catalog: CatalogResponse = response.error_for_status()?.json().await?;

        Ok(Some(catalog.config))
    }

    /// Resolves a table through the metadata service, which records its UUID/name binding.
    #[instrument(name = "metadata_client.resolve_table", skip_all, fields(catalog.id = %catalog_id, table.ident = %table), err)]
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
            .push(&catalog_id.to_string())
            .push("tables")
            .push(&table.namespace().to_url_string())
            .push(table.name());

        let response = self.client.get(url).send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::TableNotFound(table.clone()));
        }

        Ok(response.error_for_status()?.json().await?)
    }

    #[instrument(name = "metadata_client.claim_task", skip_all, fields(worker.id = %worker_id), err)]
    async fn claim_task(&self, worker_id: Uuid) -> Result<Option<TaskAssignmentResponse>> {
        Ok(self
            .task_request(TaskRequest::Claim { worker_id })
            .await?
            .json()
            .await?)
    }

    #[instrument(
        name = "metadata_client.complete_delete_table",
        skip_all,
        fields(
            catalog.id = %token.catalog_id,
            table.uuid = %token.table_uuid,
            table.ident = %token.table_ident,
            worker.id = %token.worker_id,
            attempt.id = %token.attempt_id,
        ),
        err
    )]
    async fn complete_delete_table(&self, token: TaskToken) -> Result<()> {
        self.task_request(TaskRequest::CompleteDeleteTable { token })
            .await?;
        Ok(())
    }

    fn heartbeat_task(
        &self,
        token: TaskToken,
        timeout: Duration,
        clock: Arc<dyn SystemClock>,
    ) -> Heartbeat {
        let client = self.clone();
        let last_beat = Arc::new(tokio::sync::Mutex::new(clock.now()));
        Heartbeat::new(move || {
            let client = client.clone();
            let token = token.clone();
            let clock = clock.clone();
            let last_beat = last_beat.clone();

            async move {
                let mut last_beat = last_beat.lock().await;
                let now = clock.now();
                if (now - *last_beat).to_std().unwrap_or_default() <= timeout / 2 {
                    return Ok(());
                }
                let span = info_span!(
                    "metadata_client.heartbeat_task",
                    catalog.id = %token.catalog_id,
                    table.uuid = %token.table_uuid,
                    table.ident = %token.table_ident,
                    worker.id = %token.worker_id,
                    attempt.id = %token.attempt_id,
                );
                async move {
                    client
                        .task_request(TaskRequest::Heartbeat { token })
                        .await
                        .inspect_err(|error| tracing::error!(%error))
                }
                .instrument(span)
                .await?;
                *last_beat = now;
                Ok(())
            }
        })
    }

    #[instrument(
        name = "metadata_client.complete_materialization",
        skip_all,
        fields(
            catalog.id = %token.catalog_id,
            table.uuid = %token.table_uuid,
            table.ident = %token.table_ident,
            worker.id = %token.worker_id,
            attempt.id = %token.attempt_id,
            args.schema_id = result.schema_id,
            args.range.start = result.range.start,
            args.range.end = result.range.end,
        ),
        err
    )]
    async fn complete_materialization(
        &self,
        token: TaskToken,
        result: MaterializationResult,
    ) -> Result<()> {
        self.task_request(TaskRequest::CompleteMaterialization { token, result })
            .await?;
        Ok(())
    }

    #[instrument(
        name = "metadata_client.fail_task",
        skip_all,
        fields(
            catalog.id = %token.catalog_id,
            table.uuid = %token.table_uuid,
            table.ident = %token.table_ident,
            worker.id = %token.worker_id,
            attempt.id = %token.attempt_id,
        ),
        err
    )]
    async fn fail_task(&self, token: TaskToken) -> Result<()> {
        self.task_request(TaskRequest::Fail { token }).await?;
        Ok(())
    }

    #[instrument(
        name = "metadata_client.materialization_input",
        skip_all,
        fields(
            catalog.id = %token.catalog_id,
            table.uuid = %token.table_uuid,
            table.ident = %token.table_ident,
            worker.id = %token.worker_id,
            attempt.id = %token.attempt_id,
            args.applied_through = applied_through,
        ),
        err
    )]
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

    #[instrument(
        name = "metadata_client.reconciliation_input",
        skip_all,
        fields(
            catalog.id = %token.catalog_id,
            table.uuid = %token.table_uuid,
            table.ident = %token.table_ident,
            worker.id = %token.worker_id,
            attempt.id = %token.attempt_id,
        ),
        err
    )]
    async fn reconciliation_input(&self, token: TaskToken) -> Result<ReconciliationInput> {
        Ok(self
            .task_request(TaskRequest::ReconciliationInput { token })
            .await?
            .json()
            .await?)
    }

    #[instrument(
        name = "metadata_client.complete_reconciliation",
        skip_all,
        fields(
            catalog.id = %token.catalog_id,
            table.uuid = %token.table_uuid,
            table.ident = %token.table_ident,
            worker.id = %token.worker_id,
            attempt.id = %token.attempt_id,
        ),
        err
    )]
    async fn complete_reconciliation(
        &self,
        token: TaskToken,
        commit: PreparedCommit,
    ) -> Result<()> {
        self.task_request(TaskRequest::CompleteReconciliation { token, commit })
            .await?;
        Ok(())
    }

    #[instrument(
        name = "metadata_client.wal_append",
        skip_all,
        fields(
            catalog.id = %catalog_id,
            table.uuid = %table_uuid,
            args.schema_id = batch.schema_id,
            args.wal_size_bytes = batch.wal_size_bytes,
        ),
        err
    )]
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

    #[instrument(
        name = "metadata_client.wal_read_after",
        skip_all,
        fields(
            catalog.id = %catalog_id,
            table.uuid = %table_uuid,
            args.sequence = ?sequence,
            args.max = max,
        ),
        err
    )]
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
