use reqwest::{Response, StatusCode};
use uuid::Uuid;

use crate::{
    Error, MaterializationResult, MetadataClient, Result, TaskAssignment, TaskRequest, TaskToken,
};

impl MetadataClient {
    /// Claims work for a unique worker session, returning its existing live assignment on retries.
    pub async fn claim_task(&self, worker_id: Uuid) -> Result<Option<TaskAssignment>> {
        Ok(self
            .task_request(TaskRequest::Claim { worker_id })
            .await?
            .json()
            .await?)
    }

    /// Renews an assignment's lease and returns its updated expiration.
    pub async fn heartbeat_task(&self, token: TaskToken) -> Result<TaskAssignment> {
        Ok(self
            .task_request(TaskRequest::Heartbeat { token })
            .await?
            .json()
            .await?)
    }

    /// Reports materialization completion and atomically acknowledges its WAL range.
    pub async fn complete_materialization(
        &self,
        token: TaskToken,
        result: MaterializationResult,
    ) -> Result<()> {
        self.task_request(TaskRequest::CompleteMaterialization { token, result })
            .await?;
        Ok(())
    }

    /// Releases an assignment for delayed retry without acknowledging WAL.
    pub async fn fail_task(&self, token: TaskToken) -> Result<()> {
        self.task_request(TaskRequest::Fail { token }).await?;
        Ok(())
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
}
