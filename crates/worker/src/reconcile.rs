use se_metadata_client::{Heartbeat, ReconciliationPlan, TaskToken};

use crate::{Error, Result, Worker, reconciliation};

impl Worker {
    #[tracing::instrument(name = "worker.reconcile", skip_all)]
    pub(super) async fn reconcile(
        &self,
        token: TaskToken,
        plan: ReconciliationPlan,
        heartbeat: Heartbeat,
    ) -> Result<()> {
        let input = self.metadata.reconciliation_input(token.clone()).await?;
        if input.plan != plan {
            return Err(Error::InvalidReconciliation(
                "assigned plan does not match input".into(),
            ));
        }

        let commit = if let Some(commit) = input.prepared {
            // A persisted payload is canonical even if the catalog has advanced.
            heartbeat.beat().await.map_err(Error::Heartbeat)?;
            commit
        } else {
            let table = self.load_table(&token).await?;
            reconciliation::prepare_reconciliation(
                &table,
                &plan,
                &input.files,
                &input.added_vectors,
                &input.removed_vectors,
                &heartbeat,
            )
            .await?
        };

        self.metadata
            .complete_reconciliation(token, commit)
            .await
            .map_err(Into::into)
    }
}
