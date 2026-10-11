use std::future::Future;

use iceberg::spec::{Schema, SortOrder};
use slatedb::DbTransactionOps;
use uuid::Uuid;

use super::{
    Error, PendingCommit, PublicationIntent, Result, TableRead,
    encoding::{
        CurrentSchemaIdKey, CurrentSchemaIdValue, DefaultSortOrderIdKey, DefaultSortOrderIdValue,
        MaterializedSequenceKey, MaterializedSequenceValue, PendingCommitKey, PendingCommitValue,
        PublicationIntentKey, PublicationIntentValue, PublishedSequenceKey, SchemaKey, SchemaValue,
        SortOrderKey, SortOrderValue, TableUuidKey, TableUuidValue,
    },
};

/// Typed table mutations staged in a transaction.
pub trait TableWrite: DbTransactionOps + Sync {
    /// Reserves a queue prefix. Commit and await durability before submitting it to Iceberg.
    /// Recovering callers must resolve its exact base before retrying or retiring it.
    fn start_publication(
        &self,
        intent: &PublicationIntent,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            if self.get_publication_intent().await?.is_some()
                || self.get_published_sequence().await? != intent.previous_sequence
            {
                return Err(Error::PublicationStateChanged);
            }
            let first = self
                .read_pending_commits(intent.previous_sequence, 1)
                .await?;
            let last = self
                .get(
                    PendingCommitKey {
                        last_sequence: intent.last_sequence,
                    }
                    .encode(),
                )
                .await?;
            if first
                .first()
                .is_none_or(|commit| commit.first_sequence != intent.first_sequence)
                || last.is_none()
                || intent.first_sequence > intent.last_sequence
                || intent
                    .previous_sequence
                    .is_some_and(|sequence| sequence.checked_add(1) != Some(intent.first_sequence))
            {
                return Err(Error::InvalidPublicationRange);
            }
            self.put(
                PublicationIntentKey::encode(),
                PublicationIntentValue::encode(intent)?,
            )?;
            Ok(())
        }
    }

    /// Retires a provably superseded attempt without acknowledging or removing queued files.
    /// A stale retirement cannot clear a newer intent. Commit and await durability.
    fn retire_publication(
        &self,
        intent: &PublicationIntent,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            if self.get_publication_intent().await?.as_ref() != Some(intent)
                || self.get_published_sequence().await? != intent.previous_sequence
            {
                return Err(Error::PublicationStateChanged);
            }
            self.delete(PublicationIntentKey::encode())?;
            Ok(())
        }
    }

    /// Atomically acknowledges a confirmed publication and removes only its queue prefix.
    /// A stale completion cannot clear a newer intent. Commit and await durability.
    fn finish_publication(
        &self,
        intent: &PublicationIntent,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            if self.get_publication_intent().await?.as_ref() != Some(intent)
                || self.get_published_sequence().await? != intent.previous_sequence
            {
                return Err(Error::PublicationStateChanged);
            }
            let mut after = intent.previous_sequence;
            loop {
                let commits = self.read_pending_commits(after, 100).await?;
                if commits.is_empty() {
                    return Err(Error::InvalidPublicationRange);
                }
                for commit in commits {
                    if commit.last_sequence > intent.last_sequence {
                        return Err(Error::InvalidPublicationRange);
                    }
                    self.delete(
                        PendingCommitKey {
                            last_sequence: commit.last_sequence,
                        }
                        .encode(),
                    )?;
                    after = Some(commit.last_sequence);
                    if commit.last_sequence == intent.last_sequence {
                        self.put(
                            PublishedSequenceKey::encode(),
                            MaterializedSequenceValue {
                                sequence: intent.last_sequence,
                            }
                            .encode(),
                        )?;
                        self.delete(PublicationIntentKey::encode())?;
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Stages immutable publication work and its WAL frontier together.
    /// The caller must commit and await durability before acknowledging the WAL.
    /// `expected` is the checkpoint observed before preparing the files.
    fn push_pending_commit(
        &self,
        commit: &PendingCommit,
        expected: Option<u64>,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            let actual = self.get_materialized_sequence().await?;
            if actual != expected {
                return Err(Error::MaterializedCheckpointChanged { expected, actual });
            }
            if commit.first_sequence > commit.last_sequence
                || actual
                    .is_some_and(|sequence| sequence.checked_add(1) != Some(commit.first_sequence))
            {
                return Err(Error::InvalidMaterializedRange {
                    first: commit.first_sequence,
                    last: commit.last_sequence,
                    checkpoint: actual,
                });
            }
            self.put(
                PendingCommitKey {
                    last_sequence: commit.last_sequence,
                }
                .encode(),
                PendingCommitValue::encode(commit)?,
            )?;
            self.put(
                MaterializedSequenceKey::encode(),
                MaterializedSequenceValue {
                    sequence: commit.last_sequence,
                }
                .encode(),
            )?;
            Ok(())
        }
    }

    /// Stages a schema under the given ID until commit.
    fn put_schema(&self, id: i32, schema: &Schema) -> Result<()> {
        self.put(SchemaKey { id }.encode(), SchemaValue::encode(schema)?)?;
        Ok(())
    }

    /// Stages a sort order under the given ID until commit.
    fn put_sort_order(&self, id: i64, sort_order: &SortOrder) -> Result<()> {
        self.put(
            SortOrderKey { id }.encode(),
            SortOrderValue::encode(sort_order)?,
        )?;
        Ok(())
    }

    /// Stages the current schema ID until commit.
    fn put_current_schema_id(&self, id: i32) -> Result<()> {
        self.put(
            CurrentSchemaIdKey::encode(),
            CurrentSchemaIdValue { id }.encode(),
        )?;
        Ok(())
    }

    /// Stages the default sort order ID until commit.
    fn put_default_sort_order_id(&self, id: i64) -> Result<()> {
        self.put(
            DefaultSortOrderIdKey::encode(),
            DefaultSortOrderIdValue { id }.encode(),
        )?;
        Ok(())
    }

    /// Stages the table UUID. The caller must commit the transaction to persist it.
    fn put_table_uuid(&self, uuid: Uuid) -> Result<()> {
        self.put(TableUuidKey::encode(), TableUuidValue { uuid }.encode())?;
        Ok(())
    }
}

impl<T: DbTransactionOps + Sync + ?Sized> TableWrite for T {}
