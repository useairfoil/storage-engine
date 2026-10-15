use std::future::Future;

use iceberg::spec::{Schema, SortOrder, TableMetadata};
use slatedb::DbTransactionOps;
use uuid::Uuid;

use super::{
    Error, MaterializedDeletionVector, PendingCommit, PublicationIntent, Result, RowLocation,
    TableRead,
    encoding::{
        CurrentSchemaIdKey, CurrentSchemaIdValue, DefaultSortOrderIdKey, DefaultSortOrderIdValue,
        MaterializedSequenceKey, MaterializedSequenceValue, PendingCommitKey, PendingCommitValue,
        PublicationIntentKey, PublicationIntentValue, PublishedSequenceKey, RecordType, SchemaKey,
        SchemaValue, SortOrderKey, SortOrderValue, TableUuidKey, TableUuidValue,
    },
};

/// Typed table mutations staged in a transaction.
pub trait TableWrite: DbTransactionOps + Sync {
    /// Stages only catalog fields used by ingestion, materialization, and publication.
    ///
    /// Historical indexed schemas and sort orders remain available to queued WAL entries.
    fn put_table_metadata(&self, metadata: &TableMetadata) -> Result<()> {
        self.put(
            RecordType::TableLocation.key(&[]),
            serde_json::to_vec(metadata.location())?,
        )?;
        self.put(
            RecordType::FormatVersion.key(&[]),
            serde_json::to_vec(&metadata.format_version())?,
        )?;
        self.put(
            RecordType::DefaultPartitionSpecId.key(&[]),
            serde_json::to_vec(&metadata.default_partition_spec_id())?,
        )?;
        self.put(
            RecordType::CurrentSnapshotId.key(&[]),
            serde_json::to_vec(&metadata.current_snapshot_id())?,
        )?;
        // Snapshot has no public serde implementation in the Iceberg SDK.
        let snapshots: Vec<_> = metadata
            .snapshots()
            .map(|snapshot| {
                serde_json::json!({
                    "snapshot-id": snapshot.snapshot_id(),
                    "parent-snapshot-id": snapshot.parent_snapshot_id(),
                    "sequence-number": snapshot.sequence_number(),
                    "timestamp-ms": snapshot.timestamp_ms(),
                    "manifest-list": snapshot.manifest_list(),
                    "summary": snapshot.summary(),
                    "schema-id": snapshot.schema_id(),
                    "first-row-id": snapshot.first_row_id(),
                    "added-rows": snapshot.added_rows_count(),
                    "key-id": snapshot.encryption_key_id(),
                })
            })
            .collect();
        self.put(
            RecordType::Snapshots.key(&[]),
            serde_json::to_vec(&snapshots)?,
        )?;
        self.put(
            RecordType::LastSequenceNumber.key(&[]),
            serde_json::to_vec(&metadata.last_sequence_number())?,
        )?;
        self.put(
            RecordType::NextRowId.key(&[]),
            serde_json::to_vec(&metadata.next_row_id())?,
        )?;
        // Keep only settings read by our Parquet writer and publication planner.
        let properties = metadata
            .properties()
            .iter()
            .filter(|(key, _)| {
                matches!(
                    key.as_str(),
                    "write.target-file-size-bytes"
                        | "write.parquet.content-defined-chunking.enabled"
                        | "write.parquet.content-defined-chunking.min-chunk-size"
                        | "write.parquet.content-defined-chunking.max-chunk-size"
                        | "write.parquet.content-defined-chunking.norm-level"
                        | "write.summary.partition-limit"
                        | "encryption.key-id"
                )
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        self.put_table_properties(&properties)?;
        for spec in metadata.partition_specs_iter() {
            self.put(
                RecordType::PartitionSpec.key(&spec.spec_id().to_be_bytes()),
                serde_json::to_vec(spec)?,
            )?;
        }

        self.put_current_schema_id(metadata.current_schema_id())?;
        self.put_default_sort_order_id(metadata.default_sort_order_id())?;

        for schema in metadata.schemas_iter() {
            self.put_schema(schema.schema_id(), schema)?;
        }

        for order in metadata.sort_orders_iter() {
            self.put_sort_order(order.order_id, order)?;
        }

        Ok(())
    }

    /// Stages selected writer and publication settings.
    fn put_table_properties(
        &self,
        properties: &std::collections::HashMap<String, String>,
    ) -> Result<()> {
        self.put(
            RecordType::TableProperties.key(&[]),
            serde_json::to_vec(properties)?,
        )?;
        Ok(())
    }

    /// Stage alongside the pending commit and materialized frontier, never separately.
    fn put_row_location(&self, key: &[u8], location: &RowLocation) -> Result<()> {
        self.put(
            RecordType::RowLocation.key(key),
            serde_json::to_vec(location)?,
        )?;
        Ok(())
    }

    /// Stage the cumulative bitmap and its immutable publication descriptor together.
    fn put_deletion_vector(
        &self,
        vector: &MaterializedDeletionVector,
        bitmap: &[u8],
    ) -> Result<()> {
        self.put(
            RecordType::DeletionBitmap.key(vector.data_file_path.as_bytes()),
            bitmap,
        )?;
        self.put(
            RecordType::DeletionVector.key(vector.data_file_path.as_bytes()),
            serde_json::to_vec(vector)?,
        )?;
        Ok(())
    }

    fn put_upsert_schema_id(&self, id: i32) -> Result<()> {
        self.put(
            RecordType::UpsertSchemaId.key(&[]),
            CurrentSchemaIdValue { id }.encode(),
        )?;
        Ok(())
    }

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
