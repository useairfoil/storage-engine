use std::{future::Future, ops::Bound};

use bytes::Bytes;
use iceberg::spec::{Schema, SortOrder};
use slatedb::DbReadOps;
use uuid::Uuid;

use super::{
    MaterializedDeletionVector, PendingCommit, PublicationIntent, Result, RowLocation,
    encoding::{
        CurrentSchemaIdKey, CurrentSchemaIdValue, DefaultSortOrderIdKey, DefaultSortOrderIdValue,
        MaterializedSequenceKey, MaterializedSequenceValue, PendingCommitKey, PendingCommitValue,
        PublicationIntentKey, PublicationIntentValue, PublishedSequenceKey, RecordType, SchemaKey,
        SchemaValue, SortOrderKey, SortOrderValue, TableUuidKey, TableUuidValue,
    },
};

/// Typed table reads. Use a snapshot or transaction for consistent multi-read operations.
pub trait TableRead: DbReadOps + Sync {
    /// Latest accepted row location, which may precede catalog publication.
    fn get_row_location(
        &self,
        key: &[u8],
    ) -> impl Future<Output = Result<Option<RowLocation>>> + Send {
        async move {
            self.get(RecordType::RowLocation.key(key))
                .await?
                .map(|value| serde_json::from_slice(&value).map_err(Into::into))
                .transpose()
        }
    }

    /// Serialized cumulative Roaring treemap. Missing means no deleted positions.
    fn get_deletion_bitmap(
        &self,
        file: &str,
    ) -> impl Future<Output = Result<Option<Bytes>>> + Send {
        async move {
            Ok(self
                .get(RecordType::DeletionBitmap.key(file.as_bytes()))
                .await?)
        }
    }

    fn get_deletion_vector(
        &self,
        file: &str,
    ) -> impl Future<Output = Result<Option<MaterializedDeletionVector>>> + Send {
        async move {
            self.get(RecordType::DeletionVector.key(file.as_bytes()))
                .await?
                .map(|value| serde_json::from_slice(&value).map_err(Into::into))
                .transpose()
        }
    }

    /// Schema establishing the immutable identifier fields and key encoding.
    fn get_upsert_schema_id(&self) -> impl Future<Output = Result<Option<i32>>> + Send {
        async {
            self.get(RecordType::UpsertSchemaId.key(&[]))
                .await?
                .map(|value| CurrentSchemaIdValue::decode(&value).map(|value| value.id))
                .transpose()
        }
    }

    /// Returns the WAL frontier confirmed as published to Iceberg.
    fn get_published_sequence(&self) -> impl Future<Output = Result<Option<u64>>> + Send {
        async {
            self.get(PublishedSequenceKey::encode())
                .await?
                .map(|value| MaterializedSequenceValue::decode(&value).map(|value| value.sequence))
                .transpose()
        }
    }

    /// Returns publication work whose catalog outcome must be reconciled before proceeding.
    fn get_publication_intent(
        &self,
    ) -> impl Future<Output = Result<Option<PublicationIntent>>> + Send {
        async {
            self.get(PublicationIntentKey::encode())
                .await?
                .map(|value| PublicationIntentValue::decode(&value))
                .transpose()
        }
    }

    /// Returns the accepted WAL frontier. Ensure durability before using it to acknowledge WAL.
    fn get_materialized_sequence(&self) -> impl Future<Output = Result<Option<u64>>> + Send {
        async {
            self.get(MaterializedSequenceKey::encode())
                .await?
                .map(|value| MaterializedSequenceValue::decode(&value).map(|value| value.sequence))
                .transpose()
        }
    }

    /// Reads at most `limit` publication records in WAL order, strictly after `after`.
    /// Use a snapshot to freeze a consistent publication prefix.
    fn read_pending_commits(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<PendingCommit>>> + Send {
        async move {
            let mut commits = Vec::new();
            if limit == 0 {
                return Ok(commits);
            }
            let lower = after
                .map(|sequence| Bound::Excluded(Bytes::copy_from_slice(&sequence.to_be_bytes())))
                .unwrap_or(Bound::Unbounded);
            let mut iter = self
                .scan_prefix(
                    PendingCommitKey::prefix(),
                    (lower, Bound::<Bytes>::Unbounded),
                )
                .await?;
            while commits.len() < limit {
                let Some(entry) = iter.next().await? else {
                    break;
                };
                commits.push(PendingCommitValue::decode(&entry.value)?);
            }
            Ok(commits)
        }
    }

    /// Returns the schema stored under the given ID, or `None` if absent.
    fn get_schema(&self, id: i32) -> impl Future<Output = Result<Option<Schema>>> + Send {
        async move {
            self.get(SchemaKey { id }.encode())
                .await?
                .map(|value| SchemaValue::decode(&value))
                .transpose()
        }
    }

    /// Returns the sort order stored under the given ID, or `None` if absent.
    fn get_sort_order(&self, id: i64) -> impl Future<Output = Result<Option<SortOrder>>> + Send {
        async move {
            self.get(SortOrderKey { id }.encode())
                .await?
                .map(|value| SortOrderValue::decode(&value))
                .transpose()
        }
    }

    /// Returns the current schema ID, or `None` if uninitialized.
    fn get_current_schema_id(&self) -> impl Future<Output = Result<Option<i32>>> + Send {
        async {
            self.get(CurrentSchemaIdKey::encode())
                .await?
                .map(|value| CurrentSchemaIdValue::decode(&value).map(|value| value.id))
                .transpose()
        }
    }

    /// Returns the default sort order ID, or `None` if uninitialized.
    fn get_default_sort_order_id(&self) -> impl Future<Output = Result<Option<i64>>> + Send {
        async {
            self.get(DefaultSortOrderIdKey::encode())
                .await?
                .map(|value| DefaultSortOrderIdValue::decode(&value).map(|value| value.id))
                .transpose()
        }
    }

    /// Returns the table UUID, or `None` if it has not been initialized.
    fn get_table_uuid(&self) -> impl Future<Output = Result<Option<Uuid>>> + Send {
        async {
            self.get(TableUuidKey::encode())
                .await?
                .map(|value| TableUuidValue::decode(&value).map(|value| value.uuid))
                .transpose()
        }
    }
}

impl<T: DbReadOps + Sync + ?Sized> TableRead for T {}
