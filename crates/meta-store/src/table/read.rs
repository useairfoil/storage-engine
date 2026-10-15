use std::{collections::HashMap, future::Future, ops::Bound};

use bytes::Bytes;
use iceberg::spec::{FormatVersion, PartitionSpec, Schema, SortOrder, TableMetadata};
use serde::de::DeserializeOwned;
use slatedb::DbReadOps;
use uuid::Uuid;

use super::{
    Error, MaterializedDeletionVector, PendingCommit, PublicationIntent, Result, RowLocation,
    encoding::{
        CurrentSchemaIdKey, CurrentSchemaIdValue, DefaultSortOrderIdKey, DefaultSortOrderIdValue,
        MaterializedSequenceKey, MaterializedSequenceValue, PendingCommitKey, PendingCommitValue,
        PublicationIntentKey, PublicationIntentValue, PublishedSequenceKey, RecordType, SchemaKey,
        SchemaValue, SortOrderKey, SortOrderValue, TableUuidKey, TableUuidValue,
    },
};

/// Typed table reads. Use a snapshot or transaction for consistent multi-read operations.
pub trait TableRead: DbReadOps + Sync {
    fn get_table_location(&self) -> impl Future<Output = Result<Option<String>>> + Send {
        read_record(self, RecordType::TableLocation)
    }

    fn get_format_version(&self) -> impl Future<Output = Result<Option<FormatVersion>>> + Send {
        read_record(self, RecordType::FormatVersion)
    }

    fn get_default_partition_spec_id(&self) -> impl Future<Output = Result<Option<i32>>> + Send {
        read_record(self, RecordType::DefaultPartitionSpecId)
    }

    fn get_partition_spec(
        &self,
        id: i32,
    ) -> impl Future<Output = Result<Option<PartitionSpec>>> + Send {
        async move {
            self.get(RecordType::PartitionSpec.key(&id.to_be_bytes()))
                .await?
                .map(|value| serde_json::from_slice(&value).map_err(Into::into))
                .transpose()
        }
    }

    /// Selected writer and publication settings; never storage credentials.
    fn get_table_properties(&self) -> impl Future<Output = Result<HashMap<String, String>>> + Send {
        async {
            Ok(read_record(self, RecordType::TableProperties)
                .await?
                .unwrap_or_default())
        }
    }

    fn get_current_snapshot_id(&self) -> impl Future<Output = Result<Option<i64>>> + Send {
        async {
            Ok(
                read_record::<Option<i64>>(self, RecordType::CurrentSnapshotId)
                    .await?
                    .flatten(),
            )
        }
    }

    fn get_last_sequence_number(&self) -> impl Future<Output = Result<Option<i64>>> + Send {
        read_record(self, RecordType::LastSequenceNumber)
    }

    fn get_next_row_id(&self) -> impl Future<Output = Result<Option<u64>>> + Send {
        read_record(self, RecordType::NextRowId)
    }

    /// Assembles a transient SDK planning view from typed records.
    /// This is not canonical catalog metadata and must never replace it in the catalog.
    fn publication_metadata(&self) -> impl Future<Output = Result<TableMetadata>> + Send {
        async {
            let schema_id = self
                .get_current_schema_id()
                .await?
                .ok_or(Error::TableMetadataMissing)?;
            // Resolve the current schema through the same index used by ingestion.
            self.get_schema(schema_id)
                .await?
                .ok_or(Error::TableMetadataMissing)?;
            let sort_order_id = self
                .get_default_sort_order_id()
                .await?
                .ok_or(Error::TableMetadataMissing)?;
            let sort_order = self
                .get_sort_order(sort_order_id)
                .await?
                .ok_or(Error::TableMetadataMissing)?;
            let version = self
                .get_format_version()
                .await?
                .ok_or(Error::TableMetadataMissing)?;
            let version = match version {
                FormatVersion::V1 => 1,
                FormatVersion::V2 => 2,
                FormatVersion::V3 => 3,
            };
            // These allocation/log fields are required by the SDK's metadata decoder,
            // but publication never updates schemas, specs, or metadata logs.
            Ok(serde_json::from_value(serde_json::json!({
                "format-version": version,
                "table-uuid": self.get_table_uuid().await?.ok_or(Error::TableUuidMissing)?,
                "location": self.get_table_location().await?.ok_or(Error::TableMetadataMissing)?,
                "last-sequence-number": self.get_last_sequence_number().await?.ok_or(Error::TableMetadataMissing)?,
                "last-updated-ms": 0,
                "last-column-id": 0,
                "schemas": read_records::<Schema>(self, RecordType::Schema).await?,
                "current-schema-id": schema_id,
                "partition-specs": read_records::<PartitionSpec>(self, RecordType::PartitionSpec).await?,
                "default-spec-id": self.get_default_partition_spec_id().await?.ok_or(Error::TableMetadataMissing)?,
                "last-partition-id": 0,
                "sort-orders": [sort_order],
                "default-sort-order-id": sort_order_id,
                "properties": self.get_table_properties().await?,
                "current-snapshot-id": self.get_current_snapshot_id().await?,
                "snapshots": read_record::<Vec<serde_json::Value>>(self, RecordType::Snapshots).await?.unwrap_or_default(),
                "next-row-id": self.get_next_row_id().await?.ok_or(Error::TableMetadataMissing)?,
            }))?)
        }
    }

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

async fn read_record<T: DeserializeOwned>(
    reader: &(impl DbReadOps + Sync + ?Sized),
    record_type: RecordType,
) -> Result<Option<T>> {
    reader
        .get(record_type.key(&[]))
        .await?
        .map(|value| serde_json::from_slice(&value).map_err(Into::into))
        .transpose()
}

async fn read_records<T: DeserializeOwned + Send>(
    reader: &(impl DbReadOps + Sync + ?Sized),
    record_type: RecordType,
) -> Result<Vec<T>> {
    let mut records = Vec::new();
    let mut iter = reader
        .scan_prefix(
            record_type.key(&[]),
            (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
        )
        .await?;
    while let Some(entry) = iter.next().await? {
        records.push(serde_json::from_slice(&entry.value)?);
    }
    Ok(records)
}
