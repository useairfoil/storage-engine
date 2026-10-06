use std::sync::Arc;

use bytes::Bytes;
use iceberg::spec::{PartitionSpecRef, SchemaRef, SortOrderRef};
use object_store::{ObjectStore, path::Path};
use se_common::object_store::{ObjectStoreStorageProtocol, ObjectVersion};
use serde::{Deserialize, Serialize};
use slatedb_txn_obj::{
    DirtyObject, ObjectCodec, SimpleTransactionalObject, TransactionalObject,
    TransactionalObjectError, TransactionalStorageProtocol,
};
use uuid::Uuid;

/// Metadata of a table, as stored in the meta store.
///
/// This contains a subset of the fields of the Iceberg table metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableMetadata {
    /// The uuid of the table.
    pub uuid: Uuid,
    /// The current schema of the table.
    pub schema: SchemaRef,
    /// The id of the current schema of the table.
    pub schema_id: i32,
    /// The default partition spec of the table.
    pub partition_spec: PartitionSpecRef,
    /// The id of the default partition spec of the table.
    pub partition_spec_id: i32,
    /// The default sort order of the table.
    pub sort_order: SortOrderRef,
    /// The id of the default sort order of the table.
    pub sort_order_id: i64,
}

/// A table, as persisted in the meta store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Table {
    pub metadata: TableMetadata,
    pub shards: Vec<TableShard>,
    /// None until the first WAL range has been materialized.
    pub materialization: Option<MaterializationState>,
}

/// Files belonging to one shard of a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableShard {
    /// Object-store path to the Puffin file containing the shard's deletion vector.
    pub deletion_vector_file: String,
    /// Object-store paths to the shard's data files.
    pub data_files: Vec<String>,
}

/// Progress of WAL materialization for a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializationState {
    pub last_applied_wal_sequence: u64,
}

/// The error type for table store operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Transactional object error.
    #[error("transactional object error: {0}")]
    TransactionalObject(#[from] slatedb_txn_obj::TransactionalObjectError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A store for a table.
///
/// The store persists a single table in a single object,
/// using compare-and-swap updates.
pub struct TableStore {
    inner: Arc<dyn TransactionalStorageProtocol<Table, ObjectVersion>>,
    object_store: Arc<dyn ObjectStore>,
}

/// A table, as stored in the meta store.
pub struct StoredTable {
    inner: SimpleTransactionalObject<Table, ObjectVersion>,
    object_store: Arc<dyn ObjectStore>,
}

struct TableCodec;

impl From<&iceberg::spec::TableMetadata> for TableMetadata {
    fn from(metadata: &iceberg::spec::TableMetadata) -> Self {
        Self {
            uuid: metadata.uuid(),
            schema: metadata.current_schema().clone(),
            schema_id: metadata.current_schema_id(),
            partition_spec: metadata.default_partition_spec().clone(),
            partition_spec_id: metadata.default_partition_spec_id(),
            sort_order: metadata.default_sort_order().clone(),
            sort_order_id: metadata.default_sort_order_id(),
        }
    }
}

impl From<iceberg::spec::TableMetadata> for TableMetadata {
    fn from(metadata: iceberg::spec::TableMetadata) -> Self {
        (&metadata).into()
    }
}

impl TableStore {
    /// Creates a new table store that persists the table at the given path.
    pub fn new(object_store: Arc<dyn ObjectStore>, path: Path) -> Self {
        let inner: Arc<dyn TransactionalStorageProtocol<Table, ObjectVersion>> = Arc::new(
            ObjectStoreStorageProtocol::new(object_store.clone(), path, Box::new(TableCodec)),
        );
        Self {
            inner,
            object_store,
        }
    }
}

impl StoredTable {
    /// Store a new table with no shards or materialization progress.
    pub async fn init(store: Arc<TableStore>, metadata: TableMetadata) -> Result<Self> {
        let inner = SimpleTransactionalObject::init(
            Arc::clone(&store.inner),
            Table {
                metadata,
                shards: Vec::new(),
                materialization: None,
            },
        )
        .await?;
        Ok(Self {
            inner,
            object_store: store.object_store.clone(),
        })
    }

    /// Load the latest table from the supplied table store.
    ///
    /// If there is no table at the table store's path returns `None`.
    pub async fn try_load(store: Arc<TableStore>) -> Result<Option<Self>> {
        let Some(inner) = SimpleTransactionalObject::try_load(Arc::clone(&store.inner)).await?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            inner,
            object_store: store.object_store.clone(),
        }))
    }

    /// Returns the persisted table.
    pub fn table(&self) -> &Table {
        self.inner.object()
    }

    /// Returns the metadata of the table.
    pub fn metadata(&self) -> &TableMetadata {
        &self.table().metadata
    }

    /// Returns an object store rooted at this table's UUID within its catalog.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::new(object_store::prefix::PrefixStore::new(
            self.object_store.clone(),
            self.metadata().uuid.to_string(),
        ))
    }

    /// Returns a `DirtyObject` with the current version and table, which can be
    /// modified locally and passed to `update` to persist mutations durably.
    pub fn prepare_dirty(&self) -> Result<DirtyObject<Table, ObjectVersion>> {
        Ok(self.inner.prepare_dirty()?)
    }

    /// Refresh the in-memory view of the table with the latest state stored durably.
    pub async fn refresh(&mut self) -> Result<&Table> {
        Ok(self.inner.refresh().await?)
    }

    /// Transactionally update the table.
    pub async fn update(&mut self, dirty: DirtyObject<Table, ObjectVersion>) -> Result<()> {
        Ok(self.inner.update(dirty).await?)
    }

    /// Transactionally update the table using the supplied mutator, if the
    /// mutator returns `Some`. This fn will indefinitely retry the mutation on a
    /// write conflict by refreshing and re-applying the mutation.
    pub async fn maybe_apply_update<F>(&mut self, mutator: F) -> Result<()>
    where
        F: Fn(
                &SimpleTransactionalObject<Table, ObjectVersion>,
            ) -> std::result::Result<
                Option<DirtyObject<Table, ObjectVersion>>,
                TransactionalObjectError,
            > + Send
            + Sync,
    {
        Ok(self.inner.maybe_apply_update(mutator).await?)
    }
}

impl ObjectCodec<Table> for TableCodec {
    fn encode(&self, value: &Table) -> Bytes {
        // PANIC: serialization of this type cannot fail.
        Bytes::from(serde_json::to_vec(value).expect("failed to serialize table"))
    }

    fn decode(
        &self,
        bytes: &Bytes,
    ) -> std::result::Result<Table, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Type,
        UnboundPartitionSpec,
    };
    use object_store::memory::InMemory;

    use super::*;

    const PATH: &str = "/root/table";

    fn store() -> Arc<TableStore> {
        Arc::new(TableStore::new(Arc::new(InMemory::new()), Path::from(PATH)))
    }

    fn iceberg_table_metadata() -> iceberg::spec::TableMetadata {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        TableMetadataBuilder::new(
            schema,
            UnboundPartitionSpec::default(),
            SortOrder::unsorted_order(),
            "/warehouse/table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    fn table_uuid() -> Uuid {
        Uuid::from_u128(1)
    }

    fn table_metadata() -> TableMetadata {
        let mut metadata = TableMetadata::from(iceberg_table_metadata());
        metadata.uuid = table_uuid();
        metadata
    }

    fn json(metadata: &TableMetadata) -> String {
        serde_json::to_string_pretty(metadata).unwrap()
    }

    #[test]
    fn from_iceberg_table_metadata() {
        let iceberg_metadata = iceberg_table_metadata();
        let metadata = TableMetadata::from(&iceberg_metadata);
        assert_eq!(metadata.uuid, iceberg_metadata.uuid());

        let metadata = table_metadata();
        insta::assert_snapshot!(json(&metadata), @r#"
        {
          "uuid": "00000000-0000-0000-0000-000000000001",
          "schema": {
            "schema-id": 0,
            "type": "struct",
            "fields": [
              {
                "id": 1,
                "name": "id",
                "required": true,
                "type": "long"
              }
            ]
          },
          "schema_id": 0,
          "partition_spec": {
            "spec-id": 0,
            "fields": []
          },
          "partition_spec_id": 0,
          "sort_order": {
            "order-id": 0,
            "fields": []
          },
          "sort_order_id": 0
        }
        "#);

        let roundtripped: TableMetadata = serde_json::from_str(&json(&metadata)).unwrap();
        assert_eq!(roundtripped, metadata);
    }

    #[tokio::test]
    async fn init_and_try_load() {
        let store = store();
        insta::assert_compact_debug_snapshot!(
            StoredTable::try_load(store.clone())
                .await
                .unwrap()
                .map(|table| table.metadata().clone()),
            @"None"
        );

        StoredTable::init(store.clone(), table_metadata())
            .await
            .unwrap();
        let table = StoredTable::try_load(store).await.unwrap().unwrap();
        assert_eq!(table.table().shards, Vec::new());
        assert_eq!(table.table().materialization, None);
        insta::assert_snapshot!(json(table.metadata()), @r#"
        {
          "uuid": "00000000-0000-0000-0000-000000000001",
          "schema": {
            "schema-id": 0,
            "type": "struct",
            "fields": [
              {
                "id": 1,
                "name": "id",
                "required": true,
                "type": "long"
              }
            ]
          },
          "schema_id": 0,
          "partition_spec": {
            "spec-id": 0,
            "fields": []
          },
          "partition_spec_id": 0,
          "sort_order": {
            "order-id": 0,
            "fields": []
          },
          "sort_order_id": 0
        }
        "#);
    }

    #[tokio::test]
    async fn persists_shards_and_materialization_progress() {
        let store = store();
        let mut table = StoredTable::init(store.clone(), table_metadata())
            .await
            .unwrap();
        let mut dirty = table.prepare_dirty().unwrap();
        dirty.value.shards.push(TableShard {
            deletion_vector_file: "shards/0/deletions.puffin".into(),
            data_files: vec!["shards/0/a.parquet".into(), "shards/0/b.parquet".into()],
        });
        dirty.value.materialization = Some(MaterializationState {
            last_applied_wal_sequence: 7,
        });
        table.update(dirty).await.unwrap();

        let reloaded = StoredTable::try_load(store).await.unwrap().unwrap();
        assert_eq!(reloaded.metadata(), &table_metadata());
        assert_eq!(reloaded.table(), table.table());
        insta::assert_snapshot!(serde_json::to_string_pretty(reloaded.table()).unwrap(), @r#"
        {
          "metadata": {
            "uuid": "00000000-0000-0000-0000-000000000001",
            "schema": {
              "schema-id": 0,
              "type": "struct",
              "fields": [
                {
                  "id": 1,
                  "name": "id",
                  "required": true,
                  "type": "long"
                }
              ]
            },
            "schema_id": 0,
            "partition_spec": {
              "spec-id": 0,
              "fields": []
            },
            "partition_spec_id": 0,
            "sort_order": {
              "order-id": 0,
              "fields": []
            },
            "sort_order_id": 0
          },
          "shards": [
            {
              "deletion_vector_file": "shards/0/deletions.puffin",
              "data_files": [
                "shards/0/a.parquet",
                "shards/0/b.parquet"
              ]
            }
          ],
          "materialization": {
            "last_applied_wal_sequence": 7
          }
        }
        "#);
    }

    #[tokio::test]
    async fn update_conflict() {
        let store = store();
        StoredTable::init(store.clone(), table_metadata())
            .await
            .unwrap();
        let mut table1 = StoredTable::try_load(store.clone()).await.unwrap().unwrap();
        let mut table2 = StoredTable::try_load(store).await.unwrap().unwrap();

        table1
            .update(table1.prepare_dirty().unwrap())
            .await
            .unwrap();

        insta::assert_compact_debug_snapshot!(
            table2.update(table2.prepare_dirty().unwrap()).await,
            @r#"Err(TransactionalObject(ObjectVersionExists))"#
        );

        table2.refresh().await.unwrap();
        table2
            .update(table2.prepare_dirty().unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn maybe_apply_update_retries_conflict() {
        let store = store();
        StoredTable::init(store.clone(), table_metadata())
            .await
            .unwrap();
        let mut table = StoredTable::try_load(store.clone()).await.unwrap().unwrap();
        let mut other = StoredTable::try_load(store).await.unwrap().unwrap();

        other.update(other.prepare_dirty().unwrap()).await.unwrap();

        table
            .maybe_apply_update(|table| {
                let dirty = table.prepare_dirty()?;
                Ok(Some(dirty))
            })
            .await
            .unwrap();
        insta::assert_snapshot!(json(table.metadata()), @r#"
        {
          "uuid": "00000000-0000-0000-0000-000000000001",
          "schema": {
            "schema-id": 0,
            "type": "struct",
            "fields": [
              {
                "id": 1,
                "name": "id",
                "required": true,
                "type": "long"
              }
            ]
          },
          "schema_id": 0,
          "partition_spec": {
            "spec-id": 0,
            "fields": []
          },
          "partition_spec_id": 0,
          "sort_order": {
            "order-id": 0,
            "fields": []
          },
          "sort_order_id": 0
        }
        "#);
    }
}
