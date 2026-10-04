//! Materialization of the ingestion WAL into Parquet files and deletion vectors.

use std::{sync::Arc, time::Duration};

use buffer::{Consumer, ConsumerConfig};
pub use se_ingestion::Clock;
use se_meta_store::{
    catalog::{CatalogName, CatalogStore, NamespaceIdent, TableIdent},
    table::StoredTable,
};

/// The error type for materialization operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The requested catalog does not exist.
    #[error("catalog not found: {0}")]
    CatalogNotFound(CatalogName),
    /// The requested table is not linked in the meta store.
    #[error("table not found: {0}")]
    TableNotFound(TableIdent),
    /// Failed to access the catalog or its stored table metadata.
    #[error("catalog store error: {0}")]
    CatalogStore(#[from] se_meta_store::catalog::Error),
    /// Failed to read or acknowledge the WAL.
    #[error("wal error: {0}")]
    Wal(#[from] buffer::Error),
    /// The WAL head was no longer available while reading the captured range.
    #[error("wal head {0} is no longer available")]
    WalHeadUnavailable(u64),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Options for one materialization pass.
#[derive(Debug, Default, Clone, Copy)]
pub struct MaterializationOptions;

/// Creates table-specific materializers.
#[derive(Clone)]
pub struct Materializer {
    catalog_store: CatalogStore,
    clock: Arc<dyn Clock>,
}

/// Holds an existing table for future WAL materialization.
#[derive(Clone)]
pub struct TableMaterializer {
    table: Arc<StoredTable>,
}

impl Materializer {
    pub fn new(catalog_store: CatalogStore, clock: Arc<dyn Clock>) -> Self {
        Self {
            catalog_store,
            clock,
        }
    }

    /// Returns the clock supplied to this materializer.
    pub fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
    }

    /// Creates a materializer for a table already linked in the meta store.
    pub async fn for_table(
        &self,
        catalog: CatalogName,
        namespace: NamespaceIdent,
        table_name: String,
    ) -> Result<TableMaterializer> {
        let table_ident = TableIdent::new(namespace, table_name);
        let stored_catalog = self
            .catalog_store
            .get(catalog.clone())
            .await?
            .ok_or(Error::CatalogNotFound(catalog))?;
        let table = stored_catalog
            .try_load_table(table_ident.clone())
            .await?
            .ok_or(Error::TableNotFound(table_ident))?;
        Ok(TableMaterializer {
            table: Arc::new(table),
        })
    }
}

impl std::fmt::Debug for Materializer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Materializer").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for TableMaterializer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableMaterializer")
            .field("table_uuid", &self.table.metadata().uuid)
            .finish()
    }
}

impl TableMaterializer {
    /// Returns the stored table to use for WAL reads.
    pub fn table(&self) -> &StoredTable {
        &self.table
    }

    /// Read the WAL through its current head, then acknowledge the consumed range.
    /// No Parquet files or deletion vectors are written yet.
    pub async fn run_materialization(&self, _options: MaterializationOptions) -> Result<()> {
        let manifest = se_ingestion::wal_tailer(&self.table).manifest().await?;
        let Some(head) = manifest.entries().last().map(|entry| entry.sequence) else {
            return Ok(());
        };

        self.consume_through(head).await
    }

    async fn consume_through(&self, head: u64) -> Result<()> {
        let object_store = self.table.object_store();
        let config = ConsumerConfig {
            manifest_path: se_ingestion::WAL_MANIFEST_PATH.into(),
            data_path_prefix: se_ingestion::WAL_DATA_PATH.into(),
            gc_interval: Duration::from_mins(5),
            gc_grace_period: Duration::from_mins(10),
        };
        let mut consumer = Consumer::with_object_store(config, object_store, None).await?;

        let result = async {
            loop {
                let batch = consumer
                    .next_batch()
                    .await?
                    .ok_or(Error::WalHeadUnavailable(head))?;
                if batch.sequence > head {
                    return Err(Error::WalHeadUnavailable(head));
                }

                // Materialization of the batch will happen here.
                if batch.sequence == head {
                    break;
                }
            }

            consumer.ack_through(head).await?;
            Ok(())
        }
        .await;
        let close_result = consumer.close().await;
        result?;
        close_result?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use buffer::{Producer, ProducerConfig};
    use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Type};
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
    use se_ingestion::SystemClock;
    use se_meta_store::{
        catalog::{CatalogConfig, RestCatalogConfig},
        table::{TableMetadata, TableStore},
    };
    use se_secret_store::memory::InMemorySecretStore;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn for_table_requires_an_existing_catalog_and_linked_table() {
        let store = CatalogStore::new(
            Arc::new(InMemorySecretStore::new()),
            Arc::new(InMemory::new()),
        );
        let catalog = CatalogName::new("example").unwrap();
        let namespace = NamespaceIdent::from_strs(["analytics", "events"]).unwrap();
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let materializer = Materializer::new(store.clone(), Arc::clone(&clock));
        assert!(Arc::ptr_eq(&materializer.clock(), &clock));

        insta::assert_snapshot!(
            materializer
                .for_table(catalog.clone(), namespace.clone(), "page_views".into())
                .await
                .unwrap_err(),
            @"catalog not found: catalogs/example"
        );

        store
            .create(
                catalog.clone(),
                CatalogConfig::Rest(RestCatalogConfig {
                    uri: "https://rest.catalog.example.com".into(),
                    warehouse: None,
                    properties: HashMap::new(),
                }),
            )
            .await
            .unwrap();

        insta::assert_snapshot!(
            materializer
                .for_table(catalog, namespace, "page_views".into())
                .await
                .unwrap_err(),
            @"table not found: analytics.events.page_views"
        );
    }

    async fn table_materializer() -> TableMaterializer {
        let uuid = Uuid::from_u128(7);
        let store = Arc::new(TableStore::new(
            Arc::new(InMemory::new()),
            Path::from(format!("{uuid}/metadata.json")),
        ));
        let metadata = TableMetadata {
            uuid,
            schema: Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    ])
                    .build()
                    .unwrap(),
            ),
            schema_id: 0,
            partition_spec: Arc::new(PartitionSpec::unpartition_spec()),
            partition_spec_id: 0,
            sort_order: Arc::new(SortOrder::unsorted_order()),
            sort_order_id: 0,
        };
        let table = StoredTable::init(store, metadata).await.unwrap();
        TableMaterializer {
            table: Arc::new(table),
        }
    }

    fn producer(store: Arc<dyn ObjectStore>) -> Producer {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        Producer::new(
            ProducerConfig {
                data_path_prefix: se_ingestion::WAL_DATA_PATH.into(),
                manifest_path: se_ingestion::WAL_MANIFEST_PATH.into(),
                flush_interval: Duration::from_secs(60),
                flush_size_bytes: 1024 * 1024,
                max_buffered_inputs: 10,
                batch_compression: buffer::CompressionType::None,
            },
            store,
            schema,
            Arc::new(SystemClock),
        )
        .unwrap()
    }

    async fn append(producer: &Producer, value: i64) {
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "id",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap();
        producer.produce(batch, Default::default()).await.unwrap();
        producer.flush().await.unwrap();
    }

    async fn manifest(table: &TableMaterializer) -> buffer::ManifestView {
        se_ingestion::wal_tailer(table.table())
            .manifest()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn materializes_empty_and_retained_wal() {
        let table = table_materializer().await;
        table
            .run_materialization(MaterializationOptions)
            .await
            .unwrap();
        let writer = producer(table.table().object_store());
        append(&writer, 17).await;
        append(&writer, 23).await;
        assert!(
            manifest(&table)
                .await
                .entries()
                .iter()
                .all(|entry| entry.location.starts_with("ingest/wal/"))
        );

        table
            .run_materialization(MaterializationOptions)
            .await
            .unwrap();
        let view = manifest(&table).await;
        assert_eq!(view.next_sequence, 2);
        assert!(view.entries().is_empty());
        table
            .run_materialization(MaterializationOptions)
            .await
            .unwrap();
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn failed_fetch_does_not_ack_processed_prefix() {
        let table = table_materializer().await;
        let store = table.table().object_store();
        let writer = producer(store.clone());
        append(&writer, 17).await;
        append(&writer, 23).await;
        let location = manifest(&table).await.entries()[1].location.clone();
        let path = Path::from(location);
        let original = store.get(&path).await.unwrap().bytes().await.unwrap();
        store
            .put(&path, PutPayload::from("invalid ipc"))
            .await
            .unwrap();

        let error = table
            .run_materialization(MaterializationOptions)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Wal(buffer::Error::Serialization(_))));
        assert_eq!(
            manifest(&table)
                .await
                .entries()
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            [0, 1]
        );

        store.put(&path, PutPayload::from(original)).await.unwrap();
        table
            .run_materialization(MaterializationOptions)
            .await
            .unwrap();
        assert!(manifest(&table).await.entries().is_empty());
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn missing_captured_head_does_not_ack_prefix() {
        let table = table_materializer().await;
        let writer = producer(table.table().object_store());
        append(&writer, 17).await;

        let error = table.consume_through(1).await.unwrap_err();
        assert!(matches!(error, Error::WalHeadUnavailable(1)));
        assert_eq!(manifest(&table).await.entries()[0].sequence, 0);
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn does_not_ack_batches_appended_after_captured_head() {
        let table = table_materializer().await;
        let writer = producer(table.table().object_store());
        append(&writer, 17).await;
        let head = manifest(&table).await.entries().last().unwrap().sequence;
        append(&writer, 23).await;

        table.consume_through(head).await.unwrap();
        assert_eq!(manifest(&table).await.entries()[0].sequence, 1);
        assert_eq!(manifest(&table).await.entries().len(), 1);

        table
            .run_materialization(MaterializationOptions)
            .await
            .unwrap();
        assert!(manifest(&table).await.entries().is_empty());
        writer.close().await.unwrap();
    }
}
