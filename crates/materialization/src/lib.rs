//! Materialization of the ingestion WAL into Parquet files and deletion vectors.

pub mod append;

use std::{sync::Arc, time::Duration};

use buffer::{Consumer, ConsumerConfig};
use datafusion::{arrow::array::RecordBatch, physical_expr_adapter::BatchAdapterFactory};
use iceberg::{arrow::schema_to_arrow_schema, io::MemoryStorageFactory, spec::Schema};
use object_store::ObjectStore;
use se_iceberg::table::TableExt;
pub use se_ingestion::Clock;
use se_ingestion::metadata::BatchMetadata;
use se_meta_store::{
    catalog::{CatalogName, CatalogStore, NamespaceIdent, TableIdent},
    table::{TableDb, TableRead},
};
use tracing::info;
use uuid::Uuid;

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
    /// Failed to read the table database.
    #[error("table database error: {0}")]
    TableDb(#[from] se_meta_store::table::Error),
    /// Failed to load the Iceberg table or its storage configuration.
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),
    /// Failed to construct the table storage operator.
    #[error("table storage config error: {0}")]
    TableStorage(#[from] se_iceberg::table::Error),
    /// Failed to write to table storage.
    #[error("storage error: {0}")]
    Storage(#[from] object_store::Error),
    /// Failed to encode the WAL batches as Parquet.
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// Failed to adapt or sort the WAL record batches.
    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    /// Failed to construct or sort Arrow record batches.
    #[error("arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    /// Failed to read or acknowledge the WAL.
    #[error("wal error: {0}")]
    Wal(#[from] buffer::Error),
    /// Failed to decode the schema ID attached to a WAL batch.
    #[error("batch metadata error: {0}")]
    BatchMetadata(#[from] se_ingestion::metadata::DecodeError),
    /// A WAL entry has no metadata covering its first row.
    #[error("batch metadata missing for WAL entry {0}")]
    BatchMetadataMissing(u64),
    /// A single WAL entry references more than one schema.
    #[error("conflicting schema IDs in WAL entry {0}")]
    BatchSchemaMismatch(u64),
    /// The schema referenced by a WAL batch is not in the table database.
    #[error("table schema missing: {0}")]
    SchemaMissing(i32),
    /// The table has no default sort order ID.
    #[error("default sort order ID missing")]
    DefaultSortOrderMissing,
    /// The default sort order is not in the table database.
    #[error("table sort order missing: {0}")]
    SortOrderMissing(i64),
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
    table: TableDb,
    table_ident: TableIdent,
    table_uuid: Uuid,
    table_object_store: Arc<dyn ObjectStore>,
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
            .ok_or_else(|| Error::TableNotFound(table_ident.clone()))?;
        let stored_uuid = table.get_table_uuid().await?;

        let iceberg_table = {
            let catalog = stored_catalog
                .to_catalog_with_storage_factory(Arc::new(MemoryStorageFactory))
                .await?;
            let iceberg_table = catalog.load_table(&table_ident).await?;

            if iceberg_table.metadata().uuid() != stored_uuid {
                return Err(se_meta_store::catalog::Error::UuidMismatch {
                    catalog_uuid: iceberg_table.metadata().uuid(),
                    stored_uuid,
                }
                .into());
            }

            iceberg_table
        };

        Ok(TableMaterializer {
            table,
            table_ident,
            table_uuid: stored_uuid,
            table_object_store: iceberg_table.object_store()?,
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
            .field("table_uuid", &self.table_uuid)
            .finish()
    }
}

impl TableMaterializer {
    /// Returns the current table db.
    pub fn table(&self) -> &TableDb {
        &self.table
    }

    /// Returns the table's object store, accepting paths relative to its base location.
    /// Vended credentials are captured at creation; recreate the materializer to refresh them.
    pub fn table_object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.table_object_store)
    }

    /// Collects WAL batches through its current head or the first schema ID change.
    /// Append batches are written to Parquet; upsert materialization is not yet implemented.
    pub async fn run_materialization(&self, _options: MaterializationOptions) -> Result<()> {
        let manifest = se_ingestion::wal_tailer(&self.table).manifest().await?;
        let Some(head) = manifest.entries().last().map(|entry| entry.sequence) else {
            return Ok(());
        };

        self.consume_through(head).await
    }

    async fn consume_through(&self, head: u64) -> Result<()> {
        let mut consumer = {
            let object_store = self.table.engine_object_store();
            let config = ConsumerConfig {
                manifest_path: se_ingestion::WAL_MANIFEST_PATH.into(),
                data_path_prefix: se_ingestion::WAL_DATA_PATH.into(),
                gc_interval: Duration::from_mins(5),
                gc_grace_period: Duration::from_mins(10),
            };
            Consumer::with_object_store(config, object_store, None).await?
        };

        let result = async {
            let snapshot = self.table.snapshot().await?;
            let mut schema: Option<Schema> = None;
            let mut record_batches = Vec::new();
            let mut last_sequence = 0;

            loop {
                let batch = consumer
                    .next_batch()
                    .await?
                    .ok_or(Error::WalHeadUnavailable(head))?;

                if batch.sequence > head {
                    return Err(Error::WalHeadUnavailable(head));
                }

                if batch
                    .metadata
                    .first()
                    .is_none_or(|metadata| metadata.start_index != 0)
                {
                    return Err(Error::BatchMetadataMissing(batch.sequence));
                }

                // A buffer batch can contain multiple metadata ranges, but all
                // of them must reference the same table schema.
                let batch_schema_id = BatchMetadata::decode(&batch.metadata[0].payload)?.schema_id;
                for metadata in &batch.metadata {
                    if BatchMetadata::decode(&metadata.payload)?.schema_id != batch_schema_id {
                        return Err(Error::BatchSchemaMismatch(batch.sequence));
                    }
                }

                if let Some(schema) = &schema {
                    if schema.schema_id() != batch_schema_id {
                        break;
                    }
                } else {
                    schema = Some(
                        snapshot
                            .get_schema(batch_schema_id)
                            .await?
                            .ok_or(Error::SchemaMissing(batch_schema_id))?,
                    );
                }

                record_batches.extend(batch.batches);
                last_sequence = batch.sequence;

                if batch.sequence == head {
                    break;
                }
            }

            // PANIC: The loop loads a schema before accumulating any batches.
            let schema = schema.expect("collected batches have a schema");

            let arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);
            let adapter_factory = BatchAdapterFactory::new(arrow_schema.clone());
            let record_batches = record_batches
                .into_iter()
                .map(|batch| {
                    let adapter = adapter_factory.make_adapter(&batch.schema())?;
                    let adapted = adapter.adapt_batch(&batch)?;
                    // Use the canonical fields, including Iceberg's Parquet field-ID metadata.
                    Ok(RecordBatch::try_new(
                        arrow_schema.clone(),
                        adapted.columns().to_vec(),
                    )?)
                })
                .collect::<Result<Vec<_>>>()?;

            let num_rows: usize = record_batches.iter().map(|b| b.num_rows()).sum();
            if schema.identifier_field_ids().next().is_some() {
                // PANIC: Upsert materialization is not yet implemented.
                // todo!("materialize upsert table");
                info!(num_rows, table=%self.table_ident, "materialize upsert table");
            } else {
                let sort_order_id = snapshot
                    .get_default_sort_order_id()
                    .await?
                    .ok_or(Error::DefaultSortOrderMissing)?;
                let sort_order = snapshot
                    .get_sort_order(sort_order_id)
                    .await?
                    .ok_or(Error::SortOrderMissing(sort_order_id))?;
                let file_name = append::write_to_append_table(
                    record_batches,
                    &schema,
                    arrow_schema,
                    &sort_order,
                    self.table_object_store.as_ref(),
                )
                .await?;
                info!(num_rows, table=%self.table_ident, file_name, "materialized append table");
            }

            consumer.ack_through(last_sequence).await?;

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
    use std::collections::HashMap;

    use arrow_array::{Int32Array, Int64Array, RecordBatch, RecordBatchOptions};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use buffer::{CompressionType, Producer, ProducerConfig};
    use iceberg::{
        io::FileIOBuilder,
        spec::{
            FormatVersion, NestedField, NullOrder, PrimitiveType, SortDirection, SortField,
            SortOrder, TableMetadataBuilder, Transform, Type, UnboundPartitionSpec,
        },
        table::Table,
    };
    use object_store::{ObjectStoreExt, memory::InMemory};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use se_meta_store::table::TableWrite;

    use super::*;

    #[tokio::test]
    async fn rejects_invalid_batches_without_acknowledging() {
        for mixed_schema in [false, true] {
            let materializer = materializer().await;
            // Neither an empty WAL nor historical batches need the current schema.
            let tx = materializer.table.begin_transaction().await.unwrap();
            tx.put_current_schema_id(999).unwrap();
            tx.commit().await.unwrap();
            materializer
                .run_materialization(MaterializationOptions)
                .await
                .unwrap();

            let schema = Arc::new(arrow_schema::Schema::empty());
            let producer = Producer::new(
                ProducerConfig {
                    manifest_path: se_ingestion::WAL_MANIFEST_PATH.into(),
                    data_path_prefix: se_ingestion::WAL_DATA_PATH.into(),
                    flush_interval: Duration::from_secs(3600),
                    flush_size_bytes: usize::MAX,
                    max_buffered_inputs: 10,
                    batch_compression: CompressionType::None,
                },
                materializer.table.engine_object_store(),
                schema.clone(),
                Arc::new(se_ingestion::SystemClock),
            )
            .unwrap();
            for (rows, schema_id) in [(3, 7), (2, 7), (4, if mixed_schema { 8 } else { 7 })] {
                let batch = RecordBatch::try_new_with_options(
                    schema.clone(),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(rows)),
                )
                .unwrap();
                let payload = BatchMetadata { schema_id }.encode();
                producer.produce(batch, payload).await.unwrap();
            }
            producer.close().await.unwrap();

            let error = materializer
                .run_materialization(MaterializationOptions)
                .await
                .unwrap_err();
            if mixed_schema {
                insta::assert_snapshot!(error, @"conflicting schema IDs in WAL entry 0");
            } else {
                // Load the historical schema, not the current one.
                insta::assert_snapshot!(error, @"table schema missing: 7");
            }
            let manifest = se_ingestion::wal_tailer(materializer.table())
                .manifest()
                .await
                .unwrap();
            assert_eq!(manifest.entries().len(), 1);
            materializer.table.flush_and_close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn writes_only_append_tables_and_acknowledges_only_the_collected_schema() {
        for upsert in [false, true] {
            let materializer = materializer().await;
            let mut schema =
                Schema::builder()
                    .with_schema_id(7)
                    .with_fields([NestedField::required(
                        17,
                        "key",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into()]);
            if upsert {
                schema = schema.with_identifier_field_ids([17]);
            }
            let schema = schema.build().unwrap();
            let order = SortOrder {
                order_id: 1,
                fields: vec![SortField {
                    source_id: 17,
                    transform: Transform::Identity,
                    direction: SortDirection::Descending,
                    null_order: NullOrder::Last,
                }],
            };
            let tx = materializer.table.begin_transaction().await.unwrap();
            tx.put_schema(7, &schema).unwrap();
            tx.put_current_schema_id(999).unwrap();
            tx.put_sort_order(1, &order).unwrap();
            tx.put_default_sort_order_id(1).unwrap();
            tx.commit().await.unwrap();
            // WAL batches need adaptation to the canonical type and field-ID metadata.
            let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
                "key",
                DataType::Int32,
                true,
            )]));
            let producer = Producer::new(
                ProducerConfig {
                    manifest_path: se_ingestion::WAL_MANIFEST_PATH.into(),
                    data_path_prefix: se_ingestion::WAL_DATA_PATH.into(),
                    flush_interval: Duration::from_secs(3600),
                    flush_size_bytes: usize::MAX,
                    max_buffered_inputs: 10,
                    batch_compression: CompressionType::None,
                },
                materializer.table.engine_object_store(),
                arrow_schema.clone(),
                Arc::new(se_ingestion::SystemClock),
            )
            .unwrap();
            for (schema_id, keys) in [(7, vec![3, 8]), (7, vec![1, 6]), (8, vec![9])] {
                let batch = RecordBatch::try_new(
                    arrow_schema.clone(),
                    vec![Arc::new(Int32Array::from(keys))],
                )
                .unwrap();
                producer
                    .produce(batch, BatchMetadata { schema_id }.encode())
                    .await
                    .unwrap();
                producer.flush().await.unwrap();
            }
            producer.close().await.unwrap();

            materializer
                .run_materialization(MaterializationOptions)
                .await
                .unwrap();
            let manifest = se_ingestion::wal_tailer(materializer.table())
                .manifest()
                .await
                .unwrap();
            assert_eq!(manifest.entries().len(), 1);
            assert_eq!(manifest.entries()[0].sequence, 2);
            let files = materializer
                .table_object_store
                .list_with_delimiter(Some(&"data".into()))
                .await
                .unwrap()
                .objects;
            assert_eq!(files.len(), if upsert { 0 } else { 1 });
            if !upsert {
                let bytes = materializer
                    .table_object_store
                    .get(&files[0].location)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
                assert_eq!(
                    reader
                        .parquet_schema()
                        .column(0)
                        .self_type()
                        .get_basic_info()
                        .id(),
                    17
                );
                let batch = reader.build().unwrap().next().unwrap().unwrap();
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(keys.values().as_ref(), &[8, 6, 3, 1]);
            }

            // The next schema is not loaded or acknowledged during the first pass.
            let error = materializer
                .run_materialization(MaterializationOptions)
                .await
                .unwrap_err();
            insta::allow_duplicates! {
                insta::assert_snapshot!(error, @"table schema missing: 8");
            }
            let manifest = se_ingestion::wal_tailer(materializer.table())
                .manifest()
                .await
                .unwrap();
            assert_eq!(manifest.entries().len(), 1);
            materializer.table.flush_and_close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn rejects_nulls_before_selecting_a_writer_without_acknowledging() {
        for upsert in [false, true] {
            let materializer = materializer().await;
            let mut schema = Schema::builder().with_fields([NestedField::required(
                1,
                "key",
                Type::Primitive(PrimitiveType::Long),
            )
            .into()]);
            if upsert {
                schema = schema.with_identifier_field_ids([1]);
            }
            let tx = materializer.table.begin_transaction().await.unwrap();
            tx.put_schema(0, &schema.build().unwrap()).unwrap();
            tx.commit().await.unwrap();
            let source = Arc::new(ArrowSchema::new(vec![Field::new(
                "key",
                DataType::Int64,
                true,
            )]));
            let batch = RecordBatch::try_new(
                source.clone(),
                vec![Arc::new(Int64Array::from(vec![None, Some(3)]))],
            )
            .unwrap();
            let producer = Producer::new(
                ProducerConfig {
                    manifest_path: se_ingestion::WAL_MANIFEST_PATH.into(),
                    data_path_prefix: se_ingestion::WAL_DATA_PATH.into(),
                    flush_interval: Duration::from_secs(3600),
                    flush_size_bytes: usize::MAX,
                    max_buffered_inputs: 10,
                    batch_compression: CompressionType::None,
                },
                materializer.table.engine_object_store(),
                source,
                Arc::new(se_ingestion::SystemClock),
            )
            .unwrap();
            producer
                .produce(batch, BatchMetadata { schema_id: 0 }.encode())
                .await
                .unwrap();
            producer.close().await.unwrap();

            let result = materializer
                .run_materialization(MaterializationOptions)
                .await;
            assert!(matches!(result, Err(Error::Arrow(_))));
            assert_eq!(
                materializer
                    .table_object_store
                    .list_with_delimiter(None)
                    .await
                    .unwrap()
                    .objects
                    .len(),
                0
            );
            let manifest = se_ingestion::wal_tailer(materializer.table())
                .manifest()
                .await
                .unwrap();
            assert_eq!(manifest.entries().len(), 1);
            materializer.table.flush_and_close().await.unwrap();
        }
    }

    async fn materializer() -> TableMaterializer {
        let metadata = TableMetadataBuilder::new(
            Schema::builder().build().unwrap(),
            UnboundPartitionSpec::default(),
            SortOrder::unsorted_order(),
            "memory://warehouse/table".into(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let table_ident = TableIdent::new(NamespaceIdent::new("test".into()), "table".into());
        let table = Table::builder()
            .runtime(iceberg::Runtime::try_current().unwrap())
            .identifier(table_ident.clone())
            .metadata(metadata)
            .file_io(FileIOBuilder::new(Arc::new(MemoryStorageFactory)).build())
            .build()
            .unwrap();
        let table = TableDb::load_or_init("table".into(), table, Arc::new(InMemory::new()))
            .await
            .unwrap();
        TableMaterializer {
            table_uuid: table.get_table_uuid().await.unwrap(),
            table_ident,
            table,
            table_object_store: Arc::new(InMemory::new()),
        }
    }
}
