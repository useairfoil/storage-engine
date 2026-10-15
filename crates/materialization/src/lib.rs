//! Materialization of the ingestion WAL into Parquet files and deletion vectors.

pub mod append;
pub mod upsert;

mod sort;

use std::{sync::Arc, time::Duration};

use buffer::{Consumer, ConsumerConfig};
use datafusion::{arrow::array::RecordBatch, physical_expr_adapter::BatchAdapterFactory};
use iceberg::{
    arrow::schema_to_arrow_schema,
    io::FileIO,
    spec::{FormatVersion, Schema, TableProperties},
    table::Table,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use object_store::ObjectStore;
use se_iceberg::{
    schema::{SchemaExt, TableMode},
    table::TableExt,
};
pub use se_ingestion::Clock;
use se_ingestion::metadata::BatchMetadata;
use se_meta_store::{
    catalog::{CatalogName, CatalogStore, NamespaceIdent, TableIdent},
    table::{PendingCommit, TableDb, TableRead, TableWrite},
};
use tokio::sync::Mutex;
use tracing::info;

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
    #[error("invalid upsert: {0}")]
    InvalidUpsert(&'static str),
    #[error("bitmap encoding error: {0}")]
    BitmapEncoding(#[from] std::io::Error),
    #[error("partitioned append materialization is not implemented")]
    PartitionedTableUnsupported,
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
    table_object_store: Arc<dyn ObjectStore>,
    file_io: FileIO,
    materialization_lock: Arc<Mutex<()>>,
}

/// Collects WAL batches through the current head or the first schema ID change.
///
/// The caller must serialize materialization for this table.
pub async fn run_materialization(
    table: &TableDb,
    table_ident: &TableIdent,
    file_io: &FileIO,
    _options: MaterializationOptions,
) -> Result<()> {
    info!(table = %table_ident, "starting materialization");

    let snapshot = table.snapshot().await?;
    let spec_id = snapshot
        .get_default_partition_spec_id()
        .await?
        .ok_or(se_meta_store::table::Error::TableMetadataMissing)?;
    let spec = snapshot
        .get_partition_spec(spec_id)
        .await?
        .ok_or(se_meta_store::table::Error::TableMetadataMissing)?;

    if !spec.is_unpartitioned() {
        return Err(Error::PartitionedTableUnsupported);
    }

    if snapshot.get_current_snapshot_id().await?.is_some()
        && snapshot.get_upsert_schema_id().await?.is_none()
    {
        let schema_id = snapshot
            .get_current_schema_id()
            .await?
            .ok_or(se_meta_store::table::Error::TableMetadataMissing)?;
        let schema = snapshot
            .get_schema(schema_id)
            .await?
            .ok_or(Error::SchemaMissing(schema_id))?;
        if schema.table_mode() == TableMode::Upsert {
            return Err(Error::InvalidUpsert(
                "existing table requires a key index bootstrap",
            ));
        }
    }

    let manifest = se_ingestion::wal_tailer(table).manifest().await?;
    let Some(head) = manifest.entries().last().map(|entry| entry.sequence) else {
        return Ok(());
    };

    consume_through(table, table_ident, file_io, head).await
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
    /// Table properties and vended credentials are captured here; recreate to refresh them.
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
        let catalog = stored_catalog
            .to_catalog_with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            }))
            .await?;
        let iceberg_table = catalog.load_table(&table_ident).await?;
        table.refresh_metadata(iceberg_table.metadata()).await?;
        TableMaterializer::new(table, &iceberg_table)
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
            .field("table_ident", &self.table_ident)
            .finish()
    }
}

impl TableMaterializer {
    /// Captures storage configuration and credentials from the loaded table.
    /// Refresh the database metadata before running materialization.
    pub fn new(table: TableDb, iceberg_table: &Table) -> Result<Self> {
        Ok(Self {
            table,
            table_ident: iceberg_table.identifier().clone(),
            table_object_store: iceberg_table.object_store()?,
            file_io: iceberg_table.file_io().clone(),
            materialization_lock: Arc::new(Mutex::new(())),
        })
    }

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
    /// Upserts additionally stage the key index and cumulative deletion vectors.
    pub async fn run_materialization(&self, options: MaterializationOptions) -> Result<()> {
        let _guard = self.materialization_lock.lock().await;
        run_materialization(&self.table, &self.table_ident, &self.file_io, options).await
    }
}

async fn consume_through(
    table: &TableDb,
    table_ident: &TableIdent,
    file_io: &FileIO,
    head: u64,
) -> Result<()> {
    let materialized_sequence = table.snapshot().await?.get_materialized_sequence().await?;
    let mut consumer = {
        let object_store = table.engine_object_store();
        let config = ConsumerConfig {
            manifest_path: se_ingestion::WAL_MANIFEST_PATH.into(),
            data_path_prefix: se_ingestion::WAL_DATA_PATH.into(),
            gc_interval: Duration::from_mins(5),
            gc_grace_period: Duration::from_mins(10),
        };
        Consumer::with_object_store(config, object_store, materialized_sequence).await?
    };

    let result = async {
        // Consumer initialization reconciles durable acceptance with a missed WAL ack.
        if materialized_sequence.is_some_and(|sequence| sequence >= head) {
            return Ok(());
        }

        let snapshot = table.snapshot().await?;
        let table_location = snapshot
            .get_table_location()
            .await?
            .ok_or(se_meta_store::table::Error::TableMetadataMissing)?;
        let format_version = snapshot
            .get_format_version()
            .await?
            .ok_or(se_meta_store::table::Error::TableMetadataMissing)?;
        let partition_spec_id = snapshot
            .get_default_partition_spec_id()
            .await?
            .ok_or(se_meta_store::table::Error::TableMetadataMissing)?;
        let table_properties = TableProperties::try_from(&snapshot.get_table_properties().await?)?;
        let mut schema: Option<Schema> = None;
        let mut record_batches = Vec::new();
        let mut first_sequence = None;
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

            first_sequence.get_or_insert(batch.sequence);
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
        {
            let sort_order_id = snapshot
                .get_default_sort_order_id()
                .await?
                .ok_or(Error::DefaultSortOrderMissing)?;
            let sort_order = snapshot
                .get_sort_order(sort_order_id)
                .await?
                .ok_or(Error::SortOrderMissing(sort_order_id))?;
            let tx = table.begin_transaction().await?;
            let (data_files, deletion_vectors) = if schema.table_mode() == TableMode::Upsert {
                if format_version != FormatVersion::V3 {
                    return Err(Error::InvalidUpsert("deletion vectors require Iceberg v3"));
                }
                upsert::write_to_upsert_table(
                    record_batches,
                    &schema,
                    arrow_schema,
                    &sort_order,
                    file_io,
                    &table_location,
                    &table_properties,
                    &tx,
                )
                .await?
            } else {
                (
                    append::write_to_append_table(
                        record_batches,
                        &schema,
                        arrow_schema,
                        &sort_order,
                        file_io,
                        &table_location,
                        &table_properties,
                    )
                    .await?,
                    Vec::new(),
                )
            };

            // PANIC: A schema is loaded only for the first accumulated WAL entry.
            let commit = PendingCommit {
                first_sequence: first_sequence.expect("collected batches have a first sequence"),
                last_sequence,
                schema_id: schema.schema_id(),
                table_location,
                partition_spec_id,
                data_files,
                deletion_vectors,
            };

            tx.push_pending_commit(&commit, materialized_sequence)
                .await?;
            if let Some(handle) = tx
                .commit()
                .await
                .map_err(se_meta_store::table::Error::from)?
            {
                handle
                    .await_durable()
                    .await
                    .map_err(se_meta_store::table::Error::from)?;
            }

            for file in &commit.data_files {
                info!(
                    path = %file.file_path,
                    size_bytes = file.file_size_in_bytes,
                    "added materialized file"
                );
            }
            info!(num_rows, table=%table_ident, last_sequence, "queued materialization");
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_array::{Int32Array, Int64Array, RecordBatch, RecordBatchOptions, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use buffer::{CompressionType, Producer, ProducerConfig};
    use iceberg::{
        io::{FileIOBuilder, MemoryStorageFactory},
        spec::{
            FormatVersion, NestedField, NullOrder, PrimitiveType, SortDirection, SortField,
            SortOrder, TableMetadataBuilder, Transform, Type, UnboundPartitionSpec,
        },
        table::Table,
    };
    use object_store::memory::InMemory;
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
    async fn writes_tables_and_acknowledges_only_the_collected_schema() {
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

            let result = materializer
                .run_materialization(MaterializationOptions)
                .await;
            result.unwrap();
            let snapshot = materializer.table.snapshot().await.unwrap();
            let pending = snapshot.read_pending_commits(None, 10).await.unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(snapshot.get_materialized_sequence().await.unwrap(), Some(1));
            assert_eq!(
                (
                    pending[0].first_sequence,
                    pending[0].last_sequence,
                    pending[0].schema_id
                ),
                (0, 1, 7)
            );
            assert_eq!(pending[0].table_location, "memory://warehouse/table");
            assert_eq!(pending[0].partition_spec_id, 0);
            assert_eq!(pending[0].data_files[0].record_count, 4);
            assert_eq!(pending[0].data_files[0].sort_order_id, 1);
            let manifest = se_ingestion::wal_tailer(materializer.table())
                .manifest()
                .await
                .unwrap();
            assert_eq!(manifest.entries().len(), 1);
            assert_eq!(manifest.entries()[0].sequence, 2);
            let files = &pending[0].data_files;
            assert_eq!(files.len(), 1);
            if !upsert {
                let bytes = materializer
                    .file_io
                    .new_input(format!(
                        "{}/{}",
                        "memory://warehouse/table", files[0].file_path
                    ))
                    .unwrap()
                    .read()
                    .await
                    .unwrap();
                assert_eq!(files[0].file_size_in_bytes, bytes.len() as u64);
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
            let snapshot = materializer.table.snapshot().await.unwrap();
            assert_eq!(snapshot.get_materialized_sequence().await.unwrap(), None);
            assert_eq!(
                snapshot.read_pending_commits(None, 10).await.unwrap(),
                vec![]
            );
            let manifest = se_ingestion::wal_tailer(materializer.table())
                .manifest()
                .await
                .unwrap();
            assert_eq!(manifest.entries().len(), 1);
            materializer.table.flush_and_close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn resumes_durable_materialization_without_duplicate_files_or_queue_entries() {
        for more_wal in [false, true] {
            let materializer = materializer().await;
            let schema = Schema::builder()
                .with_fields([NestedField::required(
                    17,
                    "key",
                    Type::Primitive(PrimitiveType::Long),
                )
                .into()])
                .build()
                .unwrap();
            let tx = materializer.table.begin_transaction().await.unwrap();
            tx.put_schema(0, &schema).unwrap();
            tx.commit().await.unwrap();
            let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(Int64Array::from(vec![8, 3]))],
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
                arrow_schema.clone(),
                Arc::new(se_ingestion::SystemClock),
            )
            .unwrap();
            producer
                .produce(batch.clone(), BatchMetadata { schema_id: 0 }.encode())
                .await
                .unwrap();
            producer.flush().await.unwrap();
            if more_wal {
                producer
                    .produce(batch.clone(), BatchMetadata { schema_id: 0 }.encode())
                    .await
                    .unwrap();
            }
            producer.close().await.unwrap();

            // Simulate a crash after durable acceptance but before consumer acknowledgement.
            let data_files = append::write_to_append_table(
                vec![batch],
                &schema,
                arrow_schema,
                &iceberg::spec::SortOrder::unsorted_order(),
                &materializer.file_io,
                "memory://warehouse/table",
                &TableProperties::try_from(
                    &materializer
                        .table
                        .snapshot()
                        .await
                        .unwrap()
                        .get_table_properties()
                        .await
                        .unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
            let accepted = PendingCommit {
                first_sequence: 0,
                last_sequence: 0,
                schema_id: 0,
                table_location: "memory://warehouse/table".into(),
                partition_spec_id: 0,
                data_files,
                deletion_vectors: Vec::new(),
            };
            let tx = materializer.table.begin_transaction().await.unwrap();
            tx.push_pending_commit(&accepted, None).await.unwrap();
            tx.commit()
                .await
                .unwrap()
                .unwrap()
                .await_durable()
                .await
                .unwrap();

            let clone = materializer.clone();
            let (first, second) = tokio::join!(
                materializer.run_materialization(MaterializationOptions),
                clone.run_materialization(MaterializationOptions)
            );
            first.unwrap();
            second.unwrap();
            let snapshot = materializer.table.snapshot().await.unwrap();
            let pending = snapshot.read_pending_commits(None, 10).await.unwrap();
            assert_eq!(pending.len(), if more_wal { 2 } else { 1 });
            assert_eq!(pending[0], accepted);
            assert_eq!(
                snapshot.get_materialized_sequence().await.unwrap(),
                Some(if more_wal { 1 } else { 0 })
            );
            if more_wal {
                assert_eq!(
                    (pending[1].first_sequence, pending[1].last_sequence),
                    (1, 1)
                );
            }
            for commit in &pending {
                assert_eq!(commit.data_files.len(), 1);
                for file in &commit.data_files {
                    assert!(
                        materializer
                            .file_io
                            .exists(format!("{}/{}", "memory://warehouse/table", file.file_path))
                            .await
                            .unwrap()
                    );
                }
            }
            assert_eq!(
                se_ingestion::wal_tailer(materializer.table())
                    .manifest()
                    .await
                    .unwrap()
                    .entries()
                    .len(),
                0
            );
            materializer.table.flush_and_close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn rolls_files_and_accepts_the_whole_wal_range_once() {
        for sorted in [false, true] {
            for target in ["1", "536870912"] {
                let materializer = materializer().await;
                let tx = materializer.table.begin_transaction().await.unwrap();
                tx.put_table_properties(&HashMap::from([(
                    "write.target-file-size-bytes".into(),
                    target.into(),
                )]))
                .unwrap();
                tx.commit().await.unwrap();
                let schema = Schema::builder()
                    .with_fields([NestedField::required(
                        17,
                        "key",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into()])
                    .build()
                    .unwrap();
                let order = if sorted {
                    SortOrder {
                        order_id: 1,
                        fields: vec![SortField {
                            source_id: 17,
                            transform: Transform::Identity,
                            direction: SortDirection::Ascending,
                            null_order: NullOrder::Last,
                        }],
                    }
                } else {
                    SortOrder::unsorted_order()
                };
                let tx = materializer.table.begin_transaction().await.unwrap();
                tx.put_schema(0, &schema).unwrap();
                tx.put_sort_order(order.order_id, &order).unwrap();
                tx.put_default_sort_order_id(order.order_id).unwrap();
                tx.commit().await.unwrap();
                let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());
                let input: Vec<i64> = (0..16385).rev().collect();
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
                producer
                    .produce(
                        RecordBatch::try_new(
                            arrow_schema,
                            vec![Arc::new(Int64Array::from(input.clone()))],
                        )
                        .unwrap(),
                        BatchMetadata { schema_id: 0 }.encode(),
                    )
                    .await
                    .unwrap();
                producer.close().await.unwrap();
                materializer
                    .run_materialization(MaterializationOptions)
                    .await
                    .unwrap();
                let pending = materializer
                    .table
                    .snapshot()
                    .await
                    .unwrap()
                    .read_pending_commits(None, 10)
                    .await
                    .unwrap();
                assert_eq!(pending.len(), 1);
                assert_eq!(
                    (pending[0].first_sequence, pending[0].last_sequence),
                    (0, 0)
                );
                assert_eq!(pending[0].data_files.len() > 1, target == "1");
                assert!(!pending[0].data_files.is_empty());
                let mut recovered = Vec::new();
                for file in &pending[0].data_files {
                    assert_eq!(file.sort_order_id, order.order_id);
                    let bytes = materializer
                        .file_io
                        .new_input(format!("{}/{}", "memory://warehouse/table", file.file_path))
                        .unwrap()
                        .read()
                        .await
                        .unwrap();
                    assert_eq!(file.file_size_in_bytes, bytes.len() as u64);
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
                    let start = recovered.len();
                    for batch in reader.build().unwrap() {
                        let batch = batch.unwrap();
                        let keys = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        recovered.extend_from_slice(keys.values());
                    }
                    assert_eq!(file.record_count, (recovered.len() - start) as u64);
                }
                assert_eq!(recovered, if sorted { (0..16385).collect() } else { input });
                materializer
                    .run_materialization(MaterializationOptions)
                    .await
                    .unwrap();
                assert_eq!(
                    materializer
                        .table
                        .snapshot()
                        .await
                        .unwrap()
                        .read_pending_commits(None, 10)
                        .await
                        .unwrap(),
                    pending
                );
                assert_eq!(
                    se_ingestion::wal_tailer(materializer.table())
                        .manifest()
                        .await
                        .unwrap()
                        .entries()
                        .len(),
                    0
                );
                materializer.table.flush_and_close().await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn upserts_composite_keys_without_collisions_and_rejects_key_changes() {
        let materializer = materializer().await;
        let schema = Schema::builder()
            .with_fields([
                NestedField::required(17, "a", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(29, "b", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(37, "value", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .with_identifier_field_ids([29, 17])
            .build()
            .unwrap();
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["ab", "a", "ab"])),
                Arc::new(StringArray::from(vec!["c", "bc", "c"])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let tx = materializer.table.begin_transaction().await.unwrap();
        tx.put_schema(0, &schema).unwrap();
        let (files, vectors) = upsert::write_to_upsert_table(
            vec![batch.clone()],
            &schema,
            arrow_schema.clone(),
            &SortOrder::unsorted_order(),
            &materializer.file_io,
            "memory://warehouse/table",
            &TableProperties::try_from(
                &materializer
                    .table
                    .snapshot()
                    .await
                    .unwrap()
                    .get_table_properties()
                    .await
                    .unwrap(),
            )
            .unwrap(),
            &tx,
        )
        .await
        .unwrap();
        assert_eq!(files[0].record_count, 2);
        assert_eq!(vectors, vec![]);
        tx.push_pending_commit(
            &PendingCommit {
                first_sequence: 0,
                last_sequence: 0,
                schema_id: 0,
                table_location: "memory://warehouse/table".into(),
                partition_spec_id: 0,
                data_files: files.clone(),
                deletion_vectors: vectors,
            },
            None,
        )
        .await
        .unwrap();
        tx.commit()
            .await
            .unwrap()
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        let bytes = materializer
            .file_io
            .new_input(format!(
                "{}/{}",
                "memory://warehouse/table", files[0].file_path
            ))
            .unwrap()
            .read()
            .await
            .unwrap();
        let written = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(
            written
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[20, 30]
        );

        let tx = materializer.table.begin_transaction().await.unwrap();
        let (_, vectors) = upsert::write_to_upsert_table(
            vec![batch.slice(0, 1)],
            &schema,
            arrow_schema.clone(),
            &SortOrder::unsorted_order(),
            &materializer.file_io,
            "memory://warehouse/table",
            &TableProperties::try_from(
                &materializer
                    .table
                    .snapshot()
                    .await
                    .unwrap()
                    .get_table_properties()
                    .await
                    .unwrap(),
            )
            .unwrap(),
            &tx,
        )
        .await
        .unwrap();
        assert_eq!(vectors[0].current.cardinality, 1);
        let bitmap = tx
            .get_deletion_bitmap(&files[0].file_path)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            roaring::RoaringTreemap::deserialize_from(bitmap.as_ref())
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![1]
        );
        drop(tx);

        let changed = Schema::builder()
            .with_schema_id(1)
            .with_fields(schema.as_struct().fields().to_vec())
            .with_identifier_field_ids([17])
            .build()
            .unwrap();
        let tx = materializer.table.begin_transaction().await.unwrap();
        let error = upsert::write_to_upsert_table(
            vec![batch],
            &changed,
            arrow_schema,
            &SortOrder::unsorted_order(),
            &materializer.file_io,
            "memory://warehouse/table",
            &TableProperties::try_from(
                &materializer
                    .table
                    .snapshot()
                    .await
                    .unwrap()
                    .get_table_properties()
                    .await
                    .unwrap(),
            )
            .unwrap(),
            &tx,
        )
        .await
        .unwrap_err();
        insta::assert_snapshot!(error, @"invalid upsert: identifier fields or their types changed");
        drop(tx);
        materializer.table.flush_and_close().await.unwrap();
    }

    async fn materializer() -> TableMaterializer {
        let metadata = TableMetadataBuilder::new(
            Schema::builder().build().unwrap(),
            UnboundPartitionSpec::default(),
            SortOrder::unsorted_order(),
            "memory://warehouse/table".into(),
            FormatVersion::V3,
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
            table_ident,
            table,
            table_object_store: Arc::new(InMemory::new()),
            file_io: FileIO::new_with_memory(),
            materialization_lock: Arc::new(Mutex::new(())),
        }
    }
}
