//! Materialization of the ingestion WAL into Parquet files and deletion vectors.

use std::{sync::Arc, time::Duration};

use buffer::{Consumer, ConsumerConfig};
use iceberg::{io::MemoryStorageFactory, spec::Schema};
use object_store::ObjectStore;
use se_iceberg::table::TableExt;
pub use se_ingestion::Clock;
use se_ingestion::metadata::BatchMetadata;
use se_meta_store::{
    catalog::{CatalogName, CatalogStore, NamespaceIdent, TableIdent},
    table::{TableDb, TableRead},
};
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
    /// Failed to read or acknowledge the WAL.
    #[error("wal error: {0}")]
    Wal(#[from] buffer::Error),
    /// Failed to decode the schema ID attached to a WAL batch.
    #[error("batch metadata error: {0}")]
    BatchMetadata(#[from] se_ingestion::metadata::DecodeError),
    /// A WAL entry has no metadata covering its first row.
    #[error("batch metadata missing for WAL entry {0}")]
    BatchMetadataMissing(u64),
    /// The schema referenced by a WAL batch is not in the table database.
    #[error("table schema missing: {0}")]
    SchemaMissing(i32),
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
    /// For now, the collected batches are neither written nor acknowledged.
    pub async fn run_materialization(&self, _options: MaterializationOptions) -> Result<()> {
        let manifest = se_ingestion::wal_tailer(&self.table).manifest().await?;
        let Some(head) = manifest.entries().last().map(|entry| entry.sequence) else {
            return Ok(());
        };

        self.consume_through(head).await
    }

    async fn consume_through(&self, head: u64) -> Result<()> {
        let object_store = self.table.engine_object_store();
        let config = ConsumerConfig {
            manifest_path: se_ingestion::WAL_MANIFEST_PATH.into(),
            data_path_prefix: se_ingestion::WAL_DATA_PATH.into(),
            gc_interval: Duration::from_mins(5),
            gc_grace_period: Duration::from_mins(10),
        };
        let mut consumer = Consumer::with_object_store(config, object_store, None).await?;

        let result = async {
            let mut schema: Option<Schema> = None;
            let mut record_batches = Vec::new();

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

                let mut schema_boundary = None;
                for metadata in &batch.metadata {
                    let batch_metadata = BatchMetadata::decode(&metadata.payload)?;
                    if let Some(schema) = &schema {
                        if schema.schema_id() != batch_metadata.schema_id {
                            schema_boundary = Some(metadata.start_index as usize);
                            break;
                        }
                    } else {
                        schema = Some(
                            self.table
                                .snapshot()
                                .await?
                                .get_schema(batch_metadata.schema_id)
                                .await?
                                .ok_or(Error::SchemaMissing(batch_metadata.schema_id))?,
                        );
                    }
                }

                // Metadata offsets count rows across all record batches in this entry.
                let mut remaining = schema_boundary.unwrap_or(usize::MAX);
                for record_batch in batch.batches {
                    if remaining == 0 {
                        break;
                    }
                    let rows = remaining.min(record_batch.num_rows());
                    record_batches.push(record_batch.slice(0, rows));
                    remaining -= rows;
                }

                if schema_boundary.is_some() || batch.sequence == head {
                    break;
                }
            }

            #[cfg(test)]
            assert_eq!(
                record_batches
                    .iter()
                    .map(|batch| batch.num_rows())
                    .collect::<Vec<_>>(),
                vec![3, 2]
            );

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

    use arrow_array::{RecordBatch, RecordBatchOptions};
    use buffer::{CompressionType, Producer, ProducerConfig};
    use iceberg::{
        io::FileIOBuilder,
        spec::{FormatVersion, SortOrder, TableMetadataBuilder, UnboundPartitionSpec},
        table::Table,
    };
    use object_store::memory::InMemory;
    use se_meta_store::table::TableWrite;

    use super::*;

    #[tokio::test]
    async fn collects_historical_schema_until_change_without_acknowledging() {
        for split_entries in [false, true] {
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
            let table = Table::builder()
                .runtime(iceberg::Runtime::try_current().unwrap())
                .identifier(TableIdent::new(
                    NamespaceIdent::new("test".into()),
                    "table".into(),
                ))
                .metadata(metadata)
                .file_io(FileIOBuilder::new(Arc::new(MemoryStorageFactory)).build())
                .build()
                .unwrap();
            let table = TableDb::load_or_init("table".into(), table, Arc::new(InMemory::new()))
                .await
                .unwrap();
            let materializer = TableMaterializer {
                table_uuid: table.get_table_uuid().await.unwrap(),
                table,
                table_object_store: Arc::new(InMemory::new()),
            };
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
            for (rows, schema_id) in [(3, Some(7)), (2, Some(7)), (4, Some(8)), (1, None)] {
                if split_entries && schema_id == Some(8) {
                    producer.flush().await.unwrap();
                }
                let batch = RecordBatch::try_new_with_options(
                    schema.clone(),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(rows)),
                )
                .unwrap();
                let payload = schema_id
                    .map(|schema_id| BatchMetadata { schema_id }.encode())
                    .unwrap_or_default();
                producer.produce(batch, payload).await.unwrap();
            }
            producer.close().await.unwrap();

            // The first batch's schema must be loaded, even though it is not current.
            insta::assert_snapshot!(
                materializer
                    .run_materialization(MaterializationOptions)
                    .await.unwrap_err().to_string(),
                @"table schema missing: 7"
            );
            let tx = materializer.table.begin_transaction().await.unwrap();
            tx.put_schema(7, &Schema::builder().with_schema_id(7).build().unwrap())
                .unwrap();
            tx.commit().await.unwrap();

            // Schema 8 is absent, and the following payload is invalid. Neither is read.
            materializer
                .run_materialization(MaterializationOptions)
                .await
                .unwrap();
            materializer
                .run_materialization(MaterializationOptions)
                .await
                .unwrap();
            let manifest = se_ingestion::wal_tailer(materializer.table())
                .manifest()
                .await
                .unwrap();
            assert_eq!(manifest.entries().len(), if split_entries { 2 } else { 1 });
            materializer.table.flush_and_close().await.unwrap();
        }
    }
}
