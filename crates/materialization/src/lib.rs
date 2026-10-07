//! Materialization of the ingestion WAL into Parquet files and deletion vectors.

use std::{sync::Arc, time::Duration};

use buffer::{Consumer, ConsumerConfig};
use iceberg::{arrow::schema_to_arrow_schema, io::MemoryStorageFactory};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use parquet::arrow::ArrowWriter;
use se_iceberg::table::TableExt;
pub use se_ingestion::Clock;
use se_meta_store::{
    catalog::{CatalogName, CatalogStore, NamespaceIdent, TableIdent},
    table::StoredTable,
};
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

        let iceberg_table = {
            let catalog = stored_catalog
                .to_catalog_with_storage_factory(Arc::new(MemoryStorageFactory))
                .await?;
            let iceberg_table = catalog.load_table(&table_ident).await?;
            if iceberg_table.metadata().uuid() != table.metadata().uuid {
                return Err(se_meta_store::catalog::Error::UuidMismatch {
                    catalog_uuid: iceberg_table.metadata().uuid(),
                    stored_uuid: table.metadata().uuid,
                }
                .into());
            }

            iceberg_table
        };

        Ok(TableMaterializer {
            table: Arc::new(table),
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
            .field("table_uuid", &self.table.metadata().uuid)
            .finish()
    }
}

impl TableMaterializer {
    /// Returns the stored table to use for WAL reads.
    pub fn table(&self) -> &StoredTable {
        &self.table
    }

    /// Returns the table's object store, accepting paths relative to its base location.
    /// Vended credentials are captured at creation; recreate the materializer to refresh them.
    pub fn table_object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.table_object_store)
    }

    /// Writes the WAL through its current head to one Parquet file before acknowledging it.
    /// This testing path buffers the output in memory and does not commit an Iceberg snapshot.
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
            let schema = Arc::new(schema_to_arrow_schema(
                self.table.metadata().schema.as_ref(),
            )?);

            let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;

            loop {
                let batch = consumer
                    .next_batch()
                    .await?
                    .ok_or(Error::WalHeadUnavailable(head))?;

                if batch.sequence > head {
                    return Err(Error::WalHeadUnavailable(head));
                }

                for record_batch in &batch.batches {
                    writer.write(record_batch)?;
                }

                if batch.sequence == head {
                    break;
                }
            }

            let data = writer.into_inner()?;
            let location = format!("data/wal-{head}.parquet");

            info!(location, "writing parquet data");

            self.table_object_store
                .put(&Path::from(location), PutPayload::from(data))
                .await?;

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
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::TcpListener,
    };
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn table_operator_reads_and_writes_with_vended_credentials() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let table_response = r#"{
            "metadata-location": "s3://warehouse/tables/events/metadata/v1.json",
            "metadata": {
                "format-version": 1,
                "table-uuid": "00000000-0000-0000-0000-000000000007",
                "location": "s3://warehouse/tables/events/",
                "last-updated-ms": 1,
                "last-column-id": 1,
                "schema": {"type": "struct", "schema-id": 0, "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"}
                ]},
                "current-schema-id": 0,
                "partition-spec": [],
                "default-spec-id": 0,
                "sort-orders": [{"order-id": 0, "fields": []}],
                "default-sort-order-id": 0,
                "properties": {}
            },
            "config": {
                "s3.endpoint": "ENDPOINT",
                "s3.access-key-id": "table-access-key",
                "s3.secret-access-key": "table-secret-key",
                "s3.session-token": "table-session-token",
                "client.region": "table-region"
            }
        }"#
        .replace("ENDPOINT", &endpoint);
        let server = tokio::spawn(async move {
            // Link the table, load it for materialization, then write/read table data.
            for (index, (method, path)) in [
                ("GET", "/v1/config"),
                ("GET", "/v1/namespaces/analytics/tables/events"),
                ("GET", "/v1/config"),
                ("GET", "/v1/namespaces/analytics/tables/events"),
                ("PUT", "/warehouse/tables/events/data/part.parquet"),
                ("GET", "/warehouse/tables/events/data/part.parquet"),
            ]
            .into_iter()
            .enumerate()
            {
                let (stream, _) = listener.accept().await.unwrap();
                let mut stream = BufReader::new(stream);
                let mut request = String::new();
                stream.read_line(&mut request).await.unwrap();
                assert_eq!(request, format!("{method} {path} HTTP/1.1\r\n"));
                let mut headers = String::new();
                loop {
                    let mut line = String::new();
                    assert_ne!(stream.read_line(&mut line).await.unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                    headers.push_str(&line.to_ascii_lowercase());
                }
                if index == 3 {
                    assert!(
                        headers.contains("x-iceberg-access-delegation: vended-credentials\r\n")
                    );
                }
                if index >= 4 {
                    assert!(headers.contains("credential=table-access-key/"));
                    assert!(headers.contains("/table-region/s3/aws4_request"));
                    assert!(headers.contains("x-amz-security-token: table-session-token\r\n"));
                }
                let body = if path == "/v1/config" {
                    r#"{"defaults": {}, "overrides": {}}"#
                } else if path.starts_with("/v1/namespaces/") {
                    &table_response
                } else if method == "PUT" {
                    assert!(headers.contains("content-length: 10\r\n"));
                    let mut body = [0; 10];
                    stream.read_exact(&mut body).await.unwrap();
                    assert_eq!(&body, b"table-data");
                    ""
                } else {
                    "table-data"
                };
                stream
                    .get_mut()
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nETag: \"test-etag\"\r\nLast-Modified: Tue, 06 Oct 2026 00:00:00 GMT\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        let catalogs = CatalogStore::new(
            Arc::new(InMemorySecretStore::new()),
            Arc::new(InMemory::new()),
        );
        let name = CatalogName::new("example").unwrap();
        catalogs
            .create(
                name.clone(),
                CatalogConfig::Rest(RestCatalogConfig {
                    uri: endpoint,
                    warehouse: None,
                    properties: HashMap::new(),
                }),
            )
            .await
            .unwrap();
        let namespace = NamespaceIdent::from_strs(["analytics"]).unwrap();
        catalogs
            .get(name.clone())
            .await
            .unwrap()
            .unwrap()
            .link_table(TableIdent::new(namespace.clone(), "events".into()))
            .await
            .unwrap();
        let materializer = Materializer::new(catalogs, Arc::new(SystemClock));
        let table = materializer
            .for_table(name, namespace, "events".into())
            .await
            .unwrap();
        let store = table.table_object_store();
        let location = Path::from("data/part.parquet");
        store
            .put(&location, PutPayload::from("table-data"))
            .await
            .unwrap();
        let data = store.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(data.as_ref(), b"table-data");
        assert!(matches!(
            table
                .table()
                .engine_object_store()
                .get(&Path::from("data/part.parquet"))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));
        server.await.unwrap();
    }

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
            table_object_store: Arc::new(InMemory::new()),
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

    async fn parquet_values(table: &TableMaterializer, head: u64) -> Vec<i64> {
        let data = table
            .table_object_store
            .get(&Path::from(format!("data/wal-{head}.parquet")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(data)
            .unwrap()
            .build()
            .unwrap();
        reader
            .flat_map(|batch| {
                batch
                    .unwrap()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    #[tokio::test]
    async fn materializes_empty_and_retained_wal() {
        let table = table_materializer().await;
        table
            .run_materialization(MaterializationOptions)
            .await
            .unwrap();
        let writer = producer(table.table().engine_object_store());
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
        assert_eq!(parquet_values(&table, 1).await, [17, 23]);
        assert!(matches!(
            table
                .table_object_store
                .head(&Path::from("data/wal-0.parquet"))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));
        table
            .run_materialization(MaterializationOptions)
            .await
            .unwrap();
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn failed_upload_does_not_ack_wal() {
        let mut table = table_materializer().await;
        let writer = producer(table.table().engine_object_store());
        append(&writer, 17).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 8192];
            assert_ne!(stream.read(&mut request).await.unwrap(), 0);
            stream
                .write_all(
                    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });
        table.table_object_store = Arc::new(
            object_store::aws::AmazonS3Builder::new()
                .with_bucket_name("test")
                .with_region("local")
                .with_endpoint(endpoint)
                .with_allow_http(true)
                .with_access_key_id("test")
                .with_secret_access_key("test")
                .build()
                .unwrap(),
        );
        assert!(matches!(
            table.run_materialization(MaterializationOptions).await,
            Err(Error::Storage(_))
        ));
        server.await.unwrap();
        assert_eq!(manifest(&table).await.entries()[0].sequence, 0);
        table.table_object_store = Arc::new(InMemory::new());
        table
            .run_materialization(MaterializationOptions)
            .await
            .unwrap();
        assert_eq!(parquet_values(&table, 0).await, [17]);
        assert!(manifest(&table).await.entries().is_empty());
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn failed_fetch_does_not_ack_processed_prefix() {
        let table = table_materializer().await;
        let store = table.table().engine_object_store();
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
        let writer = producer(table.table().engine_object_store());
        append(&writer, 17).await;

        let error = table.consume_through(1).await.unwrap_err();
        assert!(matches!(error, Error::WalHeadUnavailable(1)));
        assert_eq!(manifest(&table).await.entries()[0].sequence, 0);
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn does_not_ack_batches_appended_after_captured_head() {
        let table = table_materializer().await;
        let writer = producer(table.table().engine_object_store());
        append(&writer, 17).await;
        let head = manifest(&table).await.entries().last().unwrap().sequence;
        append(&writer, 23).await;

        table.consume_through(head).await.unwrap();
        assert_eq!(manifest(&table).await.entries()[0].sequence, 1);
        assert_eq!(manifest(&table).await.entries().len(), 1);
        assert_eq!(parquet_values(&table, 0).await, [17]);

        table
            .run_materialization(MaterializationOptions)
            .await
            .unwrap();
        assert_eq!(parquet_values(&table, 1).await, [23]);
        assert!(manifest(&table).await.entries().is_empty());
        writer.close().await.unwrap();
    }
}
