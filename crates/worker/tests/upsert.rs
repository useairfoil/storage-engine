use std::{
    collections::HashMap,
    io::{Cursor, Write},
    sync::{Arc, Mutex as StdMutex},
};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use axum::{Json, Router, routing::get};
use iceberg::{
    TableIdent,
    io::{MemoryStorage, Storage, StorageConfig, StorageFactory},
    spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadata,
        TableMetadataBuilder, Type, UnboundPartitionSpec,
    },
};
use object_store::{memory::InMemory, path::Path};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use roaring::RoaringTreemap;
use se_common::{
    clock::{DefaultSystemClock, SystemClock},
    rand::SeRand,
};
use se_materialization::TableIndex;
use se_metadata_client::{
    CatalogConfig, CatalogId, Heartbeat, IndexDelta, MaterializationInput, MaterializationResult,
    MetadataClient, PreparedCommit, ReconciliationInput, RestCatalogConfig, Task, TaskAssignment,
    TaskToken, WalRange,
};
use se_wal::{BatchCommit, BatchRead, InMemoryWal, Producer, ProducerOptions};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::fmt::format::FmtSpan;
use uuid::Uuid;

type Result<T, E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

#[derive(Debug, Default, Serialize, Deserialize)]
struct SharedStorageFactory {
    storage: MemoryStorage,
}

struct CatalogServer {
    config: CatalogConfig,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Default)]
struct History {
    accepted: Vec<IndexDelta>,
    submitted: Vec<MaterializationResult>,
    inputs: Vec<(u64, Vec<IndexDelta>)>,
    failures: usize,
}

struct Client {
    catalog: CatalogConfig,
    assignment: TaskAssignment,
    wal: Arc<InMemoryWal>,
    history: Arc<Mutex<History>>,
    accept: bool,
    acknowledge: bool,
    cancel: CancellationToken,
}

struct LogWriter(Arc<StdMutex<Vec<u8>>>);

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn recovers_lost_ack_delta_and_ignores_rejected_attempt_on_retry() -> Result<()> {
    let metadata = table_metadata()?;
    let table_uuid = metadata.uuid();
    let server = CatalogServer::start(metadata).await?;
    let factory = Arc::new(SharedStorageFactory::default());
    let store = Arc::new(InMemory::new());
    let wal = Arc::new(InMemoryWal::default());
    let clock = Arc::new(DefaultSystemClock::new());
    let history = Arc::new(Mutex::new(History::default()));
    let catalog_id = CatalogId::new("upsert-test")?;
    let index_path = Path::from("table-index")
        .join(catalog_id.as_str())
        .join(table_uuid.to_string());
    let source = Arc::new(ArrowSchema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let producer = Producer::new(
        ProducerOptions::default(),
        store.clone(),
        wal.clone(),
        source.clone(),
        0,
        Arc::new(SeRand::new(42)),
        clock.clone(),
    )?;
    for (keys, values) in [
        (vec![10, 20, 30], vec![100, 200, 300]),
        (vec![20], vec![201]),
    ] {
        producer
            .produce(RecordBatch::try_new(
                source.clone(),
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(Int64Array::from(values)),
                ],
            )?)
            .await?;
        producer.flush().await?;
    }
    producer.close().await?;
    let wal_before = wal.batches().await;
    let logs = Arc::new(StdMutex::new(Vec::new()));

    // The first completion commits authoritatively but loses its acknowledgment. The
    // second prepares an update but is rejected; the third retries that same WAL range.
    for (attempt, range, accept, acknowledge, expected_cursor) in [
        (1, WalRange { start: 0, end: 1 }, true, false, 0),
        (2, WalRange { start: 1, end: 2 }, false, false, 1),
        (3, WalRange { start: 1, end: 2 }, true, true, 2),
    ] {
        let client = Arc::new(Client {
            catalog: server.config.clone(),
            assignment: TaskAssignment {
                token: TaskToken {
                    worker_id: Uuid::from_u128(100 + attempt),
                    catalog_id: catalog_id.clone(),
                    table_uuid,
                    table_ident: TableIdent::from_strs(["test", "table"])?,
                    attempt_id: Uuid::from_u128(attempt),
                },
                eligible_at: Some(clock.now() - chrono::TimeDelta::milliseconds(250)),
                expires_at: chrono::DateTime::UNIX_EPOCH + chrono::TimeDelta::seconds(30),
                task: Task::Materialize {
                    range,
                    schema_id: 0,
                    epoch: attempt as u64,
                },
            },
            wal: wal.clone(),
            history: history.clone(),
            accept,
            acknowledge,
            cancel: CancellationToken::new(),
        });
        let output = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_span_events(FmtSpan::CLOSE)
            .with_writer(move || LogWriter(output.clone()))
            .finish();
        se_worker::Worker::new(
            client.assignment.token.worker_id,
            client.clone(),
            store.clone(),
            factory.clone(),
            clock.clone(),
        )
        .run(client.cancel.clone())
        .with_subscriber(subscriber)
        .await?;

        // Reopening also proves that every completion path closed its SlateDB writer.
        let index = TableIndex::open(
            index_path.clone(),
            store.clone(),
            attempt as u64,
            clock.clone(),
        )
        .await?;
        assert_eq!(index.applied_through().await?, expected_cursor);
        index.close().await?;
    }

    let logs = String::from_utf8(logs.lock().unwrap().clone())?;
    let task_totals: Vec<_> = logs
        .lines()
        .filter(|line| line.contains("worker.materialize{") && line.ends_with(": se_worker: close"))
        .map(|line| {
            let value = |name: &str| -> Result<f64> {
                Ok(line
                    .rsplit_once(name)
                    .ok_or_else(|| format!("missing {name} in {line}"))?
                    .1
                    .split(|c: char| c.is_whitespace() || c == '}')
                    .next()
                    .ok_or("missing task value")?
                    .parse()?)
            };
            Ok((
                value("wal.read_rows=")? as u64,
                value("wal.read_bytes=")? > 0.0,
                value("task.start_delay_ms=")? >= 250.0,
            ))
        })
        .collect::<Result<_>>()?;
    insta::assert_debug_snapshot!(task_totals, @r"
    [
        (
            3,
            true,
            true,
        ),
        (
            1,
            true,
            true,
        ),
        (
            1,
            true,
            true,
        ),
    ]
    ");

    let history = history.lock().await;
    assert_eq!(history.failures, 0);
    assert_eq!(history.submitted.len(), 3);
    let first = &history.submitted[0];
    let rejected = &history.submitted[1];
    let retry = &history.submitted[2];
    assert_eq!(first.data_files.len(), 1);
    assert_eq!(
        read_rows(&factory.storage, &first.data_files[0].path).await?,
        vec![(10, 100), (20, 200), (30, 300)]
    );
    insta::assert_debug_snapshot!(first.deletion_vectors, @"[]");
    assert_eq!(first.index_delta, Some(Uuid::from_u128(1)));
    assert_eq!(rejected.index_delta, Some(Uuid::from_u128(2)));
    assert_eq!(retry.index_delta, Some(Uuid::from_u128(3)));
    assert_eq!(
        history.accepted,
        vec![
            IndexDelta {
                id: Uuid::from_u128(1),
                range: first.range
            },
            IndexDelta {
                id: Uuid::from_u128(3),
                range: retry.range
            },
        ]
    );
    assert_eq!(
        history.inputs,
        vec![
            (0, vec![]),
            (
                0,
                vec![IndexDelta {
                    id: Uuid::from_u128(1),
                    range: first.range
                }]
            ),
            (1, vec![]),
        ]
    );
    for result in [rejected, retry] {
        assert_eq!(result.range, WalRange { start: 1, end: 2 });
        assert_eq!(result.data_files.len(), 1);
        assert_eq!(
            read_rows(&factory.storage, &result.data_files[0].path).await?,
            vec![(20, 201)]
        );
        assert_eq!(result.deletion_vectors.len(), 1);
        let replacement = &result.deletion_vectors[0];
        // Neither the unacknowledged initial result nor the rejected replacement
        // may cause us to delete from the wrong data file or inherit a rejected DV.
        assert_eq!(replacement.previous, None);
        assert_eq!(replacement.current.target_file, first.data_files[0].path);
        assert_eq!(replacement.current.cardinality, 1);
        let bytes = factory
            .storage
            .read(&replacement.current.puffin_path)
            .await?;
        assert_eq!(bytes.len() as u64, replacement.current.file_size);
        let offset = replacement.current.offset as usize;
        let length = replacement.current.length as usize;
        let blob = &bytes[offset..offset + length];
        assert_eq!(&blob[4..8], &[0xd1, 0xd3, 0x39, 0x64]);
        let positions = RoaringTreemap::deserialize_from(Cursor::new(&blob[8..blob.len() - 4]))?;
        assert_eq!(positions.iter().collect::<Vec<_>>(), vec![1]);
    }
    assert_ne!(rejected.data_files[0].path, retry.data_files[0].path);
    assert_eq!(wal.batches().await, wal_before);
    Ok(())
}

#[typetag::serde]
impl StorageFactory for SharedStorageFactory {
    fn build(&self, _: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.storage.clone()))
    }
}

impl CatalogServer {
    async fn start(metadata: TableMetadata) -> Result<Self> {
        let router = Router::new()
            .route(
                "/v1/config",
                get(|| async { Json(json!({"defaults": {}, "overrides": {}})) }),
            )
            .route(
                "/v1/namespaces/{namespace}/tables/{table}",
                get(move || {
                    let metadata = metadata.clone();
                    async move {
                        Json(json!({
                            "metadata": metadata,
                            "metadata-location": "memory://table/metadata.json",
                            "config": {}
                        }))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let config = CatalogConfig::Rest(RestCatalogConfig {
            uri: format!("http://{}", listener.local_addr()?),
            warehouse: None,
            properties: HashMap::new(),
        });
        let task = tokio::spawn(async move {
            // PANIC: The fixture listener must remain available until the server is dropped.
            axum::serve(listener, router)
                .await
                .expect("serve fixture catalog");
        });
        Ok(Self { config, task })
    }
}

impl Drop for CatalogServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[async_trait::async_trait]
impl MetadataClient for Client {
    async fn get_catalog(
        &self,
        id: &CatalogId,
    ) -> se_metadata_client::Result<Option<CatalogConfig>> {
        assert_eq!(id, &self.assignment.token.catalog_id);
        Ok(Some(self.catalog.clone()))
    }

    async fn resolve_table(
        &self,
        _: &CatalogId,
        _: &TableIdent,
    ) -> se_metadata_client::Result<TableMetadata> {
        // PANIC: Workers load assigned tables directly from the catalog.
        unreachable!("unexpected name resolution")
    }

    async fn claim_task(
        &self,
        worker_id: Uuid,
    ) -> se_metadata_client::Result<Option<TaskAssignment>> {
        assert_eq!(worker_id, self.assignment.token.worker_id);
        Ok(Some(self.assignment.clone()))
    }

    fn heartbeat_task(&self, token: TaskToken) -> Heartbeat {
        assert_eq!(token, self.assignment.token);
        Heartbeat::new(|| async { Ok(()) })
    }

    async fn complete_materialization(
        &self,
        token: TaskToken,
        result: MaterializationResult,
    ) -> se_metadata_client::Result<()> {
        assert_eq!(token, self.assignment.token);
        let mut history = self.history.lock().await;
        if self.accept {
            history.accepted.push(IndexDelta {
                // PANIC: Identifier-schema materialization must stage an index delta.
                id: result.index_delta.expect("upsert delta"),
                range: result.range,
            });
        }
        history.submitted.push(result);
        self.cancel.cancel();
        if self.acknowledge {
            Ok(())
        } else {
            Err(se_metadata_client::Error::TaskFenced)
        }
    }

    async fn materialization_input(
        &self,
        token: TaskToken,
        applied_through: u64,
    ) -> se_metadata_client::Result<MaterializationInput> {
        assert_eq!(token, self.assignment.token);
        let Task::Materialize { range, .. } = self.assignment.task else {
            // PANIC: This fixture assigns only materialization tasks.
            unreachable!("not materialization")
        };
        let mut history = self.history.lock().await;
        let deltas = history
            .accepted
            .iter()
            .filter(|delta| delta.range.end > applied_through && delta.range.end <= range.start)
            .cloned()
            .collect::<Vec<_>>();
        history.inputs.push((applied_through, deltas.clone()));
        Ok(MaterializationInput { deltas })
    }

    async fn reconciliation_input(
        &self,
        _: TaskToken,
    ) -> se_metadata_client::Result<ReconciliationInput> {
        // PANIC: This fixture assigns only materialization tasks.
        unreachable!("unexpected reconciliation input")
    }

    async fn complete_reconciliation(
        &self,
        _: TaskToken,
        _: PreparedCommit,
    ) -> se_metadata_client::Result<()> {
        // PANIC: This fixture assigns only materialization tasks.
        unreachable!("unexpected reconciliation completion")
    }

    async fn fail_task(&self, _: TaskToken) -> se_metadata_client::Result<()> {
        self.history.lock().await.failures += 1;
        self.cancel.cancel();
        Ok(())
    }

    async fn wal_append(
        &self,
        _: &CatalogId,
        _: Uuid,
        batch: se_wal::BatchMetadata,
    ) -> se_wal::Result<()> {
        self.wal.commit(batch).await
    }

    async fn wal_read_after(
        &self,
        catalog: &CatalogId,
        uuid: Uuid,
        sequence: Option<u64>,
        max: usize,
    ) -> se_wal::Result<Vec<se_wal::BatchDescriptor>> {
        assert_eq!(catalog, &self.assignment.token.catalog_id);
        assert_eq!(uuid, self.assignment.token.table_uuid);
        self.wal.read_after(sequence, max).await
    }
}

fn table_metadata() -> Result<TableMetadata> {
    let schema = Schema::builder()
        .with_fields([
            NestedField::required(1, "key", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "value", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .with_identifier_field_ids([1])
        .build()?;
    Ok(TableMetadataBuilder::new(
        schema,
        UnboundPartitionSpec::default(),
        SortOrder::unsorted_order(),
        "memory://table".into(),
        FormatVersion::V3,
        HashMap::new(),
    )?
    .build()?
    .metadata)
}

async fn read_rows(storage: &MemoryStorage, path: &str) -> Result<Vec<(i64, i64)>> {
    let bytes = storage.read(path).await?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch?;
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("wrong key type")?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("wrong value type")?;
        rows.extend((0..batch.num_rows()).map(|row| (keys.value(row), values.value(row))));
    }
    Ok(rows)
}
