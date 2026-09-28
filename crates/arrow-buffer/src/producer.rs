use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use arrow_array::RecordBatch;
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow_schema::SchemaRef;
use bytes::Bytes;
use common::clock::Clock;
use slatedb::object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    config::ProducerConfig,
    error::{Error, Result},
    metric_names,
    model::CompressionType,
    queue::{Metadata, QueueProducer},
    util::millis,
};

type Notifier = tokio::sync::watch::Sender<Option<Result<()>>>;

#[derive(Clone)]
pub struct DurabilityWatcher {
    receiver: tokio::sync::watch::Receiver<Option<Result<()>>>,
}

impl DurabilityWatcher {
    /// Return the outcome of the write if the batch has already been flushed,
    /// or `None` if the flush has not completed yet.
    pub fn result(&self) -> Option<Result<()>> {
        self.receiver.borrow().clone()
    }

    /// Wait until the batch containing this write has been durably flushed.
    pub async fn await_durable(&mut self) -> Result<()> {
        self.receiver
            .wait_for(|v| v.is_some())
            .await
            .map_err(|_| Error::Storage("buffer shut down".to_string()))?
            .clone()
            .expect("value must be present after wait_for")
    }
}

pub struct WriteHandle {
    pub watcher: DurabilityWatcher,
    pub ingestion_time_ms: i64,
}

enum ProduceMessage {
    Add {
        batch: RecordBatch,
        metadata: Bytes,
        ingestion_time_ms: i64,
        notifier: Notifier,
    },
    Flush {
        result_sender: tokio::sync::oneshot::Sender<Result<()>>,
    },
}

struct DataAndNotifiers {
    target_schema: SchemaRef,
    compression: CompressionType,
    writer: StreamWriter<Vec<u8>>,
    row_count: usize,
    raw_bytes: u64,
    metadata: Vec<Metadata>,
    notifiers: Vec<Notifier>,
}

impl DataAndNotifiers {
    fn new(target_schema: SchemaRef, compression: CompressionType) -> Result<Self> {
        let codec = match compression {
            CompressionType::None => None,
            CompressionType::Zstd => Some(arrow_ipc::CompressionType::ZSTD),
        };
        let options = IpcWriteOptions::default()
            .try_with_compression(codec)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        let writer = StreamWriter::try_new_with_options(Vec::new(), &target_schema, options)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        Ok(Self {
            target_schema,
            compression,
            writer,
            row_count: 0,
            raw_bytes: 0,
            metadata: Vec::new(),
            notifiers: Vec::new(),
        })
    }

    fn add(
        &mut self,
        batch: RecordBatch,
        metadata: Bytes,
        ingestion_time_ms: i64,
        notifier: Notifier,
    ) -> Result<usize> {
        if batch.schema().as_ref() != self.target_schema.as_ref() {
            return Err(Error::InvalidInput(
                "record batch schema does not match target schema".to_string(),
            ));
        }
        let Some(row_count) = self.row_count.checked_add(batch.num_rows()) else {
            return Err(Error::InvalidInput("batch row count overflow".to_string()));
        };
        if row_count > u32::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "batch row count {row_count} exceeds u32::MAX"
            )));
        }
        let start_index = self.row_count as u32;
        let written_before = self.writer.get_ref().len();
        self.writer
            .write(&batch)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        let written_bytes = self.writer.get_ref().len() - written_before;
        self.row_count = row_count;
        self.raw_bytes += batch.get_array_memory_size() as u64;
        self.metadata.push(Metadata {
            start_index,
            ingestion_time_ms,
            payload: metadata,
        });
        self.notifiers.push(notifier);
        Ok(written_bytes)
    }

    fn is_empty(&self) -> bool {
        self.notifiers.is_empty()
    }
}

struct Batch {
    data_and_notifiers: DataAndNotifiers,
    size_bytes: usize,
    started_at: Option<SystemTime>,
}

impl Batch {
    fn new(target_schema: SchemaRef, compression: CompressionType) -> Result<Self> {
        Ok(Self {
            data_and_notifiers: DataAndNotifiers::new(target_schema, compression)?,
            size_bytes: 0,
            started_at: None,
        })
    }

    fn add(
        &mut self,
        batch: RecordBatch,
        metadata: Bytes,
        ingestion_time_ms: i64,
        notifier: Notifier,
        now: SystemTime,
    ) -> Result<()> {
        let metadata_len = metadata.len();
        let written_bytes =
            self.data_and_notifiers
                .add(batch, metadata, ingestion_time_ms, notifier)?;
        self.size_bytes += written_bytes + metadata_len;
        if self.started_at.is_none() {
            self.started_at = Some(now);
        }
        Ok(())
    }

    fn take(&mut self) -> Result<DataAndNotifiers> {
        let next = DataAndNotifiers::new(
            Arc::clone(&self.data_and_notifiers.target_schema),
            self.data_and_notifiers.compression,
        )?;
        self.size_bytes = 0;
        self.started_at = None;
        Ok(std::mem::replace(&mut self.data_and_notifiers, next))
    }

    fn is_empty(&self) -> bool {
        self.data_and_notifiers.is_empty()
    }
}

struct BatchWriterTask {
    object_store: Arc<dyn ObjectStore>,
    producer: Arc<QueueProducer>,
    data_path_prefix: String,
    flush_interval: Duration,
    flush_size_bytes: usize,
    batch: Batch,
    clock: Arc<dyn Clock>,
}

impl BatchWriterTask {
    fn new(
        object_store: Arc<dyn ObjectStore>,
        producer: Arc<QueueProducer>,
        target_schema: SchemaRef,
        config: &ProducerConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        Ok(Self {
            object_store,
            producer,
            data_path_prefix: config.data_path_prefix.clone(),
            flush_interval: config.flush_interval,
            flush_size_bytes: config.flush_size_bytes,
            batch: Batch::new(target_schema, config.batch_compression)?,
            clock,
        })
    }

    async fn run(&mut self, mut rx: mpsc::Receiver<ProduceMessage>, shutdown: CancellationToken) {
        loop {
            let sleep_duration = match self.batch.started_at {
                Some(started) => (started + self.flush_interval)
                    .duration_since(self.clock.now())
                    .unwrap_or(Duration::ZERO),
                None => self.flush_interval,
            };

            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    return;
                },
                msg = rx.recv() => {
                    match msg {
                        Some(ProduceMessage::Add { batch, metadata, ingestion_time_ms, notifier }) => {
                            if let Err(e) = self.batch.add(batch, metadata, ingestion_time_ms, notifier.clone(), self.clock.now()) {
                                let _ = notifier.send(Some(Err(e)));
                            } else if self.batch.size_bytes >= self.flush_size_bytes {
                                let _ = self.write_batch().await;
                            }
                        }
                        Some(ProduceMessage::Flush { result_sender }) => {
                            let _ = result_sender.send(self.write_batch().await);
                        }
                        None => break,
                    }
                },
                _ = tokio::time::sleep(sleep_duration) => {
                    let _ = self.write_batch().await;
                },
            }
        }
    }

    async fn write_batch(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let data_and_notifiers = self.batch.take()?;
        let result = self
            .write_and_enqueue(
                data_and_notifiers.writer,
                data_and_notifiers.metadata,
                data_and_notifiers.row_count,
                data_and_notifiers.raw_bytes,
            )
            .await;

        for notifier in data_and_notifiers.notifiers {
            let _ = notifier.send(Some(result.clone()));
        }

        result
    }

    async fn write_and_enqueue(
        &self,
        writer: StreamWriter<Vec<u8>>,
        metadata: Vec<Metadata>,
        row_count: usize,
        raw_bytes: u64,
    ) -> Result<()> {
        let start = Instant::now();
        let payload = writer
            .into_inner()
            .map_err(|e| Error::Serialization(e.to_string()))?;
        let written_bytes = payload.len() as u64;

        let id = ulid::Ulid::new();
        let path = Path::from(format!("{}/{}.batch", self.data_path_prefix, id));
        self.object_store
            .put(&path, PutPayload::from(Bytes::from(payload)))
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        self.producer.enqueue(path.to_string(), metadata).await?;

        metrics::counter!(metric_names::BATCHES_FLUSHED).increment(1);
        metrics::counter!(metric_names::ENTRIES_FLUSHED).increment(row_count as u64);
        metrics::counter!(metric_names::BYTES_FLUSHED).increment(raw_bytes);
        metrics::counter!(metric_names::BYTES_WRITTEN).increment(written_bytes);
        metrics::histogram!(metric_names::FLUSH_DURATION_SECONDS)
            .record(start.elapsed().as_secs_f64());

        Ok(())
    }
}

struct BatchWriter {
    producer: Arc<QueueProducer>,
    sender: mpsc::Sender<ProduceMessage>,
    cancellation_token: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

impl BatchWriter {
    fn new(
        object_store: Arc<dyn ObjectStore>,
        target_schema: SchemaRef,
        config: &ProducerConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(config.max_buffered_inputs);
        let producer = Arc::new(QueueProducer::with_object_store(
            config.manifest_path.clone(),
            object_store.clone(),
        ));
        let mut task =
            BatchWriterTask::new(object_store, producer.clone(), target_schema, config, clock)?;
        let shutdown = CancellationToken::new();
        let cancellation_token = shutdown.clone();
        let handle = tokio::spawn(async move { task.run(receiver, shutdown).await });
        Ok(Self {
            producer,
            sender,
            cancellation_token,
            handle,
        })
    }

    async fn add(
        &self,
        batch: RecordBatch,
        metadata: Bytes,
        ingestion_time_ms: i64,
    ) -> Result<DurabilityWatcher> {
        let (notifier_sender, notifier_receiver) = tokio::sync::watch::channel(None);
        self.sender
            .send(ProduceMessage::Add {
                batch,
                metadata,
                ingestion_time_ms,
                notifier: notifier_sender,
            })
            .await
            .map_err(|_| Error::Storage("buffer shut down".to_string()))?;
        Ok(DurabilityWatcher {
            receiver: notifier_receiver,
        })
    }

    async fn flush(&self) -> Result<()> {
        let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
        self.sender
            .send(ProduceMessage::Flush { result_sender })
            .await
            .map_err(|_| Error::Storage("batch writer task not running".to_string()))?;
        result_receiver
            .await
            .map_err(|_| Error::Storage("batch writer task not running".to_string()))?
    }

    fn conflict_rate(&self) -> f64 {
        self.producer.conflict_rate()
    }

    async fn close(self) -> Result<()> {
        let result = self.flush().await;
        self.cancellation_token.cancel();
        let _ = self.handle.await;
        result
    }
}

pub struct Producer {
    writer: BatchWriter,
    clock: Arc<dyn Clock>,
}

impl Producer {
    /// Create a buffer using a pre-built object store and the schema of its data.
    pub fn new(
        config: ProducerConfig,
        object_store: Arc<dyn ObjectStore>,
        target_schema: SchemaRef,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        metric_names::describe_buffer_metrics();
        let writer = BatchWriter::new(object_store, target_schema, &config, clock.clone())?;
        Ok(Self { writer, clock })
    }

    /// Submit a record batch and associated metadata for ingestion.
    ///
    /// Returns a [`WriteHandle`] that can be used to check or await durability.
    /// Applies backpressure when the message buffer is full.
    ///
    /// Returns [`Error::InvalidInput`] if the batch has no rows or if `metadata`
    /// exceeds 2³²−1 bytes. Schema or row-count errors are reported through
    /// the handle's durability watcher.
    pub async fn produce(&self, batch: RecordBatch, metadata: Bytes) -> Result<WriteHandle> {
        if batch.num_rows() == 0 {
            return Err(Error::InvalidInput("batch must not be empty".to_string()));
        }
        if metadata.len() > u32::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "metadata size {} exceeds u32::MAX",
                metadata.len()
            )));
        }
        let ingestion_time_ms = millis(self.clock.now());
        let durability_watcher = self.writer.add(batch, metadata, ingestion_time_ms).await?;
        Ok(WriteHandle {
            watcher: durability_watcher,
            ingestion_time_ms,
        })
    }

    /// Flush the current IPC stream, blocking until all pending batches are durably written.
    pub async fn flush(&self) -> Result<()> {
        self.writer.flush().await
    }

    /// Return the fraction of manifest writes that encountered optimistic-concurrency conflicts.
    pub fn conflict_rate(&self) -> f64 {
        self.writer.conflict_rate()
    }

    /// Shut down the buffer, flushing any remaining buffered batches before returning.
    pub async fn close(self) -> Result<()> {
        self.writer.close().await
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, time::UNIX_EPOCH};

    use arrow_array::StringArray;
    use arrow_ipc::reader::StreamReader;
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use common::clock::{MockClock, SystemClock};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use slatedb::object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};

    use super::*;
    use crate::{
        config::ProducerConfig,
        queue::{Manifest, QueueEntry},
    };

    async fn read_manifest_entries(store: &Arc<dyn ObjectStore>, path: &str) -> Vec<QueueEntry> {
        let path = slatedb::object_store::path::Path::from(path);
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        let manifest = Manifest::from_bytes(data).unwrap();
        manifest.iter().map(|e| e.unwrap()).collect()
    }

    fn test_config() -> ProducerConfig {
        ProducerConfig {
            data_path_prefix: "test-ingest".to_string(),
            manifest_path: "test/manifest".to_string(),
            flush_interval: Duration::from_hours(24),
            flush_size_bytes: 64 * 1024 * 1024,
            max_buffered_inputs: 1000,
            batch_compression: CompressionType::None,
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]))
    }

    fn record_batch(values: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(StringArray::from(values.to_vec()))],
        )
        .unwrap()
    }

    fn decode_stream(data: Bytes) -> Vec<RecordBatch> {
        StreamReader::try_new(Cursor::new(data), None)
            .unwrap()
            .map(|batch| batch.unwrap())
            .collect()
    }

    fn format_batches(batches: &[RecordBatch]) -> String {
        batches
            .iter()
            .map(|batch| {
                pretty_format_batches(std::slice::from_ref(batch))
                    .unwrap()
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n\n")
    }

    #[test]
    fn batch_retains_target_schema_after_flush() {
        let schema = test_schema();
        let mut batch = Batch::new(Arc::clone(&schema), CompressionType::None).unwrap();
        let (first_notifier, _) = tokio::sync::watch::channel(None);
        batch
            .add(
                record_batch(&["first", "second"]),
                Bytes::new(),
                1,
                first_notifier,
                SystemTime::UNIX_EPOCH,
            )
            .unwrap();
        let first = batch.take().unwrap();
        assert!(Arc::ptr_eq(&first.target_schema, &schema));
        let first_rows = first.row_count;
        insta::assert_snapshot!(
            format_batches(&decode_stream(Bytes::from(first.writer.into_inner().unwrap()))),
            @"
        +--------+
        | value  |
        +--------+
        | first  |
        | second |
        +--------+
        "
        );

        let (second_notifier, _) = tokio::sync::watch::channel(None);
        batch
            .add(
                record_batch(&["third"]),
                Bytes::new(),
                2,
                second_notifier,
                SystemTime::UNIX_EPOCH,
            )
            .unwrap();
        let second = batch.take().unwrap();
        assert!(Arc::ptr_eq(&second.target_schema, &schema));
        insta::assert_compact_debug_snapshot!([first_rows, second.row_count], @"[2, 1]");
        insta::assert_snapshot!(
            format_batches(&decode_stream(Bytes::from(second.writer.into_inner().unwrap()))),
            @"
        +-------+
        | value |
        +-------+
        | third |
        +-------+
        "
        );
    }

    #[tokio::test]
    async fn should_ingest_entries_and_enqueue_location() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer = Producer::new(
            test_config(),
            store.clone(),
            test_schema(),
            Arc::new(SystemClock),
        )
        .unwrap();

        buffer
            .produce(record_batch(&["data1"]), Bytes::new())
            .await
            .unwrap();
        buffer
            .produce(record_batch(&["data2"]), Bytes::new())
            .await
            .unwrap();
        buffer.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
        assert!(entries[0].location.starts_with("test-ingest/"));
        assert!(entries[0].location.ends_with(".batch"));
    }

    #[tokio::test]
    async fn should_write_valid_batch_to_object_store() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer = Producer::new(
            test_config(),
            store.clone(),
            test_schema(),
            Arc::new(SystemClock),
        )
        .unwrap();

        buffer
            .produce(record_batch(&["mydata", "another"]), Bytes::new())
            .await
            .unwrap();
        buffer.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        let path = Path::from(entries[0].location.as_str());
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        insta::assert_snapshot!(format_batches(&decode_stream(data)), @"
        +---------+
        | value   |
        +---------+
        | mydata  |
        | another |
        +---------+
        ");
    }

    #[tokio::test]
    async fn should_flush_when_batch_size_exceeded() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut config = test_config();
        config.flush_size_bytes = 10;

        let buffer =
            Producer::new(config, store.clone(), test_schema(), Arc::new(SystemClock)).unwrap();

        let mut watcher = buffer
            .produce(record_batch(&["some-long-data"]), Bytes::new())
            .await
            .unwrap();
        watcher.watcher.await_durable().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn should_flush_when_interval_elapsed() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut config = test_config();
        config.flush_interval = Duration::from_millis(50);
        config.flush_size_bytes = 64 * 1024 * 1024;

        let buffer =
            Producer::new(config, store.clone(), test_schema(), Arc::new(SystemClock)).unwrap();

        let mut watcher = buffer
            .produce(record_batch(&["v1"]), Bytes::new())
            .await
            .unwrap();

        assert!(watcher.watcher.result().is_none());
        let manifest_path = slatedb::object_store::path::Path::from("test/manifest");
        assert!(store.get(&manifest_path).await.is_err());

        watcher.watcher.await_durable().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn should_not_flush_below_thresholds() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer = Producer::new(
            test_config(),
            store.clone(),
            test_schema(),
            Arc::new(SystemClock),
        )
        .unwrap();

        let watcher = buffer
            .produce(record_batch(&["v"]), Bytes::new())
            .await
            .unwrap();

        assert!(watcher.watcher.result().is_none());

        let manifest_path = slatedb::object_store::path::Path::from("test/manifest");
        assert!(store.get(&manifest_path).await.is_err());

        buffer.flush().await.unwrap();

        assert!(watcher.watcher.result().unwrap().is_ok());

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn should_batch_multiple_ingests_into_single_file() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer = Producer::new(
            test_config(),
            store.clone(),
            test_schema(),
            Arc::new(SystemClock),
        )
        .unwrap();

        let watcher1 = buffer
            .produce(record_batch(&["data1", "data2"]), Bytes::new())
            .await
            .unwrap();
        let watcher2 = buffer
            .produce(record_batch(&["data3"]), Bytes::new())
            .await
            .unwrap();

        buffer.flush().await.unwrap();

        assert!(watcher1.watcher.result().unwrap().is_ok());
        assert!(watcher2.watcher.result().unwrap().is_ok());

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);

        let path = Path::from(entries[0].location.as_str());
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        insta::assert_snapshot!(format_batches(&decode_stream(data)), @"
        +-------+
        | value |
        +-------+
        | data1 |
        | data2 |
        +-------+

        +-------+
        | value |
        +-------+
        | data3 |
        +-------+
        ");
        insta::assert_compact_debug_snapshot!(
            entries[0].metadata.iter().map(|m| m.start_index).collect::<Vec<_>>(),
            @"[0, 2]"
        );
    }

    #[tokio::test]
    async fn should_reject_mismatched_schema_without_adding_it_to_stream() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer = Producer::new(
            test_config(),
            store.clone(),
            test_schema(),
            Arc::new(SystemClock),
        )
        .unwrap();
        let wrong_schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Utf8,
            false,
        )]));
        let wrong = RecordBatch::try_new(
            wrong_schema,
            vec![Arc::new(StringArray::from(vec!["wrong"]))],
        )
        .unwrap();

        let mut rejected = producer.produce(wrong, Bytes::new()).await.unwrap();
        insta::assert_compact_debug_snapshot!(rejected.watcher.await_durable().await.unwrap_err(), @r#"InvalidInput("record batch schema does not match target schema")"#);
        let accepted = producer
            .produce(record_batch(&["right"]), Bytes::new())
            .await
            .unwrap();
        producer.flush().await.unwrap();
        assert!(accepted.watcher.result().unwrap().is_ok());

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
        insta::assert_compact_debug_snapshot!(
            entries[0].metadata.iter().map(|m| m.start_index).collect::<Vec<_>>(),
            @"[0]"
        );
        let data = store
            .get(&Path::from(entries[0].location.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        insta::assert_snapshot!(format_batches(&decode_stream(data)), @"
        +-------+
        | value |
        +-------+
        | right |
        +-------+
        ");
    }

    #[tokio::test]
    async fn should_write_zstd_compressed_ipc_stream() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut config = test_config();
        config.batch_compression = CompressionType::Zstd;
        let producer =
            Producer::new(config, store.clone(), test_schema(), Arc::new(SystemClock)).unwrap();
        producer
            .produce(record_batch(&["first", "second", "third"]), Bytes::new())
            .await
            .unwrap();
        producer.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        let data = store
            .get(&Path::from(entries[0].location.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        insta::assert_snapshot!(format_batches(&decode_stream(data)), @"
        +--------+
        | value  |
        +--------+
        | first  |
        | second |
        | third  |
        +--------+
        ");
    }

    #[tokio::test]
    async fn should_apply_backpressure_when_buffer_full() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut config = test_config();
        config.max_buffered_inputs = 1;

        let buffer =
            Producer::new(config, store.clone(), test_schema(), Arc::new(SystemClock)).unwrap();

        // First ingest fills the single-slot buffer
        buffer
            .produce(record_batch(&["data1"]), Bytes::new())
            .await
            .unwrap();

        // Second ingest succeeds once the background task consumes the first message
        buffer
            .produce(record_batch(&["data2"]), Bytes::new())
            .await
            .unwrap();

        buffer.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert!(!entries.is_empty());
    }

    #[tokio::test]
    async fn should_record_metadata_and_ingestion_time_in_queue_entry() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let millis = 1_700_000_000_000i64;
        let fixed_time = UNIX_EPOCH + Duration::from_millis(millis as u64);
        let clock = Arc::new(MockClock::with_time(fixed_time));

        let buffer = Producer::new(test_config(), store.clone(), test_schema(), clock).unwrap();

        let metadata = Bytes::from(r#"{"topic":"events"}"#);
        let handle = buffer
            .produce(record_batch(&["payload"]), metadata)
            .await
            .unwrap();
        buffer.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
        insta::assert_compact_debug_snapshot!(
            (handle.ingestion_time_ms, &entries[0].metadata),
            @r#"(1700000000000, [Metadata { start_index: 0, ingestion_time_ms: 1700000000000, payload: b"{\"topic\":\"events\"}" }])"#
        );
    }

    #[tokio::test]
    async fn should_flush_remaining_entries_on_close() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer = Producer::new(
            test_config(),
            store.clone(),
            test_schema(),
            Arc::new(SystemClock),
        )
        .unwrap();

        buffer
            .produce(record_batch(&["unflushed"]), Bytes::new())
            .await
            .unwrap();

        buffer.close().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);

        let path = Path::from(entries[0].location.as_str());
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        insta::assert_snapshot!(format_batches(&decode_stream(data)), @"
        +-----------+
        | value     |
        +-----------+
        | unflushed |
        +-----------+
        ");
    }

    #[tokio::test]
    async fn should_produce_separate_batches_per_flush() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer = Producer::new(
            test_config(),
            store.clone(),
            test_schema(),
            Arc::new(SystemClock),
        )
        .unwrap();

        buffer
            .produce(record_batch(&["batch1"]), Bytes::new())
            .await
            .unwrap();
        buffer.flush().await.unwrap();

        buffer
            .produce(record_batch(&["batch2"]), Bytes::new())
            .await
            .unwrap();
        buffer.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 2);
        assert_ne!(entries[0].location, entries[1].location);

        let data1 = store
            .get(&Path::from(entries[0].location.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        insta::assert_snapshot!(format_batches(&decode_stream(data1)), @"
        +--------+
        | value  |
        +--------+
        | batch1 |
        +--------+
        ");

        let data2 = store
            .get(&Path::from(entries[1].location.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        insta::assert_snapshot!(format_batches(&decode_stream(data2)), @"
        +--------+
        | value  |
        +--------+
        | batch2 |
        +--------+
        ");
    }

    #[tokio::test]
    async fn should_not_flush_empty_batch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer = Producer::new(
            test_config(),
            store.clone(),
            test_schema(),
            Arc::new(SystemClock),
        )
        .unwrap();

        buffer.flush().await.unwrap();

        let manifest_path = slatedb::object_store::path::Path::from("test/manifest");
        assert!(store.get(&manifest_path).await.is_err());
    }

    #[tokio::test]
    async fn should_preserve_empty_string_values() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer = Producer::new(
            test_config(),
            store.clone(),
            test_schema(),
            Arc::new(SystemClock),
        )
        .unwrap();

        buffer
            .produce(record_batch(&["", ""]), Bytes::from("meta"))
            .await
            .unwrap();
        buffer.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
        assert!(!entries[0].location.is_empty());
        insta::assert_compact_debug_snapshot!(
            entries[0].metadata.iter().map(|m| (m.start_index, &m.payload)).collect::<Vec<_>>(),
            @r#"[(0, b"meta")]"#
        );

        let data = store
            .get(&Path::from(entries[0].location.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        insta::assert_snapshot!(format_batches(&decode_stream(data)), @"
        +-------+
        | value |
        +-------+
        |       |
        |       |
        +-------+
        ");
    }

    #[tokio::test]
    async fn should_preserve_empty_and_non_empty_strings_in_batch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer = Producer::new(
            test_config(),
            store.clone(),
            test_schema(),
            Arc::new(SystemClock),
        )
        .unwrap();

        buffer
            .produce(record_batch(&["a", "", "b", ""]), Bytes::from("meta"))
            .await
            .unwrap();
        buffer.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
        assert!(!entries[0].location.is_empty());
        insta::assert_compact_debug_snapshot!(
            entries[0].metadata.iter().map(|m| (m.start_index, &m.payload)).collect::<Vec<_>>(),
            @r#"[(0, b"meta")]"#
        );

        let data = store
            .get(&Path::from(entries[0].location.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        insta::assert_snapshot!(format_batches(&decode_stream(data)), @"
        +-------+
        | value |
        +-------+
        | a     |
        |       |
        | b     |
        |       |
        +-------+
        ");
    }

    #[tokio::test]
    async fn should_reject_empty_batches() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer = Producer::new(
            test_config(),
            store.clone(),
            test_schema(),
            Arc::new(SystemClock),
        )
        .unwrap();

        insta::assert_compact_debug_snapshot!(
            buffer.produce(record_batch(&[]), Bytes::new()).await.err().unwrap(),
            @r#"InvalidInput("batch must not be empty")"#
        );

        insta::assert_compact_debug_snapshot!(
            buffer.produce(record_batch(&[]), Bytes::from("meta")).await.err().unwrap(),
            @r#"InvalidInput("batch must not be empty")"#
        );
    }
}
