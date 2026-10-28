use std::{sync::Arc, time::Duration};

use arrow_array::RecordBatch;
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow_schema::SchemaRef;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use se_common::{clock::SystemClock, id::IdGenerator, rand::SeRand};
use serde::{Deserialize, Serialize};
use serde_with::{DurationMilliSeconds, serde_as};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::{BatchCommit, BatchMetadata, Error, Result};

pub struct Producer {
    sender: mpsc::Sender<ProduceMessage>,
    cancellation_token: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
    clock: Arc<dyn SystemClock>,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerOptions {
    #[serde(default = "default_data_path_prefix")]
    pub data_path_prefix: String,

    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(default = "default_flush_interval")]
    pub flush_interval: Duration,

    #[serde(default = "default_flush_size_bytes")]
    pub flush_size_bytes: usize,

    #[serde(default = "default_max_buffered_inputs")]
    pub max_buffered_inputs: usize,

    #[serde(default)]
    pub batch_compression: CompressionType,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    #[default]
    None = 0,
    Zstd = 1,
}

#[derive(Clone)]
pub struct DurabilityWatcher {
    receiver: watch::Receiver<Option<Result<()>>>,
}

pub struct WriteHandle {
    pub watcher: DurabilityWatcher,
    pub ingestion_time_ms: i64,
}

type Notifier = watch::Sender<Option<Result<()>>>;

enum ProduceMessage {
    Add {
        batch: RecordBatch,
        ingestion_time_ms: i64,
        notifier: Notifier,
    },
    Flush {
        result_sender: oneshot::Sender<Result<()>>,
    },
}

struct Batch {
    writer: StreamWriter<Vec<u8>>,
    started_at_ms: Option<i64>,
    notifiers: Vec<Notifier>,
}

struct BatchWriterTask {
    options: ProducerOptions,
    object_store: Arc<dyn ObjectStore>,
    batch_commit: Arc<dyn BatchCommit>,
    target_schema: SchemaRef,
    schema_id: i32,
    clock: Arc<dyn SystemClock>,
    rng: Arc<SeRand>,
    batch: Batch,
}

impl Producer {
    /// Creates a WAL producer for batches sharing the supplied Arrow schema.
    pub fn new(
        options: ProducerOptions,
        object_store: Arc<dyn ObjectStore>,
        batch_commit: Arc<dyn BatchCommit>,
        target_schema: SchemaRef,
        schema_id: i32,
        clock: Arc<dyn SystemClock>,
        rng: Arc<SeRand>,
    ) -> Result<Self> {
        if options.max_buffered_inputs == 0 || options.flush_interval.is_zero() {
            return Err(Error::InvalidInput(
                "buffer capacity and flush interval must be positive".to_string(),
            ));
        }

        let batch = Batch::new(&target_schema, options.batch_compression)?;
        let (sender, receiver) = mpsc::channel(options.max_buffered_inputs);
        let cancellation_token = CancellationToken::new();
        let mut task = BatchWriterTask {
            options,
            object_store,
            batch_commit,
            target_schema,
            schema_id,
            clock: clock.clone(),
            rng,
            batch,
        };
        let shutdown = cancellation_token.clone();
        let handle = tokio::spawn(async move { task.run(receiver, shutdown).await });

        Ok(Self {
            sender,
            cancellation_token,
            handle,
            clock,
        })
    }

    /// Buffers a nonempty record batch, applying backpressure when full.
    pub async fn produce(&self, batch: RecordBatch) -> Result<WriteHandle> {
        if batch.num_rows() == 0 {
            return Err(Error::InvalidInput("batch must not be empty".to_string()));
        }

        let ingestion_time_ms = self.clock.now().timestamp_millis();
        let (notifier, receiver) = watch::channel(None);
        self.sender
            .send(ProduceMessage::Add {
                batch,
                ingestion_time_ms,
                notifier,
            })
            .await
            .map_err(|_| Error::Closed)?;

        Ok(WriteHandle {
            watcher: DurabilityWatcher { receiver },
            ingestion_time_ms,
        })
    }

    /// Waits for buffered batches to be uploaded and committed.
    pub async fn flush(&self) -> Result<()> {
        let (result_sender, result_receiver) = oneshot::channel();
        self.sender
            .send(ProduceMessage::Flush { result_sender })
            .await
            .map_err(|_| Error::Closed)?;

        result_receiver.await.map_err(|_| Error::Closed)?
    }

    /// Flushes pending batches and shuts down the producer.
    pub async fn close(self) -> Result<()> {
        let result = self.flush().await;
        self.cancellation_token.cancel();
        self.handle
            .await
            .map_err(|e| Error::Writer(e.to_string()))?;

        result
    }
}

impl Default for ProducerOptions {
    fn default() -> Self {
        Self {
            data_path_prefix: default_data_path_prefix(),
            flush_interval: default_flush_interval(),
            flush_size_bytes: default_flush_size_bytes(),
            max_buffered_inputs: default_max_buffered_inputs(),
            batch_compression: CompressionType::default(),
        }
    }
}

impl DurabilityWatcher {
    /// Returns the commit result, or `None` while the batch is still pending.
    pub fn result(&self) -> Option<Result<()>> {
        self.receiver.borrow().clone()
    }

    /// Waits until the batch has been uploaded and committed.
    pub async fn await_durable(&mut self) -> Result<()> {
        self.receiver
            .wait_for(|result| result.is_some())
            .await
            .map_err(|_| Error::Closed)?
            .clone()
            .ok_or(Error::Closed)?
    }
}

impl Batch {
    fn new(schema: &SchemaRef, compression: CompressionType) -> Result<Self> {
        let codec = match compression {
            CompressionType::None => None,
            CompressionType::Zstd => Some(arrow_ipc::CompressionType::ZSTD),
        };
        let options = IpcWriteOptions::default()
            .try_with_compression(codec)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        let writer = StreamWriter::try_new_with_options(Vec::new(), schema, options)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        Ok(Self {
            writer,
            started_at_ms: None,
            notifiers: Vec::new(),
        })
    }

    fn add(
        &mut self,
        batch: RecordBatch,
        ingestion_time_ms: i64,
        notifier: Notifier,
    ) -> Result<()> {
        self.writer
            .write(&batch)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        self.started_at_ms.get_or_insert(ingestion_time_ms);
        self.notifiers.push(notifier);

        Ok(())
    }

    fn size_bytes(&self) -> usize {
        self.writer.get_ref().len()
    }
}

impl BatchWriterTask {
    async fn run(&mut self, mut rx: mpsc::Receiver<ProduceMessage>, shutdown: CancellationToken) {
        loop {
            let sleep_duration = match self.batch.started_at_ms {
                Some(started_at_ms) => {
                    self.options
                        .flush_interval
                        .saturating_sub(Duration::from_millis(
                            self.clock
                                .now()
                                .timestamp_millis()
                                .saturating_sub(started_at_ms)
                                .max(0) as u64,
                        ))
                }
                None => self.options.flush_interval,
            };

            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = self.clock.sleep(sleep_duration) => {
                    let _ = self.write_batch().await;
                },
                message = rx.recv() => {
                    match message {
                        Some(ProduceMessage::Add { batch, ingestion_time_ms, notifier }) => {
                            let result = if batch.schema().as_ref() != self.target_schema.as_ref() {
                                Err(Error::InvalidInput("record batch schema does not match target schema".to_string()))
                            } else {
                                self.batch.add(batch, ingestion_time_ms, notifier.clone())
                            };

                            if let Err(error) = result {
                                let _ = notifier.send(Some(Err(error)));
                            } else if self.batch.size_bytes() >= self.options.flush_size_bytes {
                                let _ = self.write_batch().await;
                            }
                        },
                        Some(ProduceMessage::Flush { result_sender }) => {
                            let _ = result_sender.send(self.write_batch().await);
                        },
                        None => return,
                    }
                },
            }
        }
    }

    async fn write_batch(&mut self) -> Result<()> {
        if self.batch.notifiers.is_empty() {
            return Ok(());
        }

        let next = Batch::new(&self.target_schema, self.options.batch_compression)?;
        let batch = std::mem::replace(&mut self.batch, next);
        let result = self.write_and_commit(batch.writer).await;

        for notifier in batch.notifiers {
            let _ = notifier.send(Some(result.clone()));
        }

        result
    }

    async fn write_and_commit(&mut self, writer: StreamWriter<Vec<u8>>) -> Result<()> {
        let payload = writer
            .into_inner()
            .map_err(|e| Error::Serialization(e.to_string()))?;
        let id = self.rng.rng().gen_ulid(self.clock.as_ref());
        let path = Path::from(format!("{}/{}.batch", self.options.data_path_prefix, id));
        self.object_store
            .put(&path, PutPayload::from(payload))
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        self.batch_commit
            .commit(BatchMetadata {
                schema_id: self.schema_id,
                location: path.to_string(),
                timestamp: self.clock.now(),
            })
            .await
    }
}

fn default_data_path_prefix() -> String {
    "wal".to_string()
}

fn default_flush_interval() -> Duration {
    Duration::from_millis(1_000)
}

fn default_flush_size_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_max_buffered_inputs() -> usize {
    1_000
}
