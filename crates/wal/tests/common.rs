use std::{io::Cursor, sync::Arc, time::Duration};

use arrow_array::{RecordBatch, StringArray};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use se_common::{clock::MockSystemClock, rand::SeRand};
use se_wal::{BatchCommit, BatchDescriptor, InMemoryWal, Producer, ProducerOptions};

pub const TIMESTAMP_MS: i64 = 1_700_000_000_000;
pub const SCHEMA_ID: i32 = 17;

pub struct TestContext {
    pub store: Arc<dyn ObjectStore>,
    pub wal: Arc<InMemoryWal>,
    pub clock: Arc<MockSystemClock>,
    pub producer: Producer,
}

pub fn options() -> ProducerOptions {
    ProducerOptions {
        data_path_prefix: "test-wal".to_string(),
        flush_interval: Duration::from_secs(3600),
        ..ProducerOptions::default()
    }
}

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]))
}

pub fn record_batch(values: &[&str]) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(StringArray::from(values.to_vec()))]).unwrap()
}

pub fn producer(
    options: ProducerOptions,
    store: Arc<dyn ObjectStore>,
    commit: Arc<dyn BatchCommit>,
    clock: Arc<MockSystemClock>,
) -> Producer {
    Producer::new(
        options,
        store,
        commit,
        schema(),
        SCHEMA_ID,
        Arc::new(SeRand::new(42)),
        clock,
    )
    .unwrap()
}

pub async fn read_batches(store: &dyn ObjectStore, entry: &BatchDescriptor) -> Vec<RecordBatch> {
    let bytes = store
        .get(&Path::from(entry.metadata.location.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    StreamReader::try_new(Cursor::new(bytes), None)
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
}

pub async fn object_count(store: &dyn ObjectStore) -> usize {
    object_list(store).await.len()
}

pub async fn object_list(store: &dyn ObjectStore) -> Vec<String> {
    let mut paths = store
        .list(None)
        .map_ok(|object| object.location.to_string())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    paths.sort_unstable();

    paths
}

impl TestContext {
    pub fn new(options: ProducerOptions) -> Self {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal = Arc::new(InMemoryWal::default());
        let clock = Arc::new(MockSystemClock::with_time(TIMESTAMP_MS));
        let producer = producer(options, store.clone(), wal.clone(), clock.clone());

        Self {
            store,
            wal,
            clock,
            producer,
        }
    }
}
