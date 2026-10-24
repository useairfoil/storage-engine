use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use buffer::{CompressionType, FlushHook, Producer, ProducerConfig, SystemClock, parse_manifest};
use bytes::Bytes;
use slatedb::object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use tokio::sync::Semaphore;

#[tokio::test]
async fn one_hook_per_segment_without_delaying_writes_or_flushes() {
    let store = Arc::new(InMemory::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let hook: FlushHook = {
        let (store, calls, entered, release) = (
            store.clone(),
            calls.clone(),
            entered.clone(),
            release.clone(),
        );
        Arc::new(move || {
            let (store, calls, entered, release) = (
                store.clone(),
                calls.clone(),
                entered.clone(),
                release.clone(),
            );
            Box::pin(async move {
                let manifest = parse_manifest(
                    store
                        .get(&Path::from("manifest"))
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap(),
                )
                .unwrap();
                let entry = &manifest.entries()[0];
                assert_eq!(entry.metadata.len(), 3);
                store
                    .get(&Path::from(entry.location.as_str()))
                    .await
                    .unwrap();
                calls.fetch_add(1, Ordering::SeqCst);
                entered.add_permits(1);
                release.acquire().await.unwrap().forget();
            })
        })
    };
    let batch = batch();
    let producer = Producer::new_with_flush_hook(
        config(),
        store,
        batch.schema(),
        Arc::new(SystemClock),
        Some(hook),
    )
    .unwrap();
    producer.flush().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let mut first = producer.produce(batch.clone(), Bytes::new()).await.unwrap();
    let mut second = producer.produce(batch.clone(), Bytes::new()).await.unwrap();

    // Disconnecting a writer must not prevent scheduling the segment.
    drop(producer.produce(batch, Bytes::new()).await.unwrap());

    let (flush, ()) = tokio::join!(producer.flush(), async {
        entered.acquire().await.unwrap().forget();
        insta::assert_compact_debug_snapshot!((first.watcher.result(), second.watcher.result()), @"(Some(Ok(())), Some(Ok(())))");
        // The writer must accept further flushes while the hook is still blocked.
        tokio::time::timeout(Duration::from_secs(1), producer.flush())
            .await
            .unwrap()
            .unwrap();
        release.add_permits(1);
    });

    flush.unwrap();
    first.watcher.await_durable().await.unwrap();
    second.watcher.await_durable().await.unwrap();
    producer.close().await.unwrap();

    insta::assert_snapshot!(calls.load(Ordering::SeqCst), @"1");
}

fn config() -> ProducerConfig {
    ProducerConfig {
        data_path_prefix: "wal".into(),
        manifest_path: "manifest".into(),
        flush_interval: Duration::from_secs(3600),
        flush_size_bytes: usize::MAX,
        max_buffered_inputs: 10,
        batch_compression: CompressionType::None,
    }
}

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![17]))],
    )
    .unwrap()
}
