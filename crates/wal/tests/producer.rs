mod common;

use std::{sync::Arc, time::Duration};

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use common::{
    SCHEMA_ID, TIMESTAMP_MS, TestContext, object_count, object_list, options, producer,
    read_batches, record_batch,
};
use object_store::{ObjectStore, memory::InMemory};
use se_common::clock::{MockSystemClock, SystemClock};
use se_wal::{
    BatchCommit, BatchMetadata, CompressionType, Error, InMemoryWal, Result,
    test_util::BlockedCommit,
};

struct FailedCommit;

#[async_trait]
impl BatchCommit for FailedCommit {
    async fn commit(&self, _: BatchMetadata) -> Result<()> {
        Err(Error::Commit("commit rejected".to_string()))
    }
}

#[tokio::test]
async fn should_ingest_entries_and_commit_location() {
    let ctx = TestContext::new(options());

    ctx.producer
        .produce(record_batch(&["data1"]))
        .await
        .unwrap();
    ctx.producer
        .produce(record_batch(&["data2"]))
        .await
        .unwrap();
    ctx.producer.flush().await.unwrap();

    let batches = ctx.wal.batches().await;

    insta::assert_debug_snapshot!(batches, @r#"
    [
        BatchDescriptor {
            sequence: 0,
            metadata: BatchMetadata {
                schema_id: 17,
                location: "test-wal/01HF7YAT0094RYH2QPSBQHTF13.batch",
                timestamp: 2023-11-14T22:13:20Z,
            },
        },
    ]
    "#);

    insta::assert_debug_snapshot!(object_list(ctx.store.as_ref()).await, @r#"
    [
        "test-wal/01HF7YAT0094RYH2QPSBQHTF13.batch",
    ]
    "#);

    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_write_valid_batch_to_object_store() {
    let ctx = TestContext::new(options());
    let expected = record_batch(&["mydata", "another"]);
    ctx.producer.produce(expected.clone()).await.unwrap();
    ctx.producer.flush().await.unwrap();

    assert_eq!(
        read_batches(ctx.store.as_ref(), &ctx.wal.batches().await[0]).await,
        vec![expected]
    );

    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_flush_when_batch_size_exceeded() {
    let ctx = TestContext::new(se_wal::ProducerOptions {
        flush_size_bytes: 10,
        ..options()
    });
    let expected = record_batch(&["some-long-data"]);
    let mut write = ctx.producer.produce(expected.clone()).await.unwrap();
    write.watcher.await_durable().await.unwrap();

    let batches = ctx.wal.batches().await;
    assert_eq!(batches.len(), 1);
    assert_eq!(
        read_batches(ctx.store.as_ref(), &batches[0]).await,
        vec![expected]
    );
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_flush_when_interval_elapsed() {
    let ctx = TestContext::new(se_wal::ProducerOptions {
        flush_interval: Duration::from_millis(50),
        ..options()
    });
    let expected = record_batch(&["v1"]);
    let mut write = ctx.producer.produce(expected.clone()).await.unwrap();

    ctx.clock.advance(Duration::from_millis(49)).await;
    insta::assert_debug_snapshot!(write.watcher.result(), @"None");
    assert_eq!(ctx.wal.batches().await.len(), 0);

    ctx.clock.advance(Duration::from_millis(1)).await;
    tokio::time::timeout(Duration::from_secs(5), write.watcher.await_durable())
        .await
        .unwrap()
        .unwrap();

    let batches = ctx.wal.batches().await;
    assert_eq!(batches.len(), 1);
    assert_eq!(
        read_batches(ctx.store.as_ref(), &batches[0]).await,
        vec![expected]
    );

    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_not_flush_below_thresholds() {
    let ctx = TestContext::new(options());
    let mut write = ctx.producer.produce(record_batch(&["v"])).await.unwrap();
    ctx.clock.advance(Duration::from_secs(3599)).await;

    insta::assert_debug_snapshot!(write.watcher.result(), @"None");
    assert_eq!(ctx.wal.batches().await.len(), 0);
    assert_eq!(object_count(ctx.store.as_ref()).await, 0);

    ctx.producer.flush().await.unwrap();
    write.watcher.await_durable().await.unwrap();
    assert_eq!(ctx.wal.batches().await.len(), 1);
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_batch_multiple_ingests_into_single_file() {
    let ctx = TestContext::new(options());
    let first = record_batch(&["data1", "data2"]);
    let second = record_batch(&["data3"]);
    let mut first_write = ctx.producer.produce(first.clone()).await.unwrap();
    let mut second_write = ctx.producer.produce(second.clone()).await.unwrap();

    ctx.producer.flush().await.unwrap();

    first_write.watcher.await_durable().await.unwrap();
    second_write.watcher.await_durable().await.unwrap();
    let batches = ctx.wal.batches().await;
    insta::assert_debug_snapshot!(batches, @r#"
    [
        BatchDescriptor {
            sequence: 0,
            metadata: BatchMetadata {
                schema_id: 17,
                location: "test-wal/01HF7YAT0094RYH2QPSBQHTF13.batch",
                timestamp: 2023-11-14T22:13:20Z,
            },
        },
    ]
    "#);
    assert_eq!(
        read_batches(ctx.store.as_ref(), &batches[0]).await,
        vec![first, second]
    );

    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_reject_mismatched_schema_without_adding_it_to_stream() {
    let ctx = TestContext::new(options());
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
    let mut rejected = ctx.producer.produce(wrong).await.unwrap();
    insta::assert_compact_debug_snapshot!(rejected.watcher.await_durable().await.unwrap_err(), @r#"InvalidInput("record batch schema does not match target schema")"#);

    let expected = record_batch(&["right"]);
    let mut accepted = ctx.producer.produce(expected.clone()).await.unwrap();
    ctx.producer.flush().await.unwrap();
    accepted.watcher.await_durable().await.unwrap();
    let batches = ctx.wal.batches().await;
    assert_eq!(batches.len(), 1);
    assert_eq!(
        read_batches(ctx.store.as_ref(), &batches[0]).await,
        vec![expected]
    );

    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_write_zstd_compressed_ipc_stream() {
    let ctx = TestContext::new(se_wal::ProducerOptions {
        batch_compression: CompressionType::Zstd,
        ..options()
    });
    let expected = record_batch(&["first", "second", "third"]);
    ctx.producer.produce(expected.clone()).await.unwrap();
    ctx.producer.flush().await.unwrap();

    assert_eq!(
        read_batches(ctx.store.as_ref(), &ctx.wal.batches().await[0]).await,
        vec![expected]
    );
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_apply_backpressure_when_buffer_full_and_wait_for_commit() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let commit = Arc::new(BlockedCommit::new(Ok(())));
    let producer = producer(
        se_wal::ProducerOptions {
            max_buffered_inputs: 1,
            flush_size_bytes: 1,
            ..options()
        },
        store.clone(),
        commit.clone(),
        Arc::new(MockSystemClock::with_time(TIMESTAMP_MS)),
    );

    let mut first = producer.produce(record_batch(&["first"])).await.unwrap();
    commit.entered.notified().await;
    assert_eq!(object_count(store.as_ref()).await, 1);
    assert_eq!(commit.wal.batches().await.len(), 0);
    insta::assert_debug_snapshot!(first.watcher.result(), @"None");

    let mut second = producer.produce(record_batch(&["second"])).await.unwrap();
    let mut third = {
        let third = producer.produce(record_batch(&["third"]));
        tokio::pin!(third);
        insta::assert_debug_snapshot!(futures::poll!(&mut third).is_pending(), @"true");

        commit.release.add_permits(3);
        third.await.unwrap()
    };

    first.watcher.await_durable().await.unwrap();
    second.watcher.await_durable().await.unwrap();
    third.watcher.await_durable().await.unwrap();
    let batches = commit.wal.batches().await;
    assert_eq!(batches.len(), 3);
    assert_eq!(
        read_batches(store.as_ref(), &batches[0]).await,
        vec![record_batch(&["first"])]
    );
    assert_eq!(
        read_batches(store.as_ref(), &batches[1]).await,
        vec![record_batch(&["second"])]
    );
    assert_eq!(
        read_batches(store.as_ref(), &batches[2]).await,
        vec![record_batch(&["third"])]
    );

    producer.close().await.unwrap();
}

#[tokio::test]
async fn should_flush_remaining_entries_on_close() {
    let ctx = TestContext::new(options());
    let expected = record_batch(&["unflushed"]);
    let mut write = ctx.producer.produce(expected.clone()).await.unwrap();
    ctx.producer.close().await.unwrap();

    write.watcher.await_durable().await.unwrap();

    let batches = ctx.wal.batches().await;
    assert_eq!(batches.len(), 1);
    assert_eq!(
        read_batches(ctx.store.as_ref(), &batches[0]).await,
        vec![expected]
    );
}

#[tokio::test]
async fn should_produce_separate_batches_per_flush_and_retain_target_schema() {
    let ctx = TestContext::new(options());
    let first = record_batch(&["first", "second"]);
    let second = record_batch(&["third"]);

    ctx.producer.produce(first.clone()).await.unwrap();
    ctx.producer.flush().await.unwrap();

    ctx.producer.produce(second.clone()).await.unwrap();
    ctx.producer.flush().await.unwrap();

    let batches = ctx.wal.batches().await;
    insta::assert_debug_snapshot!(batches, @r#"
    [
        BatchDescriptor {
            sequence: 0,
            metadata: BatchMetadata {
                schema_id: 17,
                location: "test-wal/01HF7YAT0094RYH2QPSBQHTF13.batch",
                timestamp: 2023-11-14T22:13:20Z,
            },
        },
        BatchDescriptor {
            sequence: 1,
            metadata: BatchMetadata {
                schema_id: 17,
                location: "test-wal/01HF7YAT00QCTZ6W415K6P8RT5.batch",
                timestamp: 2023-11-14T22:13:20Z,
            },
        },
    ]
    "#);

    assert_eq!(
        read_batches(ctx.store.as_ref(), &batches[0]).await,
        vec![first]
    );
    assert_eq!(
        read_batches(ctx.store.as_ref(), &batches[1]).await,
        vec![second]
    );

    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_reject_empty_batches() {
    let ctx = TestContext::new(options());
    insta::assert_compact_debug_snapshot!(ctx.producer.produce(record_batch(&[])).await.err().unwrap(), @r#"InvalidInput("batch must not be empty")"#);
    ctx.producer.close().await.unwrap();
    assert_eq!(ctx.wal.batches().await.len(), 0);
}

#[tokio::test]
async fn should_propagate_commit_failure_to_flush_and_all_writers() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let producer = producer(
        options(),
        store.clone(),
        Arc::new(FailedCommit),
        Arc::new(MockSystemClock::with_time(TIMESTAMP_MS)),
    );

    let mut first = producer.produce(record_batch(&["first"])).await.unwrap();
    let mut second = producer.produce(record_batch(&["second"])).await.unwrap();

    insta::assert_compact_debug_snapshot!(producer.flush().await.unwrap_err(), @r#"Commit("commit rejected")"#);
    insta::assert_compact_debug_snapshot!(first.watcher.await_durable().await.unwrap_err(), @r#"Commit("commit rejected")"#);
    insta::assert_compact_debug_snapshot!(second.watcher.await_durable().await.unwrap_err(), @r#"Commit("commit rejected")"#);
    assert_eq!(object_count(store.as_ref()).await, 1);

    producer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_sequence_concurrent_commits_across_cloned_wal_handles() {
    let wal = InMemoryWal::default();
    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    let timestamp = MockSystemClock::with_time(TIMESTAMP_MS).now();
    let expected: Vec<_> = (0..16)
        .map(|index| BatchMetadata {
            schema_id: SCHEMA_ID,
            location: format!("wal/{index:02}.batch"),
            timestamp,
        })
        .collect();
    let mut tasks = Vec::new();

    for batch in expected.clone() {
        let wal = wal.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            wal.commit(batch).await.unwrap();
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    let entries = wal.batches().await;
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        (0..16).collect::<Vec<_>>()
    );

    let mut actual: Vec<_> = entries.into_iter().map(|entry| entry.metadata).collect();
    actual.sort_by(|left, right| left.location.cmp(&right.location));
    assert_eq!(actual, expected);
}
