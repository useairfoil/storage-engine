mod common;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use common::{TestContext, object_count, options, read_batches, record_batch};
use object_store::{ObjectStoreExt, path::Path};
use se_common::clock::SystemClock;
use se_wal::{
    BatchDescriptor, BatchRead, CompressionType, Consumer, Error, InMemoryWal, ProducerOptions,
    Result,
};
use tokio::sync::Notify;

#[derive(Default)]
struct ControlledRead {
    wal: Arc<InMemoryWal>,
    fail_claim: AtomicBool,
    fail_read: AtomicBool,
    fail_ack: AtomicBool,
    block_read: AtomicBool,
    block_ack: AtomicBool,
    entered: Notify,
    release: Notify,
}

#[tokio::test]
async fn should_inspect_without_fencing_or_acknowledging_the_active_consumer() {
    let ctx = TestContext::new(options());
    ctx.producer
        .produce(record_batch(&["retained"]))
        .await
        .unwrap();
    ctx.producer.flush().await.unwrap();

    let mut active = Consumer::new(ctx.wal.clone(), ctx.store.clone(), None)
        .await
        .unwrap();
    let mut observer = Consumer::new_read_only(ctx.wal.clone(), ctx.store.clone(), None);
    let descriptors = observer.next_descriptors(1).await.unwrap();
    let batch = observer
        .fetch_handle()
        .fetch(descriptors[0].clone())
        .await
        .unwrap();

    assert_eq!(batch.batches, vec![record_batch(&["retained"])]);
    insta::assert_debug_snapshot!(observer.next_descriptors(1).await.unwrap(), @"[]");
    insta::assert_compact_debug_snapshot!(observer.ack_through(0).await.unwrap_err(), @r#"InvalidInput("read-only consumer cannot acknowledge batches")"#);
    assert_eq!(ctx.wal.batches().await, descriptors);

    observer.close().await.unwrap();
    assert_eq!(ctx.wal.batches().await, descriptors);
    active.ack_through(0).await.unwrap();
    insta::assert_debug_snapshot!(ctx.wal.batches().await, @"[]");

    ctx.producer.produce(record_batch(&["new"])).await.unwrap();
    ctx.producer.flush().await.unwrap();
    let mut resumed = Consumer::new_read_only(ctx.wal.clone(), ctx.store.clone(), Some(0));
    assert_eq!(resumed.next_descriptors(1).await.unwrap()[0].sequence, 1);
    active.ack_through(1).await.unwrap();
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_fetch_producer_stream_and_metadata_without_target_schema() {
    let ctx = TestContext::new(ProducerOptions {
        batch_compression: CompressionType::Zstd,
        ..options()
    });

    let expected = vec![record_batch(&["first", "second"]), record_batch(&["third"])];
    for batch in &expected {
        ctx.producer.produce(batch.clone()).await.unwrap();
    }
    ctx.producer.flush().await.unwrap();

    let entry = ctx.wal.batches().await.remove(0);
    let mut consumer = Consumer::new(ctx.wal.clone(), ctx.store.clone(), None)
        .await
        .unwrap();

    let descriptor = consumer.next_descriptors(1).await.unwrap().remove(0);

    let batch = consumer.fetch_handle().fetch(descriptor).await.unwrap();

    assert_eq!(batch.sequence, 0);
    assert_eq!(batch.metadata, entry.metadata);
    assert_eq!(batch.metadata.timestamp, ctx.clock.now());
    assert_eq!(batch.batches, expected);
    assert_eq!(
        batch.batches,
        read_batches(ctx.store.as_ref(), &entry).await
    );
    insta::assert_debug_snapshot!(consumer.next_descriptors(1).await.unwrap(), @"[]");
    consumer.close().await.unwrap();
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_hand_out_bounded_contiguous_descriptors_without_fetching_or_acking() {
    let ctx = TestContext::new(options());
    let mut consumer = Consumer::new(ctx.wal.clone(), ctx.store.clone(), None)
        .await
        .unwrap();
    insta::assert_debug_snapshot!(consumer.next_descriptors(10).await.unwrap(), @"[]");
    for value in ["first", "second", "third"] {
        ctx.producer.produce(record_batch(&[value])).await.unwrap();
        ctx.producer.flush().await.unwrap();
    }
    let expected = ctx.wal.batches().await;
    // Descriptor discovery must not fetch the referenced objects.
    for entry in &expected {
        ctx.store
            .delete(&Path::from(entry.metadata.location.as_str()))
            .await
            .unwrap();
    }

    assert_eq!(consumer.next_descriptors(2).await.unwrap(), expected[..2]);
    assert_eq!(consumer.next_descriptors(10).await.unwrap(), expected[2..]);
    insta::assert_debug_snapshot!(consumer.next_descriptors(10).await.unwrap(), @"[]");
    assert_eq!(ctx.wal.batches().await, expected);

    ctx.producer
        .produce(record_batch(&["fourth"]))
        .await
        .unwrap();
    ctx.producer.flush().await.unwrap();
    let descriptors = consumer.next_descriptors(10).await.unwrap();
    assert_eq!(descriptors.len(), 1);
    assert_eq!(descriptors[0].sequence, 3);
    consumer.close().await.unwrap();
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_max_zero_skip_source_read_and_leave_cursor_unchanged() {
    let ctx = TestContext::new(options());
    ctx.producer
        .produce(record_batch(&["first"]))
        .await
        .unwrap();
    ctx.producer.flush().await.unwrap();
    let source = Arc::new(ControlledRead {
        wal: ctx.wal.clone(),
        ..Default::default()
    });
    source.fail_read.store(true, Ordering::SeqCst);
    let mut consumer = Consumer::new(source, ctx.store.clone(), None)
        .await
        .unwrap();

    insta::assert_debug_snapshot!(consumer.next_descriptors(0).await.unwrap(), @"[]");
    insta::assert_compact_debug_snapshot!(consumer.next_descriptors(1).await.unwrap_err(), @r#"Commit("read failed")"#);
    assert_eq!(consumer.next_descriptors(1).await.unwrap()[0].sequence, 0);
    consumer.close().await.unwrap();
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_fetch_from_cloned_handles_while_owner_advances_and_acks() {
    let ctx = TestContext::new(options());
    for value in ["first", "second", "third"] {
        ctx.producer.produce(record_batch(&[value])).await.unwrap();
        ctx.producer.flush().await.unwrap();
    }
    let mut consumer = Consumer::new(ctx.wal.clone(), ctx.store.clone(), None)
        .await
        .unwrap();
    let descriptors = consumer.next_descriptors(2).await.unwrap();
    let handle = consumer.fetch_handle();
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = Vec::new();
    for descriptor in descriptors {
        let handle = handle.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            handle.fetch(descriptor).await.unwrap()
        }));
    }

    // The owner remains usable while fetch tasks hold independent handles.
    assert_eq!(consumer.next_descriptors(1).await.unwrap()[0].sequence, 2);
    barrier.wait().await;
    for (sequence, (task, value)) in tasks.into_iter().zip(["first", "second"]).enumerate() {
        let batch = task.await.unwrap();
        assert_eq!(batch.sequence, sequence as u64);
        assert_eq!(batch.batches, vec![record_batch(&[value])]);
    }
    consumer.ack_through(1).await.unwrap();
    assert_eq!(ctx.wal.batches().await[0].sequence, 2);
    consumer.close().await.unwrap();
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_ack_prefix_without_resetting_sequence_after_queue_empties() {
    let ctx = TestContext::new(options());
    for value in ["first", "second", "third"] {
        ctx.producer.produce(record_batch(&[value])).await.unwrap();
        ctx.producer.flush().await.unwrap();
    }
    let mut consumer = Consumer::new(ctx.wal.clone(), ctx.store.clone(), None)
        .await
        .unwrap();
    consumer.next_descriptors(3).await.unwrap();

    consumer.ack_through(1).await.unwrap();
    assert_eq!(
        ctx.wal
            .batches()
            .await
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![2]
    );
    consumer.ack_through(2).await.unwrap();
    assert_eq!(ctx.wal.batches().await.len(), 0);
    assert_eq!(object_count(ctx.store.as_ref()).await, 3);

    ctx.producer
        .produce(record_batch(&["fourth"]))
        .await
        .unwrap();
    ctx.producer.flush().await.unwrap();
    assert_eq!(consumer.next_descriptors(1).await.unwrap()[0].sequence, 3);
    consumer.close().await.unwrap();

    let mut recovered = Consumer::new(ctx.wal.clone(), ctx.store.clone(), None)
        .await
        .unwrap();
    assert_eq!(recovered.next_descriptors(1).await.unwrap()[0].sequence, 3);
    recovered.close().await.unwrap();
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_resume_after_last_acked_sequence_and_reconcile_missed_ack() {
    let ctx = TestContext::new(options());
    for value in ["first", "second", "third"] {
        ctx.producer.produce(record_batch(&[value])).await.unwrap();
        ctx.producer.flush().await.unwrap();
    }

    let mut consumer = Consumer::new(ctx.wal.clone(), ctx.store.clone(), Some(1))
        .await
        .unwrap();

    assert_eq!(
        ctx.wal
            .batches()
            .await
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(consumer.next_descriptors(10).await.unwrap()[0].sequence, 2);
    consumer.close().await.unwrap();
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_not_reissue_lost_descriptors_until_restart_from_ack_frontier() {
    let ctx = TestContext::new(options());
    for value in ["first", "second", "third"] {
        ctx.producer.produce(record_batch(&[value])).await.unwrap();
        ctx.producer.flush().await.unwrap();
    }
    let mut consumer = Consumer::new(ctx.wal.clone(), ctx.store.clone(), None)
        .await
        .unwrap();
    consumer.next_descriptors(2).await.unwrap();
    consumer.ack_through(0).await.unwrap();
    assert_eq!(consumer.next_descriptors(10).await.unwrap()[0].sequence, 2);
    consumer.close().await.unwrap();

    let mut recovered = Consumer::new(ctx.wal.clone(), ctx.store.clone(), Some(0))
        .await
        .unwrap();
    let descriptors = recovered.next_descriptors(10).await.unwrap();
    assert_eq!(
        descriptors
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    recovered.close().await.unwrap();
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_reject_non_monotonic_ack_without_removing_more_entries() {
    let ctx = TestContext::new(options());
    for value in ["first", "second", "third"] {
        ctx.producer.produce(record_batch(&[value])).await.unwrap();
        ctx.producer.flush().await.unwrap();
    }
    let mut consumer = Consumer::new(ctx.wal.clone(), ctx.store.clone(), None)
        .await
        .unwrap();
    consumer.ack_through(1).await.unwrap();

    insta::assert_compact_debug_snapshot!(consumer.ack_through(1).await.unwrap_err(), @r#"InvalidInput("non-monotonic ack_through: last_acked=1, requested=1")"#);
    insta::assert_compact_debug_snapshot!(consumer.ack_through(0).await.unwrap_err(), @r#"InvalidInput("non-monotonic ack_through: last_acked=1, requested=0")"#);
    assert_eq!(
        ctx.wal
            .batches()
            .await
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![2]
    );
    consumer.ack_through(2).await.unwrap();
    assert_eq!(ctx.wal.batches().await.len(), 0);
    consumer.close().await.unwrap();
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_retry_failed_fetch_using_same_descriptor_without_reissuing_it() {
    let ctx = TestContext::new(options());
    ctx.producer
        .produce(record_batch(&["first"]))
        .await
        .unwrap();
    ctx.producer.flush().await.unwrap();
    let mut consumer = Consumer::new(ctx.wal.clone(), ctx.store.clone(), None)
        .await
        .unwrap();
    let descriptor = consumer.next_descriptors(1).await.unwrap().remove(0);
    let path = Path::from(descriptor.metadata.location.as_str());
    let original = ctx.store.get(&path).await.unwrap().bytes().await.unwrap();
    ctx.store.delete(&path).await.unwrap();
    let handle = consumer.fetch_handle();

    insta::assert_debug_snapshot!(matches!(handle.fetch(descriptor.clone()).await, Err(Error::Storage(_))), @"true");
    ctx.store
        .put(&path, b"invalid ipc".to_vec().into())
        .await
        .unwrap();
    insta::assert_debug_snapshot!(matches!(handle.fetch(descriptor.clone()).await, Err(Error::Serialization(_))), @"true");
    ctx.store
        .put(&path, original.slice(..original.len() - 20).into())
        .await
        .unwrap();
    insta::assert_debug_snapshot!(matches!(handle.fetch(descriptor.clone()).await, Err(Error::Serialization(_))), @"true");
    ctx.store.put(&path, original.into()).await.unwrap();

    let batch = handle.fetch(descriptor.clone()).await.unwrap();
    assert_eq!(batch.sequence, 0);
    assert_eq!(batch.metadata, descriptor.metadata);
    assert_eq!(batch.batches, vec![record_batch(&["first"])]);
    assert_eq!(
        handle.fetch(descriptor).await.unwrap().batches,
        batch.batches
    );
    insta::assert_debug_snapshot!(consumer.next_descriptors(1).await.unwrap(), @"[]");
    assert_eq!(ctx.wal.batches().await.len(), 1);
    consumer.close().await.unwrap();
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_retry_failed_reads_and_acks_without_advancing_frontiers() {
    let ctx = TestContext::new(options());
    ctx.producer
        .produce(record_batch(&["first"]))
        .await
        .unwrap();
    ctx.producer.flush().await.unwrap();
    let source = Arc::new(ControlledRead {
        wal: ctx.wal.clone(),
        ..Default::default()
    });
    source.fail_claim.store(true, Ordering::SeqCst);
    let result = Consumer::new(source.clone(), ctx.store.clone(), None).await;
    insta::assert_compact_debug_snapshot!(result.err().unwrap(), @r#"Commit("claim failed")"#);
    source.fail_ack.store(true, Ordering::SeqCst);
    let result = Consumer::new(source.clone(), ctx.store.clone(), Some(0)).await;
    insta::assert_compact_debug_snapshot!(result.err().unwrap(), @r#"Commit("ack failed")"#);
    assert_eq!(ctx.wal.batches().await.len(), 1);

    let mut consumer = Consumer::new(source.clone(), ctx.store.clone(), None)
        .await
        .unwrap();
    source.fail_read.store(true, Ordering::SeqCst);
    insta::assert_compact_debug_snapshot!(consumer.next_descriptors(1).await.unwrap_err(), @r#"Commit("read failed")"#);
    assert_eq!(consumer.next_descriptors(1).await.unwrap()[0].sequence, 0);

    source.fail_ack.store(true, Ordering::SeqCst);
    insta::assert_compact_debug_snapshot!(consumer.ack_through(0).await.unwrap_err(), @r#"Commit("ack failed")"#);
    assert_eq!(ctx.wal.batches().await.len(), 1);
    consumer.ack_through(0).await.unwrap();
    assert_eq!(ctx.wal.batches().await.len(), 0);
    consumer.close().await.unwrap();
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_not_skip_descriptors_when_discovery_is_cancelled() {
    let ctx = TestContext::new(options());
    ctx.producer
        .produce(record_batch(&["first"]))
        .await
        .unwrap();
    ctx.producer.flush().await.unwrap();
    let source = Arc::new(ControlledRead {
        wal: ctx.wal.clone(),
        ..Default::default()
    });
    let mut consumer = Consumer::new(source.clone(), ctx.store.clone(), None)
        .await
        .unwrap();
    source.block_read.store(true, Ordering::SeqCst);

    {
        let read = consumer.next_descriptors(1);
        tokio::pin!(read);
        tokio::select! {
            _ = source.entered.notified() => {},
            // PANIC: The controlled source blocks discovery until the future is dropped.
            result = &mut read => panic!("read unexpectedly completed: {result:?}"),
        }
    }

    assert_eq!(consumer.next_descriptors(1).await.unwrap()[0].sequence, 0);
    consumer.close().await.unwrap();
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_fence_previous_acks_without_fencing_reads_file_fetches_or_producers() {
    let ctx = TestContext::new(options());
    for value in ["first", "second"] {
        ctx.producer.produce(record_batch(&[value])).await.unwrap();
        ctx.producer.flush().await.unwrap();
    }
    let mut previous = Consumer::new(ctx.wal.clone(), ctx.store.clone(), None)
        .await
        .unwrap();
    let descriptor = previous.next_descriptors(1).await.unwrap().remove(0);
    let handle = previous.fetch_handle();
    previous.ack_through(0).await.unwrap();

    let mut current = Consumer::new(ctx.wal.clone(), ctx.store.clone(), Some(0))
        .await
        .unwrap();

    assert_eq!(previous.next_descriptors(1).await.unwrap()[0].sequence, 1);
    insta::assert_compact_debug_snapshot!(previous.ack_through(1).await.unwrap_err(), @"Fenced");
    insta::assert_compact_debug_snapshot!(previous.ack_through(1).await.unwrap_err(), @"Fenced");
    assert_eq!(ctx.wal.batches().await.len(), 1);
    assert_eq!(current.next_descriptors(1).await.unwrap()[0].sequence, 1);
    current.ack_through(1).await.unwrap();
    assert_eq!(ctx.wal.batches().await.len(), 0);

    let batch = handle.fetch(descriptor).await.unwrap();
    assert_eq!(batch.sequence, 0);
    assert_eq!(batch.batches, vec![record_batch(&["first"])]);

    ctx.producer
        .produce(record_batch(&["third"]))
        .await
        .unwrap();
    ctx.producer.flush().await.unwrap();
    assert_eq!(current.next_descriptors(1).await.unwrap()[0].sequence, 2);

    let replacement = Consumer::new(ctx.wal.clone(), ctx.store.clone(), Some(1))
        .await
        .unwrap();
    insta::assert_debug_snapshot!(current.next_descriptors(1).await.unwrap(), @"[]");
    insta::assert_compact_debug_snapshot!(current.ack_through(2).await.unwrap_err(), @"Fenced");
    previous.close().await.unwrap();
    current.close().await.unwrap();
    replacement.close().await.unwrap();
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_reject_unclaimed_or_stale_epochs_even_when_wal_is_empty() {
    let wal = InMemoryWal::default();
    insta::assert_debug_snapshot!(wal.read_after(None, 1).await.unwrap(), @"[]");
    insta::assert_compact_debug_snapshot!(wal.ack_through(0, 0).await.unwrap_err(), @"Fenced");

    let first = wal.claim_reader().await.unwrap();
    let second = wal.clone().claim_reader().await.unwrap();
    assert_eq!((first, second), (1, 2));
    insta::assert_compact_debug_snapshot!(wal.ack_through(first, 0).await.unwrap_err(), @"Fenced");
    insta::assert_debug_snapshot!(wal.read_after(None, 1).await.unwrap(), @"[]");
    insta::assert_compact_debug_snapshot!(wal.ack_through(second, 0).await.unwrap_err(), @r#"InvalidInput("cannot acknowledge sequence 0: next WAL sequence is 0")"#);
}

#[tokio::test]
async fn should_reject_acks_past_committed_sequences_without_removing_batches() {
    let ctx = TestContext::new(options());
    ctx.producer
        .produce(record_batch(&["retained"]))
        .await
        .unwrap();
    ctx.producer.flush().await.unwrap();
    let epoch = ctx.wal.claim_reader().await.unwrap();
    let retained = ctx.wal.batches().await;

    insta::assert_compact_debug_snapshot!(ctx.wal.ack_through(epoch, 1).await.unwrap_err(), @r#"InvalidInput("cannot acknowledge sequence 1: next WAL sequence is 1")"#);
    insta::assert_debug_snapshot!(ctx.wal.ack_through(epoch, u64::MAX).await.is_err(), @"true");
    assert_eq!(ctx.wal.batches().await, retained);
    ctx.wal.ack_through(epoch, 0).await.unwrap();
    ctx.wal.ack_through(epoch, 0).await.unwrap();
    insta::assert_debug_snapshot!(ctx.wal.ack_through(epoch, 1).await.is_err(), @"true");
    insta::assert_debug_snapshot!(ctx.wal.batches().await, @"[]");
    ctx.producer.close().await.unwrap();
}

#[tokio::test]
async fn should_allow_in_flight_reads_but_fence_acks_after_takeover() {
    for ack in [false, true] {
        let ctx = TestContext::new(options());
        ctx.producer
            .produce(record_batch(&["first"]))
            .await
            .unwrap();
        ctx.producer.flush().await.unwrap();
        let source = Arc::new(ControlledRead {
            wal: ctx.wal.clone(),
            ..Default::default()
        });
        let mut previous = Consumer::new(source.clone(), ctx.store.clone(), None)
            .await
            .unwrap();
        if ack {
            previous.next_descriptors(1).await.unwrap();
            source.block_ack.store(true, Ordering::SeqCst);
        } else {
            source.block_read.store(true, Ordering::SeqCst);
        }

        let mut current = {
            let operation = async {
                if ack {
                    previous.ack_through(0).await.map(|()| Vec::new())
                } else {
                    previous.next_descriptors(1).await
                }
            };
            tokio::pin!(operation);
            tokio::select! {
                _ = source.entered.notified() => {},
                // PANIC: The controlled source blocks the operation until takeover.
                result = &mut operation => panic!("operation unexpectedly completed: {result:?}"),
            }

            let current = Consumer::new(source.clone(), ctx.store.clone(), None)
                .await
                .unwrap();
            source.release.notify_one();
            let result = operation.await;
            if ack {
                insta::assert_compact_debug_snapshot!(result.unwrap_err(), @"Fenced");
            } else {
                assert_eq!(result.unwrap()[0].sequence, 0);
            }
            current
        };

        assert_eq!(ctx.wal.batches().await.len(), 1);
        let error = previous.ack_through(0).await.unwrap_err();
        insta::allow_duplicates! {
            insta::assert_compact_debug_snapshot!(error, @"Fenced");
        }
        assert_eq!(current.next_descriptors(1).await.unwrap()[0].sequence, 0);
        current.ack_through(0).await.unwrap();
        assert_eq!(ctx.wal.batches().await.len(), 0);
        previous.close().await.unwrap();
        current.close().await.unwrap();
        ctx.producer.close().await.unwrap();
    }
}

#[async_trait]
impl BatchRead for ControlledRead {
    async fn claim_reader(&self) -> Result<u64> {
        if self.fail_claim.swap(false, Ordering::SeqCst) {
            return Err(Error::Commit("claim failed".to_string()));
        }

        self.wal.claim_reader().await
    }

    async fn read_after(&self, sequence: Option<u64>, max: usize) -> Result<Vec<BatchDescriptor>> {
        if self.fail_read.swap(false, Ordering::SeqCst) {
            return Err(Error::Commit("read failed".to_string()));
        }
        if self.block_read.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }

        self.wal.read_after(sequence, max).await
    }

    async fn ack_through(&self, epoch: u64, sequence: u64) -> Result<()> {
        if self.fail_ack.swap(false, Ordering::SeqCst) {
            return Err(Error::Commit("ack failed".to_string()));
        }
        if self.block_ack.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }

        self.wal.ack_through(epoch, sequence).await
    }
}
