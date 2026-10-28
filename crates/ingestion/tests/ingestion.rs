mod common;

use std::sync::Arc;

use common::{FixedMetadata, TestContext, batch, catalog, namespace};
use se_common::{clock::SystemClock, rand::SeRand};
use se_ingestion::Ingestor;
use se_metadata::CatalogId;
use se_wal::{Consumer, test_util::BlockedCommit};

#[tokio::test]
async fn should_load_current_schema_share_producers_and_commit_before_acknowledging() {
    let ctx = TestContext::default();
    let first = ctx
        .ingestor
        .for_table(catalog(), namespace(), "table".into())
        .await
        .unwrap();
    let second = ctx
        .ingestor
        .clone()
        .for_table(catalog(), namespace(), "table".into())
        .await
        .unwrap();
    assert_eq!(first.current_schema_id(), 1);
    assert_eq!(first.table_schema().field(0).name(), "value");

    let a = batch(&ctx.metadata, vec!["one", "two"]);
    let b = batch(&ctx.metadata, vec!["three"]);
    let mut first_write = Box::pin(first.write(a.clone()));
    let mut second_write = Box::pin(second.write(b.clone()));
    tokio::select! {
        biased;
        // PANIC: Writes must remain pending until their file is committed.
        result = &mut first_write => panic!("write acknowledged early: {result:?}"),
        _ = async {} => {},
    }
    tokio::select! {
        biased;
        // PANIC: Writes must remain pending until their file is committed.
        result = &mut second_write => panic!("write acknowledged early: {result:?}"),
        _ = async {} => {},
    }
    let wal = ctx.wal(catalog());
    assert_eq!(wal.batches().await.len(), 0);

    ctx.ingestor.close().await.unwrap();
    first_write.await.unwrap();
    second_write.await.unwrap();
    let entries = wal.batches().await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].metadata.schema_id, 1);
    assert_eq!(entries[0].metadata.timestamp, ctx.clock.now());
    assert_eq!(
        entries[0].metadata.location.rsplit_once('/').unwrap().0,
        format!("catalogs/catalog/tables/{}/wal", ctx.metadata.uuid())
    );
    let consumer = Consumer::new(Arc::new(wal), ctx.store.clone(), None)
        .await
        .unwrap();
    let consumed = consumer
        .fetch_handle()
        .fetch(entries[0].clone())
        .await
        .unwrap();
    assert_eq!(consumed.batches, vec![a, b]);

    insta::assert_compact_debug_snapshot!(first.write(batch(&ctx.metadata, vec!["late"])).await.unwrap_err(), @"Write");
    insta::assert_compact_debug_snapshot!(ctx.ingestor.for_table(catalog(), namespace(), "table".into()).await.unwrap_err(), @"Closed");
    ctx.ingestor.close().await.unwrap();
}

#[tokio::test]
async fn should_validate_inputs_and_keep_catalog_wals_separate() {
    let ctx = TestContext::default();
    insta::assert_compact_debug_snapshot!(ctx.ingestor.for_table(catalog(), namespace(), "missing".into()).await.unwrap_err(), @r#"TableNotFound(TableIdent { namespace: NamespaceIdent(["namespace"]), name: "missing" })"#);
    let table = ctx
        .ingestor
        .for_table(catalog(), namespace(), "table".into())
        .await
        .unwrap();
    insta::assert_compact_debug_snapshot!(table.write(batch(&ctx.metadata, vec![])).await.unwrap_err(), @"EmptyBatch");
    // Use a nonempty batch with the historical schema to distinguish schema rejection from empty input.
    let old_schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(ctx.metadata.schema_by_id(0).unwrap()).unwrap(),
    );
    let wrong = arrow_array::RecordBatch::try_new(
        old_schema,
        batch(&ctx.metadata, vec!["wrong"]).columns().to_vec(),
    )
    .unwrap();
    insta::assert_compact_debug_snapshot!(table.write(wrong).await.unwrap_err(), @"InvalidSchema");

    let other_catalog = CatalogId::new("other").unwrap();
    let other = ctx
        .ingestor
        .for_table(other_catalog.clone(), namespace(), "table".into())
        .await
        .unwrap();
    let mut write = Box::pin(other.write(batch(&ctx.metadata, vec!["other"])));
    tokio::select! {
        biased;
        // PANIC: Writes must remain pending until shutdown flushes the producer.
        result = &mut write => panic!("write acknowledged early: {result:?}"),
        _ = async {} => {},
    }
    ctx.ingestor.close().await.unwrap();
    write.await.unwrap();
    assert_eq!(ctx.wal(catalog()).batches().await.len(), 0);
    assert_eq!(ctx.wal(other_catalog).batches().await.len(), 1);
}

#[tokio::test]
async fn should_wait_for_commit_and_propagate_commit_failure() {
    let ctx = TestContext::default();
    let commit = Arc::new(BlockedCommit::new(Err(se_wal::Error::Commit(
        "rejected".into(),
    ))));
    let ingestor = Ingestor::new(
        Arc::new(FixedMetadata(ctx.metadata.clone())),
        commit.clone(),
        ctx.store.clone(),
        ctx.clock.clone(),
        Arc::new(SeRand::new(7)),
    );
    let table = ingestor
        .for_table(catalog(), namespace(), "table".into())
        .await
        .unwrap();
    let mut write = Box::pin(table.write(batch(&ctx.metadata, vec!["value"])));
    tokio::select! {
        biased;
        // PANIC: Writes must remain pending until their file is committed.
        result = &mut write => panic!("write acknowledged early: {result:?}"),
        _ = async {} => {},
    }
    let mut close = Box::pin(ingestor.close());
    tokio::select! {
        biased;
        // PANIC: Shutdown must wait for the batch commit to finish.
        result = &mut close => panic!("shutdown completed early: {result:?}"),
        _ = commit.entered.notified() => {},
    }
    tokio::select! {
        biased;
        // PANIC: Upload alone must not acknowledge a write while its commit is blocked.
        result = &mut write => panic!("write acknowledged early: {result:?}"),
        _ = async {} => {},
    }
    commit.release.add_permits(1);
    let (closed, written) = tokio::join!(close, write);
    insta::assert_compact_debug_snapshot!(closed.unwrap_err(), @r#"Wal(Commit("rejected"))"#);
    insta::assert_compact_debug_snapshot!(written.unwrap_err(), @"Flush");
    ctx.ingestor.close().await.unwrap();
}
