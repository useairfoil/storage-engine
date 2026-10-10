use std::{collections::HashMap, sync::Arc};

use iceberg::{
    NamespaceIdent, TableIdent,
    io::{FileIOBuilder, MemoryStorageFactory},
    spec::{FormatVersion, Schema, SortOrder, TableMetadataBuilder, UnboundPartitionSpec},
    table::Table,
};
use object_store::{ObjectStoreExt, memory::InMemory};
use se_meta_store::table::{TableDb, TableRead, TableReader, TableWrite};
use slatedb::{Db, IsolationLevel};
use uuid::Uuid;

#[tokio::test]
async fn table_uuid_transaction_and_snapshot() {
    let db = Db::open("table", Arc::new(InMemory::new())).await.unwrap();
    let first = Uuid::from_u128(0x12345678_9abc_def0_1234_56789abcdef0);
    let second = Uuid::from_u128(0xfedcba98_7654_3210_fedc_ba9876543210);

    insta::assert_debug_snapshot!(db.get_table_uuid().await.unwrap(), @"None");
    let tx = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    tx.put_table_uuid(first).unwrap();
    tx.put_current_schema_id(0x12345678).unwrap();
    tx.put_default_sort_order_id(-0x123456789).unwrap();
    assert_eq!(tx.get_current_schema_id().await.unwrap(), Some(0x12345678));
    assert_eq!(
        tx.get_default_sort_order_id().await.unwrap(),
        Some(-0x123456789)
    );
    assert_eq!(tx.get_table_uuid().await.unwrap(), Some(first));
    insta::assert_debug_snapshot!(db.get_table_uuid().await.unwrap(), @"None");
    tx.commit().await.unwrap();

    let snapshot = db.snapshot().await.unwrap();
    assert_eq!(snapshot.get_table_uuid().await.unwrap(), Some(first));
    let tx = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    tx.put_table_uuid(second).unwrap();
    tx.commit().await.unwrap();

    assert_eq!(db.get_table_uuid().await.unwrap(), Some(second));
    assert_eq!(db.get_current_schema_id().await.unwrap(), Some(0x12345678));
    assert_eq!(
        db.get_default_sort_order_id().await.unwrap(),
        Some(-0x123456789)
    );
    assert_eq!(snapshot.get_table_uuid().await.unwrap(), Some(first));
    db.close().await.unwrap();
}

#[tokio::test]
async fn load_or_init_validates_persisted_uuid() {
    let store = Arc::new(InMemory::new());
    assert!(
        TableReader::try_load("table".into(), store.clone())
            .await
            .unwrap()
            .is_none()
    );
    let uuid = Uuid::from_u128(7);
    let table = iceberg_table(uuid);
    let metadata = table.metadata().clone();
    assert_eq!(metadata.schemas_iter().len(), 2);
    assert_eq!(metadata.sort_orders_iter().len(), 2);
    let db = TableDb::load_or_init("table".into(), table, store.clone())
        .await
        .unwrap();
    let snapshot = db.snapshot().await.unwrap();
    let reader = TableReader::try_load("table".into(), store.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reader.get_table_uuid().await.unwrap(), uuid);
    assert_eq!(reader.get_current_schema_id().await.unwrap(), Some(0));
    assert_eq!(
        reader.get_schema(0).await.unwrap().as_ref(),
        Some(metadata.current_schema().as_ref())
    );
    let engine_store = db.engine_object_store();
    assert!(Arc::ptr_eq(&engine_store, &db.engine_object_store()));
    engine_store
        .put(&"ingest/probe".into(), "payload".into())
        .await
        .unwrap();
    assert_eq!(
        store
            .get(&format!("{uuid}/ingest/probe").into())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"payload"
    );
    let tx = db.begin_transaction().await.unwrap();
    assert_eq!(tx.get_table_uuid().await.unwrap(), Some(uuid));
    assert_eq!(tx.get_current_schema_id().await.unwrap(), Some(0));
    assert_eq!(tx.get_default_sort_order_id().await.unwrap(), Some(0));
    for schema in metadata.schemas_iter() {
        assert_eq!(
            tx.get_schema(schema.schema_id()).await.unwrap().as_ref(),
            Some(schema.as_ref())
        );
    }
    for order in metadata.sort_orders_iter() {
        assert_eq!(
            tx.get_sort_order(order.order_id).await.unwrap().as_ref(),
            Some(order.as_ref())
        );
    }
    tx.put_current_schema_id(23).unwrap();
    tx.put_default_sort_order_id(42).unwrap();
    tx.commit()
        .await
        .unwrap()
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    assert_eq!(snapshot.get_current_schema_id().await.unwrap(), Some(0));
    assert_eq!(
        snapshot.get_schema(0).await.unwrap().as_ref(),
        Some(metadata.current_schema().as_ref())
    );
    drop(snapshot);
    assert_eq!(
        reader
            .engine_object_store()
            .get(&"ingest/probe".into())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"payload"
    );
    reader.close().await.unwrap();
    drop(db);

    let db = TableDb::load_or_init("table".into(), iceberg_table(uuid), store.clone())
        .await
        .unwrap();
    let tx = db.begin_transaction().await.unwrap();
    assert_eq!(tx.get_current_schema_id().await.unwrap(), Some(23));
    assert_eq!(tx.get_default_sort_order_id().await.unwrap(), Some(42));
    tx.commit().await.unwrap();
    drop(db);

    let error = TableDb::load_or_init(
        "table".into(),
        iceberg_table(Uuid::from_u128(8)),
        store.clone(),
    )
    .await
    .err()
    .unwrap();
    insta::assert_snapshot!(error, @"table uuid mismatch: catalog 00000000-0000-0000-0000-000000000008, stored 00000000-0000-0000-0000-000000000007");

    let db = TableDb::try_load("table".into(), store)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        db.engine_object_store()
            .get(&"ingest/probe".into())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"payload"
    );
    let tx = db.begin_transaction().await.unwrap();
    assert_eq!(tx.get_table_uuid().await.unwrap(), Some(uuid));
    tx.commit().await.unwrap();
    assert_eq!(db.get_table_uuid().await.unwrap(), uuid);
}

#[tokio::test]
async fn schemas_and_sort_orders_are_stored_by_id() {
    let db = Db::open("records", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let schema: Schema = serde_json::from_str(r#"{"type":"struct","schema-id":1,"fields":[{"id":7,"name":"name","required":false,"type":"string"}]}"#).unwrap();
    let other = Schema::builder().with_schema_id(256).build().unwrap();
    let order: SortOrder = serde_json::from_str(r#"{"order-id":1,"fields":[{"source-id":7,"transform":"identity","direction":"desc","null-order":"nulls-last"}]}"#).unwrap();
    let tx = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    assert_eq!(tx.get_schema(1).await.unwrap(), None);
    assert_eq!(tx.get_sort_order(1).await.unwrap(), None);
    tx.put_schema(1, &schema).unwrap();
    tx.put_schema(256, &other).unwrap();
    tx.put_sort_order(1, &order).unwrap();
    tx.put_sort_order(256, &SortOrder::unsorted_order())
        .unwrap();
    assert_eq!(tx.get_schema(1).await.unwrap(), Some(schema.clone()));
    assert_eq!(db.get_schema(1).await.unwrap(), None);
    tx.commit().await.unwrap();
    assert_eq!(db.get_schema(1).await.unwrap(), Some(schema));
    assert_eq!(db.get_schema(256).await.unwrap(), Some(other));
    assert_eq!(db.get_sort_order(1).await.unwrap(), Some(order));
    assert_eq!(
        db.get_sort_order(256).await.unwrap(),
        Some(SortOrder::unsorted_order())
    );
    assert_eq!(db.get_schema(2).await.unwrap(), None);
    assert_eq!(db.get_sort_order(2).await.unwrap(), None);
    db.close().await.unwrap();
}

fn iceberg_table(uuid: Uuid) -> Table {
    let metadata = TableMetadataBuilder::new(
        serde_json::from_str(r#"{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"name","required":false,"type":"string"}]}"#).unwrap(),
        UnboundPartitionSpec::default(),
        SortOrder::unsorted_order(),
        "memory://warehouse/table".into(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .unwrap()
    .assign_uuid(uuid)
    .add_schema(Schema::builder().build().unwrap())
    .unwrap()
    .add_sort_order(serde_json::from_str(r#"{"order-id":1,"fields":[{"source-id":1,"transform":"identity","direction":"desc","null-order":"nulls-last"}]}"#).unwrap())
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    Table::builder()
        .runtime(iceberg::Runtime::try_current().unwrap())
        .identifier(TableIdent::new(
            NamespaceIdent::new("test".into()),
            "table".into(),
        ))
        .metadata(metadata)
        .file_io(FileIOBuilder::new(Arc::new(MemoryStorageFactory)).build())
        .build()
        .unwrap()
}
