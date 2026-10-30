use std::{collections::HashMap, sync::Arc};

use iceberg::{
    NamespaceIdent, TableIdent,
    io::{FileIOBuilder, MemoryStorageFactory},
    spec::{FormatVersion, Schema, SortOrder, TableMetadataBuilder, UnboundPartitionSpec},
    table::Table,
};
use object_store::{ObjectStoreExt, memory::InMemory};
use se_meta_store::table::{
    MaterializedDataFile, PendingCommit, PublicationIntent, TableDb, TableRead, TableReader,
    TableWrite,
};
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
async fn refreshes_metadata_atomically_and_preserves_historical_schemas() {
    let store = Arc::new(InMemory::new());
    let table = iceberg_table(Uuid::from_u128(7));
    let db = TableDb::load_or_init("refresh".into(), table.clone(), store.clone())
        .await
        .unwrap();
    let historical = Schema::builder().with_schema_id(37).build().unwrap();
    let tx = db.begin_transaction().await.unwrap();
    tx.put_schema(37, &historical).unwrap();
    tx.commit().await.unwrap();
    let before = db.snapshot().await.unwrap();
    let refreshed = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_current_schema(1)
        .unwrap()
        .set_location("memory://warehouse/updated".into())
        .set_properties(HashMap::from([
            ("write.target-file-size-bytes".into(), "12345".into()),
            ("unrelated.catalog.property".into(), "not persisted".into()),
        ]))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    db.refresh_metadata(&refreshed).await.unwrap();
    assert_eq!(before.get_current_schema_id().await.unwrap(), Some(0));
    assert_eq!(
        before.get_table_location().await.unwrap().as_deref(),
        Some("memory://warehouse/table")
    );
    let after = db.snapshot().await.unwrap();
    assert_eq!(after.get_current_schema_id().await.unwrap(), Some(1));
    assert_eq!(after.get_schema(37).await.unwrap(), Some(historical));
    assert_eq!(
        after.get_table_location().await.unwrap().as_deref(),
        Some("memory://warehouse/updated")
    );
    assert_eq!(
        after.get_schema(1).await.unwrap().as_ref(),
        refreshed.schema_by_id(1).map(AsRef::as_ref)
    );
    assert_eq!(
        after.get_format_version().await.unwrap(),
        Some(FormatVersion::V2)
    );
    assert_eq!(
        after.get_default_partition_spec_id().await.unwrap(),
        Some(0)
    );
    assert_eq!(
        after.get_partition_spec(0).await.unwrap().as_ref(),
        Some(refreshed.default_partition_spec().as_ref())
    );
    assert_eq!(
        after.get_table_properties().await.unwrap(),
        HashMap::from([("write.target-file-size-bytes".into(), "12345".into())])
    );

    let error = db
        .refresh_metadata(iceberg_table(Uuid::from_u128(8)).metadata())
        .await
        .unwrap_err();
    insta::assert_snapshot!(error, @"table uuid mismatch: catalog 00000000-0000-0000-0000-000000000008, stored 00000000-0000-0000-0000-000000000007");
    assert_eq!(
        db.snapshot()
            .await
            .unwrap()
            .get_table_location()
            .await
            .unwrap()
            .as_deref(),
        Some("memory://warehouse/updated")
    );
    drop((before, after));
    db.flush_and_close().await.unwrap();
    let db = TableDb::try_load("refresh".into(), store)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        db.snapshot()
            .await
            .unwrap()
            .get_table_properties()
            .await
            .unwrap(),
        HashMap::from([("write.target-file-size-bytes".into(), "12345".into())])
    );
    assert_eq!(
        db.snapshot()
            .await
            .unwrap()
            .get_current_schema_id()
            .await
            .unwrap(),
        Some(1)
    );
    db.flush_and_close().await.unwrap();
}

#[tokio::test]
async fn publication_view_preserves_snapshot_lineage_across_formats() {
    for version in [1, 2, 3] {
        let table = iceberg_table(Uuid::from_u128(7));
        let db = TableDb::load_or_init(
            "projection".into(),
            table.clone(),
            Arc::new(InMemory::new()),
        )
        .await
        .unwrap();
        let mut metadata = serde_json::to_value(table.metadata()).unwrap();
        metadata["format-version"] = version.into();
        metadata["current-schema-id"] = 1.into();
        metadata["current-snapshot-id"] = 52.into();
        metadata["last-sequence-number"] = 9.into();
        metadata["next-row-id"] = 27.into();
        metadata["snapshots"] = serde_json::json!([
            {
                "snapshot-id": 41, "sequence-number": 8, "timestamp-ms": 1000,
                "manifest-list": "memory://warehouse/parent.avro", "schema-id": 0,
                "summary": {"operation": "append", "airfoil.publication-token": "earlier"},
                "first-row-id": 0, "added-rows": 20
            },
            {
                "snapshot-id": 52, "parent-snapshot-id": 41, "sequence-number": 9,
                "timestamp-ms": 2000, "manifest-list": "memory://warehouse/current.avro",
                "schema-id": 1, "summary": {"operation": "append"},
                "first-row-id": 20, "added-rows": 7
            }
        ]);
        let metadata = serde_json::from_value(metadata).unwrap();
        db.refresh_metadata(&metadata).await.unwrap();
        let view = db
            .snapshot()
            .await
            .unwrap()
            .publication_metadata()
            .await
            .unwrap();
        assert_eq!(view.current_schema_id(), 1);
        assert_eq!(view.current_snapshot_id(), Some(52));
        let current = view.current_snapshot().unwrap();
        assert_eq!(current.parent_snapshot_id(), Some(41));
        assert_eq!(current.manifest_list(), "memory://warehouse/current.avro");
        assert_eq!(current.schema_id(), Some(1));
        assert_eq!(
            view.snapshot_by_id(41)
                .unwrap()
                .summary()
                .additional_properties["airfoil.publication-token"],
            "earlier"
        );
        if version > 1 {
            assert_eq!(view.last_sequence_number(), 9);
        }
        if version == 3 {
            assert_eq!(view.next_row_id(), 27);
            assert_eq!(current.row_range(), Some((20, 7)));
        }
        db.flush_and_close().await.unwrap();
    }
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

#[tokio::test]
async fn publication_queue_is_atomic_ordered_bounded_and_durable() {
    // Pending commits queued before column metrics were introduced remain readable.
    let legacy: PendingCommit = serde_json::from_str(
        r#"{
        "first_sequence": 0, "last_sequence": 0, "schema_id": 7,
        "table_location": "s3://bucket/warehouse/table", "partition_spec_id": 3,
        "data_files": [{"file_path": "data/legacy.parquet", "file_format": "parquet",
            "record_count": 13, "file_size_in_bytes": 9876, "sort_order_id": 2}]
    }"#,
    )
    .unwrap();
    let file = &legacy.data_files[0];
    assert!(
        file.column_sizes.is_empty()
            && file.value_counts.is_empty()
            && file.null_value_counts.is_empty()
            && file.nan_value_counts.is_empty()
            && file.lower_bounds.is_empty()
            && file.upper_bounds.is_empty()
    );

    let store = Arc::new(InMemory::new());
    let db = Db::open("queue", store.clone()).await.unwrap();
    let before = db.snapshot().await.unwrap();
    let mut commits = Vec::new();
    let mut checkpoint = None;
    for (first, last) in [(0, 255), (256, 256), (257, 65536)] {
        let commit = PendingCommit {
            first_sequence: first,
            last_sequence: last,
            schema_id: 7,
            table_location: "s3://bucket/warehouse/table".into(),
            partition_spec_id: 3,
            data_files: vec![MaterializedDataFile {
                file_path: format!("data/{last}.parquet"),
                file_format: iceberg::spec::DataFileFormat::Parquet,
                record_count: 13,
                file_size_in_bytes: 9876,
                sort_order_id: 2,
                column_sizes: HashMap::from([(17, 123), (29, 456)]),
                value_counts: HashMap::from([(17, 13), (29, 13)]),
                null_value_counts: HashMap::from([(17, 0), (29, 2)]),
                nan_value_counts: HashMap::from([(31, 3)]),
                lower_bounds: HashMap::from([
                    (17, iceberg::spec::Datum::long(-5)),
                    (29, iceberg::spec::Datum::string("alpha")),
                    (31, iceberg::spec::Datum::double(f64::NEG_INFINITY)),
                ]),
                upper_bounds: HashMap::from([
                    (17, iceberg::spec::Datum::long(42)),
                    (29, iceberg::spec::Datum::string("zebra")),
                    (31, iceberg::spec::Datum::double(f64::INFINITY)),
                ]),
            }],
            deletion_vectors: Vec::new(),
        };
        let tx = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        tx.push_pending_commit(&commit, checkpoint).await.unwrap();
        assert_eq!(tx.get_materialized_sequence().await.unwrap(), Some(last));
        assert_eq!(db.get_materialized_sequence().await.unwrap(), checkpoint);
        tx.commit()
            .await
            .unwrap()
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        checkpoint = Some(last);
        commits.push(commit);
    }
    assert_eq!(before.get_materialized_sequence().await.unwrap(), None);
    assert_eq!(before.read_pending_commits(None, 10).await.unwrap(), vec![]);
    assert_eq!(
        db.read_pending_commits(None, 2).await.unwrap(),
        commits[..2]
    );
    assert_eq!(
        db.read_pending_commits(Some(255), 10).await.unwrap(),
        commits[1..]
    );
    assert_eq!(
        db.read_pending_commits(Some(256), 1).await.unwrap(),
        commits[2..]
    );
    assert_eq!(
        db.read_pending_commits(Some(u64::MAX), 10).await.unwrap(),
        vec![]
    );
    assert_eq!(db.read_pending_commits(None, 0).await.unwrap(), vec![]);

    let tx = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    assert!(matches!(
        tx.push_pending_commit(&commits[0], None).await,
        Err(se_meta_store::table::Error::MaterializedCheckpointChanged { .. })
    ));
    let mut invalid = commits[2].clone();
    invalid.first_sequence = 65538;
    invalid.last_sequence = 65540;
    assert!(matches!(
        tx.push_pending_commit(&invalid, checkpoint).await,
        Err(se_meta_store::table::Error::InvalidMaterializedRange { .. })
    ));
    drop(tx);
    let mut aborted = invalid.clone();
    aborted.first_sequence = 65537;
    let tx = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    tx.push_pending_commit(&aborted, checkpoint).await.unwrap();
    drop(tx);
    assert_eq!(db.read_pending_commits(None, 10).await.unwrap(), commits);
    assert_eq!(db.get_materialized_sequence().await.unwrap(), checkpoint);
    drop(before);
    db.close().await.unwrap();
    let reopened = Db::open("queue", store.clone()).await.unwrap();
    assert_eq!(
        reopened.get_materialized_sequence().await.unwrap(),
        checkpoint
    );
    assert_eq!(
        reopened.read_pending_commits(None, 10).await.unwrap(),
        commits
    );
    let intent = PublicationIntent {
        token: Uuid::from_u128(1),
        previous_sequence: None,
        first_sequence: 0,
        last_sequence: 256,
        prepared: None,
    };
    let tx = reopened
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let mut invalid_intent = intent.clone();
    invalid_intent.last_sequence = 254;
    assert!(matches!(
        tx.start_publication(&invalid_intent).await,
        Err(se_meta_store::table::Error::InvalidPublicationRange)
    ));
    tx.start_publication(&intent).await.unwrap();
    tx.commit()
        .await
        .unwrap()
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    reopened.close().await.unwrap();
    let reopened = Db::open("queue", store).await.unwrap();
    assert_eq!(
        reopened.get_publication_intent().await.unwrap(),
        Some(intent.clone())
    );
    let before_publication = reopened.snapshot().await.unwrap();
    let tx = reopened
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    assert!(matches!(
        tx.start_publication(&intent).await,
        Err(se_meta_store::table::Error::PublicationStateChanged)
    ));
    tx.finish_publication(&intent).await.unwrap();
    assert_eq!(tx.get_published_sequence().await.unwrap(), Some(256));
    assert_eq!(
        tx.read_pending_commits(None, 10).await.unwrap(),
        commits[2..]
    );
    drop(tx);
    assert_eq!(reopened.get_published_sequence().await.unwrap(), None);
    assert_eq!(
        reopened.read_pending_commits(None, 10).await.unwrap(),
        commits
    );
    let tx = reopened
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    tx.finish_publication(&intent).await.unwrap();
    tx.commit()
        .await
        .unwrap()
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    assert_eq!(reopened.get_published_sequence().await.unwrap(), Some(256));
    assert_eq!(
        reopened.get_materialized_sequence().await.unwrap(),
        checkpoint
    );
    assert_eq!(reopened.get_publication_intent().await.unwrap(), None);
    assert_eq!(
        reopened.read_pending_commits(None, 10).await.unwrap(),
        commits[2..]
    );
    assert_eq!(
        before_publication
            .read_pending_commits(None, 10)
            .await
            .unwrap(),
        commits
    );
    let next_intent = PublicationIntent {
        token: Uuid::from_u128(2),
        previous_sequence: Some(256),
        first_sequence: 257,
        last_sequence: 65536,
        prepared: None,
    };
    let tx = reopened
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    tx.start_publication(&next_intent).await.unwrap();
    tx.commit()
        .await
        .unwrap()
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    let tx = reopened
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    assert!(matches!(
        tx.finish_publication(&intent).await,
        Err(se_meta_store::table::Error::PublicationStateChanged)
    ));
    assert!(matches!(
        tx.retire_publication(&intent).await,
        Err(se_meta_store::table::Error::PublicationStateChanged)
    ));
    tx.retire_publication(&next_intent).await.unwrap();
    assert_eq!(tx.get_publication_intent().await.unwrap(), None);
    assert_eq!(tx.get_published_sequence().await.unwrap(), Some(256));
    assert_eq!(
        tx.read_pending_commits(None, 10).await.unwrap(),
        commits[2..]
    );
    drop(tx);
    assert_eq!(
        reopened.get_publication_intent().await.unwrap(),
        Some(next_intent.clone())
    );
    let tx = reopened
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    tx.retire_publication(&next_intent).await.unwrap();
    tx.commit()
        .await
        .unwrap()
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    assert_eq!(reopened.get_publication_intent().await.unwrap(), None);
    assert_eq!(reopened.get_published_sequence().await.unwrap(), Some(256));
    assert_eq!(
        reopened.read_pending_commits(None, 10).await.unwrap(),
        commits[2..]
    );
    drop(before_publication);
    reopened.close().await.unwrap();
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
