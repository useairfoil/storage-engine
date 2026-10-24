use std::collections::{BTreeMap, HashMap};

use arrow_array::{Int64Array, RecordBatch};
use iceberg::{
    CatalogBuilder, TableCreation,
    arrow::schema_to_arrow_schema,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    spec::{
        NestedField, NullOrder, PrimitiveType, Schema, SortDirection, SortField, SortOrder,
        Transform, Type,
    },
};
use object_store::memory::InMemory;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use roaring::RoaringTreemap;
use se_materialization::upsert::write_to_upsert_table;

use super::*;

#[tokio::test]
async fn publishes_cumulative_vectors_and_recovers_catalog_success() {
    let (mut committer, table, store) = fixture().await;
    // Duplicated key 3 spans incoming batches; last row wins before payload sorting.
    let first = materialize(
        &committer,
        &table,
        0,
        &[&[(3, 90), (1, 30)], &[(3, 10), (2, 20)]],
    )
    .await;
    assert_eq!(first.data_files[0].record_count, 3);
    assert!(first.deletion_vectors.is_empty());
    committer.run_commit().await.unwrap();
    let published = committer
        .catalog
        .load_table(&committer.table_ident)
        .await
        .unwrap();
    assert_eq!(
        published
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .operation,
        iceberg::spec::Operation::Append
    );
    assert_eq!(
        visible_rows(&published).await.0,
        vec![(1, 30), (2, 20), (3, 10)]
    );

    // Two materializations before publication delete two different rows of the first file.
    let second = materialize(&committer, &published, 1, &[&[(3, 100)]]).await;
    let third = materialize(&committer, &published, 2, &[&[(2, 200), (3, 101)]]).await;
    assert_eq!(second.deletion_vectors[0].current.cardinality, 1);
    let cumulative = third
        .deletion_vectors
        .iter()
        .find(|dv| dv.current.data_file_path == first.data_files[0].file_path)
        .unwrap();
    assert_eq!(cumulative.current.cardinality, 2);
    assert_eq!(
        cumulative.previous.as_ref(),
        Some(&second.deletion_vectors[0].current)
    );
    committer.run_commit().await.unwrap();
    let published = committer
        .catalog
        .load_table(&committer.table_ident)
        .await
        .unwrap();
    let (rows, vectors) = visible_rows(&published).await;
    assert_eq!(rows, vec![(1, 30), (2, 200), (3, 101)]);
    assert_eq!(vectors.len(), 2);
    assert!(vectors.values().all(|count| *count == 1));
    assert_eq!(
        published
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .operation,
        iceberg::spec::Operation::Overwrite
    );

    // Replace just one vector in a shared Puffin, retaining the unrelated blob.
    let fourth = materialize(&committer, &published, 3, &[&[(1, 300)]]).await;
    assert_eq!(fourth.deletion_vectors[0].current.cardinality, 3);
    let old_snapshot = published.clone();
    // Simulate catalog success followed by a crash before local acknowledgement.
    let token = rand::rng().gen_uuid();
    let intent = PublicationIntent {
        token,
        previous_sequence: Some(2),
        first_sequence: 3,
        last_sequence: 3,
        prepared: Some(
            prepare_publication(&published, token, &[fourth])
                .await
                .unwrap(),
        ),
    };
    let tx = committer.table.begin_transaction().await.unwrap();
    tx.start_publication(&intent).await.unwrap();
    tx.commit()
        .await
        .unwrap()
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    let committed = intent
        .prepared
        .as_ref()
        .unwrap()
        .submit(committer.catalog.as_ref(), &published)
        .await
        .unwrap();
    let snapshot_id = committed.metadata().current_snapshot_id();
    committer.table.clone().flush_and_close().await.unwrap();
    committer.table = TableDb::try_load("table".into(), store)
        .await
        .unwrap()
        .unwrap();
    // Reconciliation refreshes catalog metadata before recovering the durable intent.
    committer
        .table
        .refresh_metadata(committed.metadata())
        .await
        .unwrap();
    committer.run_commit().await.unwrap();
    committer.run_commit().await.unwrap();
    let published = committer
        .catalog
        .load_table(&committer.table_ident)
        .await
        .unwrap();
    assert_eq!(published.metadata().current_snapshot_id(), snapshot_id);
    assert_eq!(
        visible_rows(&published).await.0,
        vec![(1, 300), (2, 200), (3, 101)]
    );
    assert_eq!(
        visible_rows(&old_snapshot).await.0,
        vec![(1, 30), (2, 200), (3, 101)]
    );
    let snapshot = committer.table.snapshot().await.unwrap();
    assert_eq!(snapshot.get_published_sequence().await.unwrap(), Some(3));
    assert_eq!(
        snapshot.read_pending_commits(None, 100).await.unwrap(),
        vec![]
    );
    drop(snapshot);

    // An insert-only batch is still an append when older snapshots contain deletes.
    let fifth = materialize(&committer, &published, 4, &[&[(4, 400)]]).await;
    assert!(fifth.deletion_vectors.is_empty());
    committer.run_commit().await.unwrap();
    let published = committer
        .catalog
        .load_table(&committer.table_ident)
        .await
        .unwrap();
    assert_eq!(
        published
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .operation,
        iceberg::spec::Operation::Append
    );
    assert_eq!(
        visible_rows(&published).await.0,
        vec![(1, 300), (2, 200), (3, 101), (4, 400)]
    );
    committer.table.flush_and_close().await.unwrap();
}

#[tokio::test]
async fn indexes_physical_positions_across_rollover_and_aborts_unaccepted_work() {
    let (committer, mut table, _) = fixture().await;
    // Force rollover between the writer's 8192-row slices.
    let properties = HashMap::from([("write.target-file-size-bytes".to_owned(), "1".to_owned())]);
    let table_commit = iceberg::TableCommit::builder()
        .ident(table.identifier().clone())
        .requirements(vec![])
        .updates(vec![iceberg::TableUpdate::SetProperties {
            updates: properties,
        }])
        .build();
    table = committer.catalog.update_table(table_commit).await.unwrap();
    let input = (0..8193)
        .rev()
        .map(|key| (key, key * 2))
        .collect::<Vec<_>>();
    let first = materialize(&committer, &table, 0, &[&input]).await;
    assert_eq!(first.data_files.len(), 2);
    let second = materialize(&committer, &table, 1, &[&[(0, -5), (8192, -9)]]).await;
    assert_eq!(second.deletion_vectors.len(), 2);
    committer.run_commit().await.unwrap();
    let published = committer
        .catalog
        .load_table(&committer.table_ident)
        .await
        .unwrap();
    let (rows, _) = visible_rows(&published).await;
    let mut expected = (0..8193).map(|key| (key, key * 2)).collect::<Vec<_>>();
    expected[0].1 = -5;
    expected[8192].1 = -9;
    assert_eq!(rows, expected);

    let tx = committer.table.begin_transaction().await.unwrap();
    write_to_upsert_table(
        vec![batch(&published, &[(0, 999)])],
        published.metadata().current_schema(),
        Arc::new(schema_to_arrow_schema(published.metadata().current_schema()).unwrap()),
        published.metadata().default_sort_order(),
        published.file_io(),
        published.metadata().location(),
        &published.metadata().table_properties().unwrap(),
        &tx,
    )
    .await
    .unwrap();
    drop(tx); // Artifacts alone cannot advance either the index or its bitmap.
    materialize(&committer, &published, 2, &[&[(0, 888)]]).await;
    committer.run_commit().await.unwrap();
    expected[0].1 = 888;
    assert_eq!(
        visible_rows(
            &committer
                .catalog
                .load_table(&committer.table_ident)
                .await
                .unwrap()
        )
        .await
        .0,
        expected
    );
    committer.table.flush_and_close().await.unwrap();
}

async fn fixture() -> (TableCommitter, Table, Arc<InMemory>) {
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    "memory://warehouse".to_owned(),
                )]),
            )
            .await
            .unwrap(),
    );
    let namespace = NamespaceIdent::new("test".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Schema::builder()
        .with_fields([
            NestedField::required(17, "key", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(29, "value", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .with_identifier_field_ids([17])
        .build()
        .unwrap();
    let sort_order = SortOrder {
        order_id: 1,
        fields: vec![SortField {
            source_id: 29,
            transform: Transform::Identity,
            direction: SortDirection::Descending,
            null_order: NullOrder::Last,
        }],
    };
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("table".into())
                .schema(schema)
                .sort_order(sort_order)
                .build(),
        )
        .await
        .unwrap();
    let upgrade = iceberg::TableCommit::builder()
        .ident(table.identifier().clone())
        .requirements(vec![])
        .updates(vec![iceberg::TableUpdate::UpgradeFormatVersion {
            format_version: iceberg::spec::FormatVersion::V3,
        }])
        .build();
    let table = catalog.update_table(upgrade).await.unwrap();
    let store = Arc::new(InMemory::new());
    let db = TableDb::load_or_init("table".into(), table.clone(), store.clone())
        .await
        .unwrap();
    (
        TableCommitter {
            table: db,
            table_ident: table.identifier().clone(),
            catalog,
            file_io: table.file_io().clone(),
            commit_lock: Arc::new(Mutex::new(())),
        },
        table,
        store,
    )
}

fn batch(table: &Table, rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).unwrap()),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn materialize(
    committer: &TableCommitter,
    table: &Table,
    sequence: u64,
    rows: &[&[(i64, i64)]],
) -> PendingCommit {
    committer
        .table
        .refresh_metadata(table.metadata())
        .await
        .unwrap();
    let tx = committer.table.begin_transaction().await.unwrap();
    let (data_files, deletion_vectors) = write_to_upsert_table(
        rows.iter().map(|rows| batch(table, rows)).collect(),
        table.metadata().current_schema(),
        Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).unwrap()),
        table.metadata().default_sort_order(),
        table.file_io(),
        table.metadata().location(),
        &table.metadata().table_properties().unwrap(),
        &tx,
    )
    .await
    .unwrap();
    let commit = PendingCommit {
        first_sequence: sequence,
        last_sequence: sequence,
        schema_id: table.metadata().current_schema_id(),
        table_location: table.metadata().location().to_owned(),
        partition_spec_id: table.metadata().default_partition_spec_id(),
        data_files,
        deletion_vectors,
    };
    tx.push_pending_commit(&commit, sequence.checked_sub(1))
        .await
        .unwrap();
    tx.commit()
        .await
        .unwrap()
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    commit
}

/// Read the snapshot's manifests and apply its on-disk DV blobs, independently of SlateDB.
async fn visible_rows(table: &Table) -> (Vec<(i64, i64)>, BTreeMap<String, usize>) {
    let snapshot = table.metadata().current_snapshot().unwrap();
    let list = table.manifest_list_reader(snapshot).load().await.unwrap();
    let mut data = Vec::new();
    let mut deleted = HashMap::new();
    let mut counts = BTreeMap::new();
    for manifest in list.entries() {
        let manifest = manifest.load_manifest(table.file_io()).await.unwrap();
        for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            let file = &entry.data_file;
            if file.content_type() == DataContentType::Data {
                data.push(file.clone());
                continue;
            }
            let target = file.referenced_data_file().unwrap();
            *counts.entry(target.clone()).or_insert(0) += 1;
            let bytes = table
                .file_io()
                .new_input(file.file_path())
                .unwrap()
                .read()
                .await
                .unwrap();
            let start = file.content_offset().unwrap() as usize;
            let end = start + file.content_size_in_bytes().unwrap() as usize;
            let blob = &bytes[start..end];
            assert_eq!(
                u32::from_be_bytes(blob[..4].try_into().unwrap()) as usize,
                blob.len() - 8
            );
            assert_eq!(&blob[4..8], &[0xd1, 0xd3, 0x39, 0x64]);
            assert_eq!(
                u32::from_be_bytes(blob[blob.len() - 4..].try_into().unwrap()),
                crc32fast::hash(&blob[4..blob.len() - 4])
            );
            let bitmap = RoaringTreemap::deserialize_from(&blob[8..blob.len() - 4]).unwrap();
            assert_eq!(bitmap.len(), file.record_count());
            deleted.insert(target, bitmap);
        }
    }
    let mut rows = Vec::new();
    for file in data {
        let bytes = table
            .file_io()
            .new_input(file.file_path())
            .unwrap()
            .read()
            .await
            .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        let mut position = 0;
        for batch in reader {
            let batch = batch.unwrap();
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for index in 0..batch.num_rows() {
                if !deleted
                    .get(file.file_path())
                    .is_some_and(|bitmap| bitmap.contains(position))
                {
                    rows.push((keys.value(index), values.value(index)));
                }
                position += 1;
            }
        }
    }
    rows.sort_unstable();
    (rows, counts)
}
