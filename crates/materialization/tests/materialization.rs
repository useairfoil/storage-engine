use std::{collections::HashMap, sync::Arc};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use iceberg::{
    TableIdent,
    io::FileIO,
    puffin::{CompressionCodec, PuffinReader},
    spec::{
        FormatVersion, NestedField, NullOrder, PrimitiveType, Schema, SortDirection, SortField,
        SortOrder, TableMetadataBuilder, Transform, Type, UnboundPartitionSpec,
    },
    table::Table,
};
use object_store::memory::InMemory;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use se_common::{
    clock::{DefaultSystemClock, SystemClock},
    id::IdGenerator,
    rand::SeRand,
};
use se_materialization::{TableIndex, run_materialization, run_upsert_materialization};
use se_metadata::{CatalogId, IndexDelta, MaterializationResult, TaskToken, WalRange};
use se_metadata_client::{Error as MetadataError, Heartbeat};
use se_wal::{BatchCommit, BatchRead, InMemoryWal, Producer, ProducerOptions};

#[tokio::test]
async fn writes_only_assigned_rows_with_sorting_rollover_and_iceberg_metrics()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let wal = Arc::new(InMemoryWal::default());
    // Deliberately omit Parquet field IDs from the producer's schema.
    let source = Arc::new(ArrowSchema::new(vec![Field::new(
        "key",
        DataType::Int64,
        true,
    )]));
    let producer = Producer::new(
        ProducerOptions::default(),
        store.clone(),
        wal.clone(),
        source.clone(),
        0,
        Arc::new(SeRand::new(42)),
        Arc::new(DefaultSystemClock::new()),
    )?;
    let mut input = (0_i64..16385).rev().map(Some).collect::<Vec<_>>();
    input.push(None);
    for values in [
        vec![Some(-99)],
        input[..8193].to_vec(),
        input[8193..].to_vec(),
        vec![Some(99999)],
    ] {
        producer
            .produce(RecordBatch::try_new(
                source.clone(),
                vec![Arc::new(Int64Array::from(values))],
            )?)
            .await?;
        producer.flush().await?;
    }
    producer.close().await?;
    let before = wal.batches().await;
    let epoch = wal.claim_reader().await?;
    for sorted in [false, true] {
        for target_size in [1, 536870912] {
            let table = table(sorted, false, target_size)?;
            let range = WalRange { start: 1, end: 3 };
            let result = run_materialization(
                &table,
                wal.clone(),
                store.clone(),
                range,
                0,
                Heartbeat::new(|| async { Ok(()) }),
            )
            .await?;
            assert_eq!(result.range, range);
            assert_eq!(result.schema_id, 0);
            insta::allow_duplicates! {
                insta::assert_debug_snapshot!(result.deletion_vectors, @"[]");
            }
            assert_eq!(result.data_files.len() > 1, target_size == 1);
            let mut recovered = Vec::new();
            for file in result.data_files {
                let bytes = table.file_io().new_input(&file.path)?.read().await?;
                assert_eq!(file.size, bytes.len() as u64);
                assert_eq!(file.partition_spec_id, 0);
                assert_eq!(file.sort_order_id, Some(i64::from(sorted)));
                let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
                assert_eq!(
                    reader
                        .parquet_schema()
                        .column(0)
                        .self_type()
                        .get_basic_info()
                        .id(),
                    1
                );
                let mut values = Vec::new();
                for batch in reader.build()? {
                    let batch = batch?;
                    let keys = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or("wrong column type")?;
                    values.extend(keys.iter());
                }
                assert_eq!(file.num_rows, values.len() as u64);
                assert_eq!(file.value_counts.get(&1), Some(&file.num_rows));
                assert_eq!(
                    file.null_value_counts.get(&1),
                    Some(&(values.iter().filter(|v| v.is_none()).count() as u64))
                );
                assert_eq!(
                    file.lower_bounds.get(&1),
                    values
                        .iter()
                        .flatten()
                        .min()
                        .map(|v| v.to_le_bytes().to_vec())
                        .as_ref()
                );
                assert_eq!(
                    file.upper_bounds.get(&1),
                    values
                        .iter()
                        .flatten()
                        .max()
                        .map(|v| v.to_le_bytes().to_vec())
                        .as_ref()
                );
                recovered.extend(values);
            }
            let mut expected = input.clone();
            if sorted {
                expected.sort();
            }
            assert_eq!(recovered, expected);
            insta::allow_duplicates! {
                insta::assert_debug_snapshot!(table.metadata().current_snapshot_id(), @"None");
            }
        }
    }
    let error = run_materialization(
        &table(false, false, 536870912)?,
        wal.clone(),
        store.clone(),
        WalRange { start: 1, end: 3 },
        0,
        Heartbeat::new(|| async { Err(MetadataError::TaskFenced) }),
    )
    .await
    .err()
    .ok_or("materialization ignored a fenced heartbeat")?;
    insta::assert_snapshot!(error, @"task heartbeat failed: task attempt is no longer active");
    assert_eq!(wal.batches().await, before);
    // A materializer must not claim ownership or fence an existing reader either.
    wal.ack_through(epoch, 0).await?;
    Ok(())
}

#[tokio::test]
async fn rejects_unavailable_ranges_and_mismatched_schemas()
-> Result<(), Box<dyn std::error::Error>> {
    let table = table(false, false, 536870912)?;
    let wal = Arc::new(InMemoryWal::default());
    let store = Arc::new(InMemory::new());
    for range in [WalRange { start: 2, end: 2 }, WalRange { start: 0, end: 1 }] {
        let error = run_materialization(
            &table,
            wal.clone(),
            store.clone(),
            range,
            0,
            Heartbeat::new(|| async { Ok(()) }),
        )
        .await
        .err()
        .ok_or("invalid range accepted")?;
        insta::allow_duplicates! {
            insta::assert_snapshot!(error, @"invalid or unavailable WAL range");
        }
    }
    let range = WalRange { start: 0, end: 1 };
    let error = run_materialization(
        &table,
        wal.clone(),
        store.clone(),
        range,
        99,
        Heartbeat::new(|| async { Ok(()) }),
    )
    .await
    .err()
    .ok_or("missing schema accepted")?;
    insta::assert_snapshot!(error, @"table schema missing: 99");
    wal.commit(se_wal::BatchMetadata {
        schema_id: 7,
        location: "does-not-exist".into(),
        timestamp: DefaultSystemClock::new().now(),
    })
    .await?;
    let error = run_materialization(
        &table,
        wal,
        store,
        range,
        0,
        Heartbeat::new(|| async { Ok(()) }),
    )
    .await
    .err()
    .ok_or("mismatched schema accepted")?;
    insta::assert_snapshot!(error, @"WAL batch 0 does not match the assigned schema");
    Ok(())
}

#[tokio::test]
async fn upserts_require_a_recovered_index() -> Result<(), Box<dyn std::error::Error>> {
    let error = run_materialization(
        &table(false, true, 536870912)?,
        Arc::new(InMemoryWal::default()),
        Arc::new(InMemory::new()),
        WalRange { start: 0, end: 1 },
        0,
        Heartbeat::new(|| async { Ok(()) }),
    )
    .await
    .err()
    .ok_or("upsert accepted without an index")?;
    insta::assert_snapshot!(error, @"upsert materialization requires a recovered table index");
    Ok(())
}

#[tokio::test]
async fn staged_upserts_recover_only_accepted_deltas_and_keep_cumulative_vectors()
-> Result<(), Box<dyn std::error::Error>> {
    let table = upsert_table(536870912)?;
    let store = Arc::new(InMemory::new());
    let wal = Arc::new(InMemoryWal::default());
    let clock = Arc::new(DefaultSystemClock::new());
    let source = Arc::new(ArrowSchema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("tenant", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let producer = Producer::new(
        ProducerOptions::default(),
        store.clone(),
        wal.clone(),
        source.clone(),
        0,
        Arc::new(SeRand::new(42)),
        clock.clone(),
    )?;
    for values in [
        vec![(2, "b", 20), (1, "a", 10), (2, "b", 21)],
        vec![(3, "c", 30), (1, "a", 11), (2, "x", 22)],
        vec![(3, "c", 31), (1, "a", 12), (1, "a", 13)],
        vec![(2, "b", 23), (3, "c", 32)],
        vec![(2, "x", 24)],
    ] {
        producer
            .produce(RecordBatch::try_new(
                source.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        values.iter().map(|r| r.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        values.iter().map(|r| r.1).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        values.iter().map(|r| r.2).collect::<Vec<_>>(),
                    )),
                ],
            )?)
            .await?;
        producer.flush().await?;
    }
    producer.close().await?;
    let heartbeat = Heartbeat::new(|| async { Ok(()) });
    let token = upsert_token(&table)?;
    let index = TableIndex::open("index", store.clone(), 1, clock.clone()).await?;
    let rejected = run_upsert_materialization(
        &table,
        wal.clone(),
        store.clone(),
        &token,
        WalRange { start: 0, end: 2 },
        0,
        &index,
        &heartbeat,
    )
    .await?;
    assert_eq!(index.applied_through().await?, 0);
    assert_eq!(
        run_upsert_materialization(
            &table,
            wal.clone(),
            store.clone(),
            &token,
            WalRange { start: 0, end: 2 },
            0,
            &index,
            &heartbeat
        )
        .await?,
        rejected
    );
    index.close().await?;

    // A prepared but unaccepted attempt must not influence the replacement attempt.
    let token = upsert_token(&table)?;
    let index = TableIndex::open("index", store.clone(), 2, clock.clone()).await?;
    let first = run_upsert_materialization(
        &table,
        wal.clone(),
        store.clone(),
        &token,
        WalRange { start: 0, end: 2 },
        0,
        &index,
        &heartbeat,
    )
    .await?;
    insta::assert_debug_snapshot!(first.deletion_vectors, @"[]");
    assert_eq!(first.data_files.iter().map(|f| f.num_rows).sum::<u64>(), 4);
    assert_eq!(
        physical_rows(&table, &first).await?,
        vec![
            (1, "a".into(), 11),
            (2, "b".into(), 21),
            (2, "x".into(), 22),
            (3, "c".into(), 30)
        ]
    );
    index.close().await?;

    // Metadata accepted `first`, but the process died before index application.
    let index = TableIndex::open("index", store.clone(), 3, clock.clone()).await?;
    index.catch_up(&[delta_ref(&first)?], &heartbeat).await?;
    index.catch_up(&[delta_ref(&first)?], &heartbeat).await?;
    assert_eq!(index.applied_through().await?, 2);
    let second = run_upsert_materialization(
        &table,
        wal.clone(),
        store.clone(),
        &upsert_token(&table)?,
        WalRange { start: 2, end: 3 },
        0,
        &index,
        &heartbeat,
    )
    .await?;
    assert_eq!(
        physical_rows(&table, &second).await?,
        vec![(1, "a".into(), 13), (3, "c".into(), 31)]
    );
    assert_eq!(second.deletion_vectors.len(), 1);
    assert_eq!(
        second.deletion_vectors[0].current.target_file,
        first.data_files[0].path
    );
    assert_eq!(
        vector_positions(&table, &second.deletion_vectors[0].current).await?,
        vec![0, 3]
    );
    index.close().await?;

    let index = TableIndex::open("index", store.clone(), 4, clock.clone()).await?;
    index.catch_up(&[delta_ref(&second)?], &heartbeat).await?;
    let third = run_upsert_materialization(
        &table,
        wal.clone(),
        store.clone(),
        &upsert_token(&table)?,
        WalRange { start: 3, end: 4 },
        0,
        &index,
        &heartbeat,
    )
    .await?;
    assert_eq!(third.deletion_vectors.len(), 2);
    let first_vector = third
        .deletion_vectors
        .iter()
        .find(|v| v.current.target_file == first.data_files[0].path)
        .ok_or("missing cumulative vector")?;
    assert_eq!(
        first_vector.previous.as_ref(),
        Some(&second.deletion_vectors[0].current)
    );
    assert_eq!(
        vector_positions(&table, &first_vector.current).await?,
        vec![0, 1, 3]
    );
    let second_vector = third
        .deletion_vectors
        .iter()
        .find(|v| v.current.target_file == second.data_files[0].path)
        .ok_or("missing second target")?;
    assert_eq!(
        vector_positions(&table, &second_vector.current).await?,
        vec![1]
    );
    index.apply(&delta_ref(&third)?).await?;
    let fourth = run_upsert_materialization(
        &table,
        wal.clone(),
        store.clone(),
        &upsert_token(&table)?,
        WalRange { start: 4, end: 5 },
        0,
        &index,
        &heartbeat,
    )
    .await?;
    assert_eq!(fourth.deletion_vectors.len(), 1);
    assert_eq!(
        fourth.deletion_vectors[0].previous.as_ref(),
        Some(&first_vector.current)
    );
    assert_eq!(
        vector_positions(&table, &fourth.deletion_vectors[0].current).await?,
        vec![0, 1, 2, 3]
    );
    index.apply(&delta_ref(&fourth)?).await?;
    index.apply(&delta_ref(&fourth)?).await?;
    assert_eq!(index.applied_through().await?, 5);
    assert_eq!(wal.batches().await.len(), 5);
    index.close().await?;
    Ok(())
}

#[tokio::test]
async fn upsert_locations_follow_sorted_rows_across_file_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let table = upsert_table(1)?;
    let store = Arc::new(InMemory::new());
    let wal = Arc::new(InMemoryWal::default());
    let clock = Arc::new(DefaultSystemClock::new());
    let source = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);
    let producer = Producer::new(
        ProducerOptions::default(),
        store.clone(),
        wal.clone(),
        source.clone(),
        0,
        Arc::new(SeRand::new(42)),
        clock.clone(),
    )?;
    for keys in [(0..8200).collect::<Vec<i64>>(), vec![8199, 8, 7, 0]] {
        producer
            .produce(RecordBatch::try_new(
                source.clone(),
                vec![
                    Arc::new(Int64Array::from(keys.clone())),
                    Arc::new(StringArray::from(vec!["tenant"; keys.len()])),
                    Arc::new(Int64Array::from(
                        keys.iter().map(|key| 8200 - key).collect::<Vec<_>>(),
                    )),
                ],
            )?)
            .await?;
        producer.flush().await?;
    }
    producer.close().await?;
    let heartbeat = Heartbeat::new(|| async { Ok(()) });
    let index = TableIndex::open("index", store.clone(), 1, clock).await?;
    let first = run_upsert_materialization(
        &table,
        wal.clone(),
        store.clone(),
        &upsert_token(&table)?,
        WalRange { start: 0, end: 1 },
        0,
        &index,
        &heartbeat,
    )
    .await?;
    assert_eq!(
        first
            .data_files
            .iter()
            .map(|file| file.num_rows)
            .collect::<Vec<_>>(),
        vec![8192, 8]
    );
    index.apply(&delta_ref(&first)?).await?;
    let second = run_upsert_materialization(
        &table,
        wal,
        store,
        &upsert_token(&table)?,
        WalRange { start: 1, end: 2 },
        0,
        &index,
        &heartbeat,
    )
    .await?;
    assert_eq!(second.deletion_vectors.len(), 2);
    for (file, expected) in first.data_files.iter().zip([vec![0, 8191], vec![0, 7]]) {
        let vector = second
            .deletion_vectors
            .iter()
            .find(|vector| vector.current.target_file == file.path)
            .ok_or("missing rollover target")?;
        assert_eq!(vector_positions(&table, &vector.current).await?, expected);
    }
    index.close().await?;
    Ok(())
}

#[tokio::test]
async fn newer_index_writers_fence_old_attempts_and_preserve_prepared_state()
-> Result<(), Box<dyn std::error::Error>> {
    let table = table(false, true, 536870912)?;
    let store = Arc::new(InMemory::new());
    let wal = Arc::new(InMemoryWal::default());
    let clock = Arc::new(DefaultSystemClock::new());
    let source = Arc::new(ArrowSchema::new(vec![Field::new(
        "key",
        DataType::Int64,
        false,
    )]));
    let producer = Producer::new(
        ProducerOptions::default(),
        store.clone(),
        wal.clone(),
        source.clone(),
        0,
        Arc::new(SeRand::new(42)),
        clock.clone(),
    )?;
    producer
        .produce(RecordBatch::try_new(
            source,
            vec![Arc::new(Int64Array::from(vec![7, 9]))],
        )?)
        .await?;
    producer.close().await?;
    let heartbeat = Heartbeat::new(|| async { Ok(()) });
    let old = TableIndex::open("index", store.clone(), 1, clock.clone()).await?;
    let prepared = run_upsert_materialization(
        &table,
        wal,
        store.clone(),
        &upsert_token(&table)?,
        WalRange { start: 0, end: 1 },
        0,
        &old,
        &heartbeat,
    )
    .await?;
    let newer = TableIndex::open("index", store.clone(), 2, clock.clone()).await?;
    let error = old
        .apply(&delta_ref(&prepared)?)
        .await
        .err()
        .ok_or("expired writer applied a delta")?;
    match error {
        se_materialization::Error::SlateDb(_) | se_materialization::Error::IndexFenced => (),
        error => return Err(error.into()),
    }
    let _ = old.close().await;
    newer.catch_up(&[delta_ref(&prepared)?], &heartbeat).await?;
    assert_eq!(newer.applied_through().await?, 1);
    newer.close().await?;
    let error = TableIndex::open("index", store.clone(), 1, clock.clone())
        .await
        .err()
        .ok_or("older epoch reopened index")?;
    insta::assert_snapshot!(error,@"table index writer epoch is no longer current");
    let recovered = TableIndex::open("index", store, 3, clock).await?;
    assert_eq!(recovered.applied_through().await?, 1);
    recovered.close().await?;
    Ok(())
}

fn upsert_token(table: &Table) -> Result<TaskToken, Box<dyn std::error::Error>> {
    Ok(TaskToken {
        worker_id: rand::rng().gen_uuid(),
        catalog_id: CatalogId::new("test")?,
        table_uuid: table.metadata().uuid(),
        table_ident: table.identifier().clone(),
        attempt_id: rand::rng().gen_uuid(),
    })
}

fn delta_ref(result: &MaterializationResult) -> Result<IndexDelta, Box<dyn std::error::Error>> {
    Ok(IndexDelta {
        id: result.index_delta.ok_or("missing delta ID")?,
        range: result.range,
    })
}

async fn physical_rows(
    table: &Table,
    result: &MaterializationResult,
) -> Result<Vec<(i64, String, i64)>, Box<dyn std::error::Error>> {
    let mut rows = Vec::new();
    for file in &result.data_files {
        for batch in ParquetRecordBatchReaderBuilder::try_new(
            table.file_io().new_input(&file.path)?.read().await?,
        )?
        .build()?
        {
            let batch = batch?;
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("key type")?;
            let tenants = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("tenant type")?;
            let values = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("value type")?;
            for row in 0..batch.num_rows() {
                rows.push((
                    keys.value(row),
                    tenants.value(row).into(),
                    values.value(row),
                ));
            }
        }
    }
    Ok(rows)
}

async fn vector_positions(
    table: &Table,
    vector: &se_metadata::DeletionVectorMetadata,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let input = table.file_io().new_input(&vector.puffin_path)?;
    assert_eq!(input.metadata().await?.size, vector.file_size);
    let reader = PuffinReader::new(input);
    let metadata = &reader.file_metadata().await?.blobs()[0];
    assert_eq!(
        (metadata.offset(), metadata.length()),
        (vector.offset, vector.length)
    );
    assert_eq!(metadata.compression_codec(), CompressionCodec::None);
    let blob = reader.blob(metadata).await?;
    assert_eq!(
        blob.properties().get("referenced-data-file"),
        Some(&vector.target_file)
    );
    assert_eq!(
        blob.properties().get("cardinality"),
        Some(&vector.cardinality.to_string())
    );
    assert_eq!((blob.snapshot_id(), blob.sequence_number()), (-1, -1));
    let bytes = blob.data();
    assert_eq!(
        u32::from_be_bytes(bytes[..4].try_into()?) as usize,
        bytes.len() - 8
    );
    assert_eq!(&bytes[4..8], &[0xd1, 0xd3, 0x39, 0x64]);
    assert_eq!(
        u32::from_be_bytes(bytes[bytes.len() - 4..].try_into()?),
        crc32fast::hash(&bytes[4..bytes.len() - 4])
    );
    let bitmap = roaring::RoaringTreemap::deserialize_from(&bytes[8..bytes.len() - 4])?;
    assert_eq!(bitmap.len(), vector.cardinality);
    Ok(bitmap.iter().collect())
}

fn upsert_table(target_size: u64) -> iceberg::Result<Table> {
    let schema = Schema::builder()
        .with_fields([
            NestedField::required(1, "key", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "tenant", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .with_identifier_field_ids([1, 2])
        .build()?;
    let metadata = TableMetadataBuilder::new(
        schema,
        UnboundPartitionSpec::default(),
        SortOrder {
            order_id: 1,
            fields: vec![SortField {
                source_id: 3,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            }],
        },
        "memory://upsert".into(),
        FormatVersion::V3,
        HashMap::from([(
            "write.target-file-size-bytes".into(),
            target_size.to_string(),
        )]),
    )?
    .build()?
    .metadata;
    Table::builder()
        .runtime(iceberg::Runtime::try_current()?)
        .identifier(TableIdent::from_strs(["test", "upsert"])?)
        .metadata(metadata)
        .file_io(FileIO::new_with_memory())
        .build()
}

fn table(sorted: bool, upsert: bool, target_size: u64) -> iceberg::Result<Table> {
    let schema = Schema::builder()
        .with_fields([if upsert {
            NestedField::required(17, "key", Type::Primitive(PrimitiveType::Long)).into()
        } else {
            NestedField::optional(17, "key", Type::Primitive(PrimitiveType::Long)).into()
        }])
        .with_identifier_field_ids(if upsert { vec![17] } else { vec![] })
        .build()?;
    let order = if sorted {
        SortOrder {
            order_id: 1,
            fields: vec![SortField {
                source_id: 17,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            }],
        }
    } else {
        SortOrder::unsorted_order()
    };
    let metadata = TableMetadataBuilder::new(
        schema,
        UnboundPartitionSpec::default(),
        order,
        "memory://table".into(),
        FormatVersion::V3,
        HashMap::from([(
            "write.target-file-size-bytes".into(),
            target_size.to_string(),
        )]),
    )?
    .build()?
    .metadata;
    Table::builder()
        .runtime(iceberg::Runtime::try_current()?)
        .identifier(TableIdent::from_strs(["test", "table"])?)
        .metadata(metadata)
        .file_io(FileIO::new_with_memory())
        .build()
}
