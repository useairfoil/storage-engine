use std::{collections::HashMap, sync::Arc};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use iceberg::{
    TableIdent,
    io::FileIO,
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
    rand::SeRand,
};
use se_materialization::run_materialization;
use se_metadata::WalRange;
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
        Arc::new(DefaultSystemClock::new()),
        Arc::new(SeRand::new(42)),
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
#[should_panic(expected = "not yet implemented: upsert materialization")]
async fn upserts_are_explicitly_unimplemented() {
    // PANIC: This fixture is valid; materialization must panic at the deliberate upsert TODO.
    let table = table(false, true, 536870912).unwrap();
    let _ = run_materialization(
        &table,
        Arc::new(InMemoryWal::default()),
        Arc::new(InMemory::new()),
        WalRange { start: 0, end: 1 },
        0,
        Heartbeat::new(|| async { Ok(()) }),
    )
    .await;
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
