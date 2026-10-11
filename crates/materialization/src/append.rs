//! Writing append-table batches to a single sorted Parquet data file.

use std::sync::Arc;

use datafusion::arrow::{
    array::RecordBatch,
    compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take_record_batch},
    datatypes::SchemaRef,
};
use iceberg::{
    arrow::record_batch_projector::RecordBatchProjector,
    spec::{DataFileFormat, NullOrder, Schema, SortDirection, SortOrder},
    transform::create_transform_function,
};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::arrow::ArrowWriter;
use se_common::id::IdGenerator;
use se_meta_store::table::MaterializedDataFile;

use crate::Result;

/// Writes batches already adapted to the table schema, sorting them globally if needed.
/// Returns metadata captured while writing, with a table-relative path.
/// This does not register the file in an Iceberg snapshot.
pub async fn write_to_append_table(
    record_batches: Vec<RecordBatch>,
    table_schema: &Schema,
    arrow_schema: SchemaRef,
    sort_order: &SortOrder,
    table_object_store: &dyn ObjectStore,
) -> Result<MaterializedDataFile> {
    let record_count = record_batches
        .iter()
        .map(|batch| batch.num_rows() as u64)
        .sum();
    let mut writer = ArrowWriter::try_new(Vec::new(), arrow_schema.clone(), None)?;

    if sort_order.is_unsorted() {
        for batch in record_batches {
            writer.write(&batch)?;
        }
    } else {
        let batch = concat_batches(&arrow_schema, &record_batches)?;
        let source_ids = sort_order
            .fields
            .iter()
            .map(|field| field.source_id)
            .collect::<Vec<_>>();
        let projector =
            RecordBatchProjector::from_iceberg_schema(Arc::new(table_schema.clone()), &source_ids)?;
        let source_columns = projector.project_column(batch.columns())?;
        let sort_columns = sort_order
            .fields
            .iter()
            .zip(source_columns)
            .map(|(field, column)| {
                Ok(SortColumn {
                    values: create_transform_function(&field.transform)?.transform(column)?,
                    options: Some(SortOptions {
                        descending: field.direction == SortDirection::Descending,
                        nulls_first: field.null_order == NullOrder::First,
                    }),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let indices = lexsort_to_indices(&sort_columns, None)?;
        let batch = take_record_batch(&batch, &indices)?;
        writer.write(&batch)?;
    }

    let file_name = format!("data/{}.parquet", rand::rng().gen_uuid());
    let parquet = writer.into_inner()?;
    let file_size_in_bytes = parquet.len() as u64;
    table_object_store
        .put(&Path::from(file_name.clone()), parquet.into())
        .await?;

    Ok(MaterializedDataFile {
        file_path: file_name,
        file_format: DataFileFormat::Parquet,
        record_count,
        file_size_in_bytes,
        sort_order_id: sort_order.order_id,
    })
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::{
        array::{Int64Array, StructArray},
        datatypes::DataType,
    };
    use iceberg::{
        arrow::schema_to_arrow_schema,
        spec::{NestedField, PrimitiveType, SortField, StructType, Transform, Type},
    };
    use object_store::memory::InMemory;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn writes_batches_with_global_sort_and_field_ids() {
        let schema = Schema::builder()
            .with_fields([
                NestedField::optional(17, "key", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(29, "rank", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let source = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let batches = vec![
            RecordBatch::try_new(
                source.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![Some(25), None, Some(12), Some(19)])),
                    Arc::new(Int64Array::from(vec![Some(1), Some(7), None, Some(2)])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                source.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![Some(11), Some(22), None])),
                    Arc::new(Int64Array::from(vec![5, 9, 3])),
                ],
            )
            .unwrap(),
        ];
        let order = SortOrder {
            order_id: 1,
            fields: vec![
                SortField {
                    source_id: 17,
                    transform: Transform::Truncate(10),
                    direction: SortDirection::Ascending,
                    null_order: NullOrder::Last,
                },
                SortField {
                    source_id: 29,
                    transform: Transform::Identity,
                    direction: SortDirection::Descending,
                    null_order: NullOrder::First,
                },
            ],
        };
        let store = InMemory::new();
        let mut file_names = Vec::new();
        for sorted in [true, false] {
            let order = if sorted {
                order.clone()
            } else {
                SortOrder::unsorted_order()
            };
            let file =
                write_to_append_table(batches.clone(), &schema, source.clone(), &order, &store)
                    .await
                    .unwrap();
            assert_eq!(file.record_count, 7);
            assert_eq!(file.sort_order_id, order.order_id);
            assert_eq!(file.file_format, DataFileFormat::Parquet);
            let file_name = file.file_path;
            let uuid = file_name
                .strip_prefix("data/")
                .unwrap()
                .strip_suffix(".parquet")
                .unwrap();
            assert_eq!(Uuid::parse_str(uuid).unwrap().get_version_num(), 4);
            let bytes = store
                .get(&Path::from(file_name.clone()))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(file.file_size_in_bytes, bytes.len() as u64);
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
            let field_ids = reader
                .parquet_schema()
                .columns()
                .iter()
                .map(|column| column.self_type().get_basic_info().id())
                .collect::<Vec<_>>();
            assert_eq!(field_ids, vec![17, 29]);
            let batch = reader.build().unwrap().next().unwrap().unwrap();
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            let ranks = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            if sorted {
                assert_eq!(
                    keys,
                    vec![Some(12), Some(11), Some(19), Some(22), Some(25), None, None]
                );
                assert_eq!(
                    ranks,
                    vec![None, Some(5), Some(2), Some(9), Some(1), Some(7), Some(3)]
                );
            } else {
                assert_eq!(
                    keys,
                    vec![Some(25), None, Some(12), Some(19), Some(11), Some(22), None]
                );
                assert_eq!(
                    ranks,
                    vec![Some(1), Some(7), None, Some(2), Some(5), Some(9), Some(3)]
                );
            }
            file_names.push(file_name);
        }
        assert_ne!(file_names[0], file_names[1]);
    }

    #[tokio::test]
    async fn sorts_nested_fields_with_parent_nulls() {
        let schema = Schema::builder()
            .with_fields([
                NestedField::optional(
                    10,
                    "nested",
                    Type::Struct(StructType::new(vec![
                        NestedField::required(17, "key", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    ])),
                )
                .into(),
                NestedField::required(29, "row", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        // PANIC: The fixture's first field is a struct.
        let DataType::Struct(fields) = arrow_schema.field(0).data_type() else {
            unreachable!()
        };
        let nested = StructArray::new(
            fields.clone(),
            vec![Arc::new(Int64Array::from(vec![4, -99, 2]))],
            Some(vec![true, false, true].into()),
        );
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(nested), Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let order = SortOrder {
            order_id: 1,
            fields: vec![SortField {
                source_id: 17,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }],
        };
        let store = InMemory::new();
        let file = write_to_append_table(vec![batch], &schema, arrow_schema, &order, &store)
            .await
            .unwrap();
        let bytes = store
            .get(&file.file_path.into())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let batch = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let rows = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(rows.values().as_ref(), &[3, 1, 2]);
    }
}
