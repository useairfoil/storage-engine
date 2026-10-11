//! Writing append-table batches to size-targeted Parquet data files.

use std::sync::Arc;

use datafusion::arrow::{
    array::RecordBatch,
    compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take_record_batch},
    datatypes::SchemaRef,
};
use iceberg::{
    arrow::record_batch_projector::RecordBatchProjector,
    io::FileIO,
    spec::{DataFileFormat, NullOrder, Schema, SortDirection, SortOrder, TableProperties},
    transform::create_transform_function,
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            rolling_writer::RollingFileWriterBuilder,
        },
    },
};
use se_common::id::IdGenerator;
use se_meta_store::table::MaterializedDataFile;

use crate::Result;

// Rollover happens between writes, including when sorting produces one large batch.
// This bounds rows per write, not the memory used to collect or sort the WAL range.
const WRITE_BATCH_ROWS: usize = 8192;

/// Writes batches already adapted to the table schema, sorting them globally if needed.
/// Returns metadata captured while writing, with a table-relative path.
/// This does not register the files in an Iceberg snapshot.
pub async fn write_to_append_table(
    mut record_batches: Vec<RecordBatch>,
    table_schema: &Schema,
    arrow_schema: SchemaRef,
    sort_order: &SortOrder,
    file_io: &FileIO,
    table_location: &str,
    table_properties: &TableProperties,
) -> Result<Vec<MaterializedDataFile>> {
    if !sort_order.is_unsorted() {
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
        record_batches = vec![batch];
    }

    let table_prefix = format!("{}/", table_location.trim_end_matches('/'));
    let builder = DataFileWriterBuilder::new(RollingFileWriterBuilder::new(
        ParquetWriterBuilder::from_table_properties(
            table_properties,
            Arc::new(table_schema.clone()),
        ),
        table_properties.write_target_file_size_bytes,
        file_io.clone(),
        DefaultLocationGenerator::with_data_location(format!("{table_prefix}data")),
        DefaultFileNameGenerator::new(
            rand::rng().gen_uuid().to_string(),
            None,
            DataFileFormat::Parquet,
        ),
    ));
    let mut writer = builder.build(None).await?;
    for batch in record_batches {
        for offset in (0..batch.num_rows()).step_by(WRITE_BATCH_ROWS) {
            writer
                .write(batch.slice(offset, WRITE_BATCH_ROWS.min(batch.num_rows() - offset)))
                .await?;
        }
    }
    Ok(writer
        .close()
        .await?
        .into_iter()
        .map(|file| MaterializedDataFile {
            // PANIC: The location generator above always writes under this table prefix.
            file_path: file.file_path()[table_prefix.len()..].to_owned(),
            file_format: file.file_format(),
            record_count: file.record_count(),
            file_size_in_bytes: file.file_size_in_bytes(),
            sort_order_id: sort_order.order_id,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::{
        array::{Int64Array, StructArray},
        datatypes::DataType,
    };
    use iceberg::{
        arrow::schema_to_arrow_schema,
        spec::{NestedField, PrimitiveType, SortField, StructType, Transform, Type},
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

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
        let file_io = FileIO::new_with_memory();
        let properties = TableProperties::try_from(&HashMap::new()).unwrap();
        let mut file_names = Vec::new();
        for sorted in [true, false] {
            let order = if sorted {
                order.clone()
            } else {
                SortOrder::unsorted_order()
            };
            let files = write_to_append_table(
                batches.clone(),
                &schema,
                source.clone(),
                &order,
                &file_io,
                "memory://table",
                &properties,
            )
            .await
            .unwrap();
            assert_eq!(files.len(), 1);
            let file = files.into_iter().next().unwrap();
            assert_eq!(file.record_count, 7);
            assert_eq!(file.sort_order_id, order.order_id);
            assert_eq!(file.file_format, DataFileFormat::Parquet);
            let file_name = file.file_path;
            let bytes = file_io
                .new_input(format!("memory://table/{file_name}"))
                .unwrap()
                .read()
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
        let file_io = FileIO::new_with_memory();
        let files = write_to_append_table(
            vec![batch],
            &schema,
            arrow_schema,
            &order,
            &file_io,
            "memory://table",
            &TableProperties::try_from(&HashMap::new()).unwrap(),
        )
        .await
        .unwrap();
        let bytes = file_io
            .new_input(format!("memory://table/{}", files[0].file_path))
            .unwrap()
            .read()
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
