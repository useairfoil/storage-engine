//! Writing append-table batches to size-targeted Parquet data files.

use std::sync::Arc;

use datafusion::arrow::{array::RecordBatch, compute::concat_batches};
use iceberg::{
    arrow::schema_to_arrow_schema,
    spec::{DataFileFormat, Schema},
    table::Table,
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
use se_metadata::DataFileMetadata;
use se_metadata_client::Heartbeat;

use crate::{Result, sort::sort_batch};

// Rollover happens between writes, including when sorting produces one large batch.
// This bounds rows per write, not the memory used to collect or sort the WAL range.
const WRITE_BATCH_ROWS: usize = 8192;

/// Writes canonical batches and returns file metadata and batches in their physical row order.
pub(crate) async fn write_data_files(
    record_batches: Vec<RecordBatch>,
    table_schema: &Schema,
    table: &Table,
    heartbeat: &Heartbeat,
) -> Result<(Vec<DataFileMetadata>, Vec<RecordBatch>)> {
    let metadata = table.metadata();
    let sort_order = metadata.default_sort_order();
    let table_properties = metadata.table_properties()?;
    let record_batches = if sort_order.is_unsorted() {
        record_batches
    } else {
        let sorted = sort_batch(
            concat_batches(
                &Arc::new(schema_to_arrow_schema(table_schema)?),
                &record_batches,
            )?,
            table_schema,
            sort_order,
        )?;
        heartbeat.beat().await?;
        vec![sorted]
    };
    let builder = DataFileWriterBuilder::new(RollingFileWriterBuilder::new(
        ParquetWriterBuilder::from_table_properties(
            &table_properties,
            Arc::new(table_schema.clone()),
        ),
        table_properties.write_target_file_size_bytes,
        table.file_io().clone(),
        DefaultLocationGenerator::with_data_location(format!(
            "{}/data",
            metadata.location().trim_end_matches('/')
        )),
        DefaultFileNameGenerator::new(
            rand::rng().gen_uuid().to_string(),
            None,
            DataFileFormat::Parquet,
        ),
    ));
    let mut writer = builder.build(None).await?;
    for batch in &record_batches {
        for offset in (0..batch.num_rows()).step_by(WRITE_BATCH_ROWS) {
            writer
                .write(batch.slice(offset, WRITE_BATCH_ROWS.min(batch.num_rows() - offset)))
                .await?;
            heartbeat.beat().await?;
        }
    }
    let files = writer.close().await?;
    heartbeat.beat().await?;
    let files = files
        .into_iter()
        .map(|file| {
            Ok(DataFileMetadata {
                path: file.file_path().to_owned(),
                format: file.file_format(),
                size: file.file_size_in_bytes(),
                num_rows: file.record_count(),
                partition_spec_id: metadata.default_partition_spec_id(),
                partition: Default::default(),
                sort_order_id: Some(sort_order.order_id),
                column_sizes: file
                    .column_sizes()
                    .iter()
                    .map(|(id, value)| (*id, *value))
                    .collect(),
                value_counts: file
                    .value_counts()
                    .iter()
                    .map(|(id, value)| (*id, *value))
                    .collect(),
                null_value_counts: file
                    .null_value_counts()
                    .iter()
                    .map(|(id, value)| (*id, *value))
                    .collect(),
                nan_value_counts: file
                    .nan_value_counts()
                    .iter()
                    .map(|(id, value)| (*id, *value))
                    .collect(),
                lower_bounds: file
                    .lower_bounds()
                    .iter()
                    .map(|(id, value)| Ok((*id, value.to_bytes()?.into_vec())))
                    .collect::<iceberg::Result<_>>()?,
                upper_bounds: file
                    .upper_bounds()
                    .iter()
                    .map(|(id, value)| Ok((*id, value.to_bytes()?.into_vec())))
                    .collect::<iceberg::Result<_>>()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((files, record_batches))
}
