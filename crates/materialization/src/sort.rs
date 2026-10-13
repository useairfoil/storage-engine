//! Sorting canonical batches using Iceberg transforms and ordering rules.

use std::sync::Arc;

use datafusion::arrow::{
    array::RecordBatch,
    compute::{SortColumn, SortOptions, lexsort_to_indices, take_record_batch},
};
use iceberg::{
    arrow::record_batch_projector::RecordBatchProjector,
    spec::{NullOrder, Schema, SortDirection, SortOrder},
    transform::create_transform_function,
};

use crate::Result;

pub(crate) fn sort_batch(
    batch: RecordBatch,
    table_schema: &Schema,
    sort_order: &SortOrder,
) -> Result<RecordBatch> {
    if sort_order.is_unsorted() {
        return Ok(batch);
    }
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
    Ok(take_record_batch(
        &batch,
        &lexsort_to_indices(&sort_columns, None)?,
    )?)
}
