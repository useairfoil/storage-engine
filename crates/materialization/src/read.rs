use std::sync::Arc;

use datafusion::{arrow::array::RecordBatch, physical_expr_adapter::BatchAdapterFactory};
use iceberg::{arrow::schema_to_arrow_schema, table::Table};
use object_store::ObjectStore;
use se_metadata::WalRange;
use se_metadata_client::Heartbeat;
use se_wal::{BatchRead, Consumer};

use crate::{Error, Result};

const READ_BATCHES: usize = 128;

/// Reads and adapts batches from the assigned WAL range without acknowledging them.
pub(crate) async fn read_batches(
    table: &Table,
    wal: Arc<dyn BatchRead>,
    object_store: Arc<dyn ObjectStore>,
    range: WalRange,
    schema_id: i32,
    heartbeat: &Heartbeat,
) -> Result<Vec<RecordBatch>> {
    if range.start >= range.end {
        return Err(Error::InvalidWalRange);
    }
    let schema = table
        .metadata()
        .schema_by_id(schema_id)
        .ok_or(Error::SchemaMissing(schema_id))?;
    let arrow_schema = Arc::new(schema_to_arrow_schema(schema)?);
    let adapter_factory = BatchAdapterFactory::new(arrow_schema.clone());
    let mut consumer = Consumer::new_read_only(wal, object_store, range.start.checked_sub(1));
    let fetch = consumer.fetch_handle();
    let mut next = range.start;
    let mut record_batches = Vec::new();
    let mut read_bytes = 0_u64;
    let mut read_rows = 0_u64;
    let span = tracing::Span::current();
    span.record("wal.read_bytes", read_bytes);
    span.record("wal.read_rows", read_rows);

    while next < range.end {
        let limit = (range.end - next).min(READ_BATCHES as u64) as usize;
        let descriptors = consumer.next_descriptors(limit).await?;
        if descriptors.is_empty() {
            return Err(Error::InvalidWalRange);
        }
        for descriptor in descriptors {
            if descriptor.sequence != next || descriptor.sequence >= range.end {
                return Err(Error::InvalidWalRange);
            }
            if descriptor.metadata.schema_id != schema_id {
                return Err(Error::BatchSchemaMismatch(descriptor.sequence));
            }
            for batch in fetch.fetch(descriptor).await?.batches {
                read_bytes += batch.get_array_memory_size() as u64;
                read_rows += batch.num_rows() as u64;
                span.record("wal.read_bytes", read_bytes);
                span.record("wal.read_rows", read_rows);

                let adapter = adapter_factory.make_adapter(&batch.schema())?;
                let adapted = adapter.adapt_batch(&batch)?;
                record_batches.push(RecordBatch::try_new(
                    arrow_schema.clone(),
                    adapted.columns().to_vec(),
                )?);
            }
            next += 1;
            heartbeat.beat().await?;
        }
    }

    Ok(record_batches)
}
