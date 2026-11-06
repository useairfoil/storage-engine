use std::sync::Arc;

use datafusion::{arrow::array::RecordBatch, physical_expr_adapter::BatchAdapterFactory};
use futures::{StreamExt, stream};
use iceberg::{arrow::schema_to_arrow_schema, table::Table};
use object_store::ObjectStore;
use se_metadata::WalRange;
use se_metadata_client::Heartbeat;
use se_wal::{BatchRead, Consumer};
use tracing::{Instrument, Span, info_span, instrument};

use crate::{Error, Result};

const READ_BATCHES: usize = 128;
const FETCH_CONCURRENCY: usize = 32;

/// Reads and adapts batches from the assigned WAL range without acknowledging them.
#[instrument(skip_all, fields(wal.read_bytes, wal.read_rows, wal.descriptor_count))]
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
    let fetcher = consumer.fetch_handle();

    let mut next = range.start;

    let mut read_bytes = 0_u64;
    let mut read_rows = 0_u64;
    let mut descriptor_count = 0_u64;

    let mut record_batches = Vec::new();

    while next < range.end {
        let limit = (range.end - next).min(READ_BATCHES as u64) as usize;
        let descriptors = consumer.next_descriptors(limit).await?;

        descriptor_count += descriptors.len() as u64;

        if descriptors.is_empty() {
            return Err(Error::InvalidWalRange);
        }

        let start = next;

        let mut fetched = stream::iter(descriptors.into_iter().enumerate())
            .map(|(offset, descriptor)| {
                let f = fetcher.clone();
                async move {
                    if descriptor.sequence != start + offset as u64
                        || descriptor.sequence >= range.end
                    {
                        return Err(Error::InvalidWalRange);
                    }

                    if descriptor.metadata.schema_id != schema_id {
                        return Err(Error::BatchSchemaMismatch(descriptor.sequence));
                    }

                    let fetch_span = info_span!(
                        "fetch_batch",
                        wal.sequence = descriptor.sequence,
                        wal.schema_id = descriptor.metadata.schema_id,
                        wal.location = %descriptor.metadata.location,
                        wal.timestamp = %descriptor.metadata.timestamp,
                        wal.size_bytes = descriptor.metadata.wal_size_bytes,
                    );

                    Ok(f.fetch(descriptor).instrument(fetch_span).await?)
                }
            })
            .buffered(FETCH_CONCURRENCY);

        while let Some(batch) = fetched.next().await {
            let batch = batch?;
            read_bytes += batch.metadata.wal_size_bytes;

            for batch in batch.batches {
                read_rows += batch.num_rows() as u64;

                let adapter = adapter_factory.make_adapter(&batch.schema())?;
                let adapted = adapter.adapt_batch(&batch)?;

                // Reapply the schema to keep Iceberg field ids in metadata.
                record_batches.push(RecordBatch::try_new(
                    arrow_schema.clone(),
                    adapted.columns().to_vec(),
                )?);
            }

            next += 1;

            heartbeat.beat().await?;
        }
    }

    Span::current()
        .record("wal.read_bytes", read_bytes)
        .record("wal.read_rows", read_rows)
        .record("wal.descriptor_count", descriptor_count);

    Ok(record_batches)
}
