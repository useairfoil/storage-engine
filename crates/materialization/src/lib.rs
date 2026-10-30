//! Materialization of assigned WAL ranges without publication or WAL acknowledgment.

mod append;
mod index;
mod sort;
mod upsert;

use std::sync::Arc;

use datafusion::{arrow::array::RecordBatch, physical_expr_adapter::BatchAdapterFactory};
use iceberg::{arrow::schema_to_arrow_schema, table::Table};
pub use index::TableIndex;
use object_store::ObjectStore;
use se_iceberg::schema::{SchemaExt, TableMode};
use se_metadata::{MaterializationResult, WalRange};
use se_metadata_client::Heartbeat;
use se_wal::{BatchRead, Consumer};
pub use upsert::run_upsert_materialization;

const READ_BATCHES: usize = 128;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error("wal error: {0}")]
    Wal(#[from] se_wal::Error),

    #[error("task heartbeat failed: {0}")]
    Heartbeat(#[from] se_metadata_client::Error),

    #[error("table schema missing: {0}")]
    SchemaMissing(i32),

    #[error("invalid or unavailable WAL range")]
    InvalidWalRange,

    #[error("WAL batch {0} does not match the assigned schema")]
    BatchSchemaMismatch(u64),

    #[error("partitioned append materialization is not implemented")]
    PartitionedTableUnsupported,

    #[error("slatedb error: {0}")]
    SlateDb(#[from] slatedb::Error),

    #[error("index encoding error: {0}")]
    IndexEncoding(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("upsert materialization requires a recovered table index")]
    UpsertIndexRequired,

    #[error("table index writer epoch is no longer current")]
    IndexFenced,

    #[error("table index does not match the accepted materialization history")]
    InvalidIndexState,

    #[error("identifier fields or their types changed")]
    IdentifierSchemaChanged,

    #[error("upserts require Iceberg v3 and an initially empty table")]
    UnsupportedUpsertTable,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Writes an assigned WAL range to data files without committing or acknowledging it.
pub async fn run_materialization(
    table: &Table,
    wal: Arc<dyn BatchRead>,
    object_store: Arc<dyn ObjectStore>,
    range: WalRange,
    schema_id: i32,
    heartbeat: Heartbeat,
) -> Result<MaterializationResult> {
    if range.start >= range.end {
        return Err(Error::InvalidWalRange);
    }
    let schema = table
        .metadata()
        .schema_by_id(schema_id)
        .ok_or(Error::SchemaMissing(schema_id))?;
    if schema.table_mode() == TableMode::Upsert {
        return Err(Error::UpsertIndexRequired);
    }
    if !table.metadata().default_partition_spec().is_unpartitioned() {
        return Err(Error::PartitionedTableUnsupported);
    }

    let record_batches =
        read_batches(table, wal, object_store, range, schema_id, &heartbeat).await?;
    let (data_files, _) =
        append::write_data_files(record_batches, schema, table, &heartbeat).await?;
    Ok(MaterializationResult {
        range,
        schema_id,
        data_files,
        deletion_vectors: Vec::new(),
        index_delta: None,
    })
}

async fn read_batches(
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
