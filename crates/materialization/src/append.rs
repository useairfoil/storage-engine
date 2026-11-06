use std::sync::Arc;

use iceberg::table::Table;
use object_store::ObjectStore;
use se_iceberg::schema::{SchemaExt, TableMode};
use se_metadata::{MaterializationResult, WalRange};
use se_metadata_client::Heartbeat;
use se_wal::BatchRead;

use crate::{Error, Result, read::read_batches, write::write_data_files};

/// Writes an assigned WAL range to data files without committing or acknowledging it.
pub async fn run(
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

    let (data_files, _) = write_data_files(record_batches, schema, table, &heartbeat).await?;

    Ok(MaterializationResult {
        range,
        schema_id,
        data_files,
        deletion_vectors: Vec::new(),
        index_delta: None,
    })
}
