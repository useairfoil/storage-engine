use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use datafusion::arrow::{
    array::{RecordBatch, UInt64Array},
    compute::{concat_batches, take_record_batch},
    row::{RowConverter, SortField},
};
use iceberg::{
    arrow::{record_batch_projector::RecordBatchProjector, schema_to_arrow_schema},
    puffin::{Blob, CompressionCodec, DELETION_VECTOR_V1, PuffinWriter},
    spec::{FormatVersion, Schema},
    table::Table,
};
use object_store::ObjectStore;
use roaring::RoaringTreemap;
use se_common::id::IdGenerator;
use se_iceberg::schema::{SchemaExt, TableMode};
use se_metadata::{
    DeletionVectorMetadata, DeletionVectorReplacement, MaterializationResult, Task, TaskAssignment,
};
use se_metadata_client::Heartbeat;
use se_wal::BatchRead;

use crate::{
    Error, Result, TableIndex,
    append::write_data_files,
    index::{PreparedDelta, RowLocation, VectorState},
    read_batches,
};

/// Prepares last-write-wins rows and cumulative deletion vectors without advancing the index.
pub async fn run_upsert_materialization(
    table: &Table,
    wal: Arc<dyn BatchRead>,
    object_store: Arc<dyn ObjectStore>,
    assignment: &TaskAssignment,
    index: &TableIndex,
    heartbeat: &Heartbeat,
) -> Result<MaterializationResult> {
    let Task::Materialize {
        range, schema_id, ..
    } = assignment.task
    else {
        return Err(Error::InvalidWalRange);
    };
    let schema = table
        .metadata()
        .schema_by_id(schema_id)
        .ok_or(Error::SchemaMissing(schema_id))?;
    if schema.table_mode() != TableMode::Upsert || index.applied_through().await? != range.start {
        return Err(Error::InvalidIndexState);
    }
    if table.metadata().format_version() != FormatVersion::V3
        || (range.start == 0 && table.metadata().current_snapshot_id().is_some())
    {
        return Err(Error::UnsupportedUpsertTable);
    }
    if !table.metadata().default_partition_spec().is_unpartitioned() {
        return Err(Error::PartitionedTableUnsupported);
    }
    let mut ids = schema.identifier_field_ids().collect::<Vec<_>>();
    ids.sort_unstable();
    let identifiers = ids
        .iter()
        .map(|id| {
            let field = schema.field_by_id(*id).ok_or(Error::InvalidIndexState)?;
            Ok((*id, field.field_type.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    index.check_identifiers(&identifiers).await?;
    if let Some(result) = index.prepared(assignment.token.attempt_id).await? {
        if result.range != range || result.schema_id != schema_id {
            return Err(Error::InvalidIndexState);
        }
        heartbeat.beat().await?;
        return Ok(result);
    }

    let batches = read_batches(table, wal, object_store, range, schema_id, heartbeat).await?;
    let batch = concat_batches(&Arc::new(schema_to_arrow_schema(schema)?), &batches)?;
    let keys = row_keys(&batch, schema, &ids)?;
    let mut last = HashMap::new();
    for (row, key) in keys.into_iter().enumerate() {
        last.insert(key, row as u64);
    }
    let mut selected = last.into_values().collect::<Vec<_>>();
    selected.sort_unstable();
    let batch = take_record_batch(&batch, &UInt64Array::from(selected))?;
    heartbeat.beat().await?;
    let (data_files, ordered_batches) =
        write_data_files(vec![batch], schema, table, heartbeat).await?;

    let mut locations = data_files.iter().flat_map(|file| {
        (0..file.num_rows).map(|row| RowLocation {
            file: file.path.clone(),
            row,
        })
    });
    let mut rows = Vec::new();
    let mut deleted = BTreeMap::<String, RoaringTreemap>::new();
    for batch in ordered_batches {
        for key in row_keys(&batch, schema, &ids)? {
            if let Some(previous) = index.location(&key).await? {
                deleted
                    .entry(previous.file)
                    .or_default()
                    .insert(previous.row);
            }
            rows.push((key, locations.next().ok_or(Error::InvalidIndexState)?));
            if rows.len().is_multiple_of(128) {
                heartbeat.beat().await?;
            }
        }
    }
    if locations.next().is_some() {
        return Err(Error::InvalidIndexState);
    }
    let mut vectors = Vec::new();
    let mut deletion_vectors = Vec::new();
    for (target, mut positions) in deleted {
        let previous = index.vector(&target).await?;
        if let Some(previous) = &previous {
            positions |= RoaringTreemap::deserialize_from(previous.bitmap.as_slice())?;
        }
        let metadata = write_vector(table, &target, &positions).await?;
        deletion_vectors.push(DeletionVectorReplacement {
            previous: previous.map(|previous| previous.metadata),
            current: metadata.clone(),
        });
        let mut bitmap = Vec::new();
        positions.serialize_into(&mut bitmap)?;
        vectors.push(VectorState { metadata, bitmap });
        heartbeat.beat().await?;
    }
    let result = index
        .prepare(PreparedDelta {
            result: MaterializationResult {
                range,
                schema_id,
                data_files,
                deletion_vectors,
                index_delta: Some(assignment.token.attempt_id),
            },
            identifiers,
            rows,
            vectors,
        })
        .await?;
    heartbeat.beat().await?;
    Ok(result)
}

fn row_keys(batch: &RecordBatch, schema: &Schema, ids: &[i32]) -> Result<Vec<Vec<u8>>> {
    let columns = RecordBatchProjector::from_iceberg_schema(Arc::new(schema.clone()), ids)?
        .project_column(batch.columns())?;
    if columns.iter().any(|column| column.null_count() != 0) {
        return Err(Error::InvalidIndexState);
    }
    let converter = RowConverter::new(
        columns
            .iter()
            .map(|column| SortField::new(column.data_type().clone()))
            .collect(),
    )?;
    Ok(converter
        .convert_columns(&columns)?
        .iter()
        .map(|row| row.as_ref().to_vec())
        .collect())
}

async fn write_vector(
    table: &Table,
    target: &str,
    positions: &RoaringTreemap,
) -> Result<DeletionVectorMetadata> {
    let mut bitmap = Vec::new();
    bitmap.extend_from_slice(&[0xd1, 0xd3, 0x39, 0x64]);
    positions.serialize_into(&mut bitmap)?;
    let length = u32::try_from(bitmap.len()).map_err(|_| Error::InvalidIndexState)?;
    let checksum = crc32fast::hash(&bitmap);
    let mut data = length.to_be_bytes().to_vec();
    data.extend_from_slice(&bitmap);
    data.extend_from_slice(&checksum.to_be_bytes());
    let blob_length = data.len() as u64;
    let path = format!(
        "{}/data/{}.puffin",
        table.metadata().location().trim_end_matches('/'),
        rand::rng().gen_uuid()
    );
    let output = table.file_io().new_output(&path)?;
    let mut writer = PuffinWriter::new(&output, HashMap::new(), false).await?;
    writer
        .add(
            Blob::builder()
                .r#type(DELETION_VECTOR_V1.into())
                .fields(Vec::new())
                .snapshot_id(-1)
                .sequence_number(-1)
                .data(data)
                .properties(HashMap::from([
                    ("referenced-data-file".into(), target.into()),
                    ("cardinality".into(), positions.len().to_string()),
                ]))
                .build(),
            CompressionCodec::None,
        )
        .await?;
    writer.close().await?;
    Ok(DeletionVectorMetadata {
        puffin_path: path.clone(),
        target_file: target.into(),
        cardinality: positions.len(),
        file_size: table.file_io().new_input(&path)?.metadata().await?.size,
        offset: 4,
        length: blob_length,
    })
}
