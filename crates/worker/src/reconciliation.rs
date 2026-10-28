use std::collections::{BTreeMap, HashMap};

use iceberg::{
    TableRequirement,
    spec::{DataContentType, DataFile, DataFileBuilder, Datum, Schema, Struct, TableMetadata},
    table::Table,
    transaction::Transaction,
};
use se_common::id::IdGenerator;
use se_metadata_client::{
    Heartbeat, PreparedCommit, RECONCILIATION_ID_PROPERTY, REVISION_PROPERTY, ReconciliationPlan,
    VersionedDataFile,
};

use crate::{Error, Result};

const CONVERSION_BATCH_SIZE: usize = 128;

/// Prepares an append against the planned base without submitting or rebasing it.
pub async fn prepare_reconciliation(
    table: &Table,
    plan: &ReconciliationPlan,
    files: &[VersionedDataFile],
    heartbeat: &Heartbeat,
) -> Result<PreparedCommit> {
    let metadata = table.metadata();
    if metadata.current_snapshot_id() != plan.base_snapshot_id {
        return Err(Error::InvalidReconciliation("base snapshot changed".into()));
    }
    if plan.target_revision <= plan.base_revision {
        return Err(Error::InvalidReconciliation("revision must advance".into()));
    }

    let mut data_files = Vec::with_capacity(files.len());
    for chunk in files.chunks(CONVERSION_BATCH_SIZE) {
        for file in chunk {
            if file.added_revision <= plan.base_revision
                || file.added_revision > plan.target_revision
                || file
                    .removed_revision
                    .is_some_and(|revision| revision <= plan.target_revision)
            {
                return Err(Error::InvalidReconciliation(
                    "file is not an append in the planned range".into(),
                ));
            }
            data_files.push(convert_data_file(metadata, file)?);
        }
        heartbeat.beat().await.map_err(Error::Heartbeat)?;
    }

    let mut commit = Transaction::new(table)
        .fast_append()
        // Isolate artifacts from concurrent/expired preparations of the same logical plan.
        .set_commit_uuid(rand::rng().gen_uuid())
        .set_snapshot_properties(HashMap::from([
            (RECONCILIATION_ID_PROPERTY.into(), plan.id.to_string()),
            (REVISION_PROPERTY.into(), plan.target_revision.to_string()),
        ]))
        .add_data_files(data_files)
        .prepare(table)
        .await?;
    heartbeat.beat().await.map_err(Error::Heartbeat)?;

    let mut requirements = commit.take_requirements();
    requirements.extend([
        TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: metadata.current_schema_id(),
        },
        TableRequirement::DefaultSpecIdMatch {
            default_spec_id: metadata.default_partition_spec_id(),
        },
    ]);
    Ok(PreparedCommit {
        updates: commit.take_updates(),
        requirements,
    })
}

fn convert_data_file(metadata: &TableMetadata, file: &VersionedDataFile) -> Result<DataFile> {
    let schema = metadata.schema_by_id(file.schema_id).ok_or_else(|| {
        Error::InvalidReconciliation(format!("schema {} not found", file.schema_id))
    })?;
    let file = &file.metadata;
    let spec = metadata
        .partition_spec_by_id(file.partition_spec_id)
        .ok_or_else(|| {
            Error::InvalidReconciliation(format!(
                "partition spec {} not found",
                file.partition_spec_id
            ))
        })?;
    // Fast append currently writes one manifest using the table's default spec.
    if file.partition_spec_id != metadata.default_partition_spec_id() {
        return Err(Error::InvalidReconciliation(
            "fast append requires the default partition spec".into(),
        ));
    }
    let partition_type = spec.partition_type(schema)?;
    if file.partition.len() != partition_type.fields().len() {
        return Err(Error::InvalidReconciliation(
            "partition fields do not match recorded spec".into(),
        ));
    }
    let partition = partition_type
        .fields()
        .iter()
        .map(|field| {
            let bytes = file.partition.get(&field.id).ok_or_else(|| {
                Error::InvalidReconciliation(format!("partition field {} missing", field.id))
            })?;
            bytes
                .as_ref()
                .map(|bytes| {
                    let primitive = field.field_type.as_primitive_type().ok_or_else(|| {
                        Error::InvalidReconciliation("partition type is not primitive".into())
                    })?;
                    Ok(Datum::try_from_bytes(bytes, primitive.clone())?.into())
                })
                .transpose()
        })
        .collect::<Result<Struct>>()?;

    let mut builder = DataFileBuilder::default();
    builder
        .content(DataContentType::Data)
        .file_path(file.path.clone())
        .file_format(file.format)
        .record_count(file.num_rows)
        .file_size_in_bytes(file.size)
        .partition_spec_id(file.partition_spec_id)
        .partition(partition)
        .column_sizes(
            file.column_sizes
                .iter()
                .map(|(&id, &value)| (id, value))
                .collect(),
        )
        .value_counts(
            file.value_counts
                .iter()
                .map(|(&id, &value)| (id, value))
                .collect(),
        )
        .null_value_counts(
            file.null_value_counts
                .iter()
                .map(|(&id, &value)| (id, value))
                .collect(),
        )
        .nan_value_counts(
            file.nan_value_counts
                .iter()
                .map(|(&id, &value)| (id, value))
                .collect(),
        )
        .lower_bounds(decode_bounds(schema, &file.lower_bounds)?)
        .upper_bounds(decode_bounds(schema, &file.upper_bounds)?);
    if let Some(id) = file.sort_order_id {
        builder.sort_order_id(
            i32::try_from(id)
                .map_err(|_| Error::InvalidReconciliation("sort order ID exceeds i32".into()))?,
        );
    }
    builder
        .build()
        .map_err(|error| Error::InvalidReconciliation(error.to_string()))
}

fn decode_bounds(schema: &Schema, bounds: &BTreeMap<i32, Vec<u8>>) -> Result<HashMap<i32, Datum>> {
    bounds
        .iter()
        .map(|(&id, bytes)| {
            let primitive = schema
                .field_by_id(id)
                .and_then(|field| field.field_type.as_primitive_type())
                .ok_or_else(|| {
                    Error::InvalidReconciliation(format!(
                        "bound field {id} is not primitive in recorded schema"
                    ))
                })?;
            Ok((id, Datum::try_from_bytes(bytes, primitive.clone())?))
        })
        .collect()
}
