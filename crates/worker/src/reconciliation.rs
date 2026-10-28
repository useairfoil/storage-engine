use std::collections::{BTreeMap, HashMap};

use iceberg::{
    TableRequirement,
    spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Schema, Struct,
        TableMetadata,
    },
    table::Table,
    transaction::Transaction,
};
use se_common::id::IdGenerator;
use se_iceberg::publication::prepare_upsert;
use se_metadata_client::{
    Heartbeat, PreparedCommit, RECONCILIATION_ID_PROPERTY, REVISION_PROPERTY, ReconciliationPlan,
    VersionedDataFile, VersionedDeletionVector,
};

use crate::{Error, Result};

const CONVERSION_BATCH_SIZE: usize = 128;

/// Prepares file changes against the planned base without submitting or rebasing them.
pub async fn prepare_reconciliation(
    table: &Table,
    plan: &ReconciliationPlan,
    files: &[VersionedDataFile],
    added_vectors: &[VersionedDeletionVector],
    removed_vectors: &[VersionedDeletionVector],
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

    let properties = HashMap::from([
        (RECONCILIATION_ID_PROPERTY.into(), plan.id.to_string()),
        (REVISION_PROPERTY.into(), plan.target_revision.to_string()),
    ]);
    // Isolate artifacts from concurrent/expired preparations of the same logical plan.
    let artifact_id = rand::rng().gen_uuid();
    let (updates, mut requirements) = if added_vectors.is_empty() && removed_vectors.is_empty() {
        let mut commit = Transaction::new(table)
            .fast_append()
            .set_commit_uuid(artifact_id)
            .set_snapshot_properties(properties)
            .add_data_files(data_files)
            .prepare(table)
            .await?;
        (commit.take_updates(), commit.take_requirements())
    } else {
        let mut vectors = [Vec::new(), Vec::new()];
        for (index, changes) in [added_vectors, removed_vectors].into_iter().enumerate() {
            for chunk in changes.chunks(CONVERSION_BATCH_SIZE) {
                for vector in chunk {
                    let valid = if index == 0 {
                        vector.added_revision > plan.base_revision
                            && vector.added_revision <= plan.target_revision
                            && vector
                                .removed_revision
                                .is_none_or(|revision| revision > plan.target_revision)
                    } else {
                        vector.added_revision <= plan.base_revision
                            && vector.removed_revision.is_some_and(|revision| {
                                revision > plan.base_revision && revision <= plan.target_revision
                            })
                    };
                    if !valid {
                        return Err(Error::InvalidReconciliation(
                            "vector is not a change in the planned range".into(),
                        ));
                    }
                    vectors[index].push(convert_deletion_vector(metadata, vector)?);
                }
                heartbeat.beat().await.map_err(Error::Heartbeat)?;
            }
        }
        let [added, removed] = vectors;
        let commit = prepare_upsert(table, artifact_id, data_files, added, removed, properties)
            .await
            .map_err(|error| Error::InvalidReconciliation(error.to_string()))?;
        (
            commit.updates,
            vec![
                TableRequirement::UuidMatch {
                    uuid: metadata.uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: "main".into(),
                    snapshot_id: plan.base_snapshot_id,
                },
            ],
        )
    };
    heartbeat.beat().await.map_err(Error::Heartbeat)?;

    requirements.extend([
        TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: metadata.current_schema_id(),
        },
        TableRequirement::DefaultSpecIdMatch {
            default_spec_id: metadata.default_partition_spec_id(),
        },
    ]);
    Ok(PreparedCommit {
        updates,
        requirements,
    })
}

fn convert_deletion_vector(
    metadata: &TableMetadata,
    vector: &VersionedDeletionVector,
) -> Result<DataFile> {
    let vector = &vector.metadata;
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(vector.puffin_path.clone())
        .file_format(DataFileFormat::Puffin)
        .partition(Struct::empty())
        .partition_spec_id(metadata.default_partition_spec_id())
        .record_count(vector.cardinality)
        .file_size_in_bytes(vector.file_size)
        .referenced_data_file(Some(vector.target_file.clone()))
        .content_offset(Some(i64::try_from(vector.offset).map_err(|_| {
            Error::InvalidReconciliation("vector offset exceeds i64".into())
        })?))
        .content_size_in_bytes(Some(i64::try_from(vector.length).map_err(|_| {
            Error::InvalidReconciliation("vector length exceeds i64".into())
        })?))
        .build()
        .map_err(|error| Error::InvalidReconciliation(error.to_string()))
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
