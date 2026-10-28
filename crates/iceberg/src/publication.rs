//! Durable append plans submitted once against an exact catalog base.
//!
//! Recovery assumes `main` and schemas move forward, not through rollback or ref
//! replacement. Restoring an old schema ID could re-enable a delayed submission.
//! Location/format changes must be coordinated with publishers: the REST protocol
//! has no requirements to guard those values atomically.

use std::collections::{HashMap, HashSet};

use iceberg::{
    Catalog, TableCommit, TableRequirement, TableUpdate,
    spec::{
        DataContentType, DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType,
        ManifestListWriter, ManifestWriterBuilder, Operation, Snapshot, SnapshotReference,
        SnapshotRetention, Summary,
    },
    table::Table,
    transaction::Transaction,
};
use se_common::clock::{DefaultSystemClock, SystemClock};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const PUBLICATION_TOKEN: &str = "se.publication-token";

/// Immutable append metadata. Retrying must reuse these updates, not rewrite manifests.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedAppend {
    pub table_uuid: Uuid,
    pub base_snapshot_id: Option<i64>,
    pub schema_id: i32,
    pub partition_spec_id: i32,
    pub location: String,
    pub format_version: FormatVersion,
    pub updates: Vec<TableUpdate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    Published,
    Retry,
    Superseded,
    Unknown,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),
    #[error("prepared append is incompatible with the current table: {0}")]
    IncompatibleTable(&'static str),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Plans once using the SDK's manifest writer, with no catalog reload or rebase.
pub async fn prepare_append(
    table: &Table,
    token: Uuid,
    files: Vec<DataFile>,
) -> Result<PreparedAppend> {
    if table
        .metadata()
        .table_properties()?
        .encryption_key_id
        .is_some()
    {
        return Err(Error::IncompatibleTable(
            "encrypted writes are not supported",
        ));
    }
    let mut action = Transaction::new(table)
        .fast_append()
        .set_commit_uuid(token)
        .set_snapshot_properties(HashMap::from([(
            PUBLICATION_TOKEN.into(),
            token.to_string(),
        )]))
        .add_data_files(files)
        .prepare(table)
        .await?;
    let metadata = table.metadata();
    Ok(PreparedAppend {
        table_uuid: metadata.uuid(),
        base_snapshot_id: metadata.current_snapshot_id(),
        schema_id: metadata.current_schema_id(),
        partition_spec_id: metadata.default_partition_spec_id(),
        location: metadata.location().into(),
        format_version: metadata.format_version(),
        updates: action.take_updates(),
    })
}

/// Atomically adds data and replaces cumulative v3 vectors against an exact base.
/// Removed files identify individual blobs by Puffin path and offset.
pub async fn prepare_upsert(
    table: &Table,
    token: Uuid,
    data_files: Vec<DataFile>,
    added_vectors: Vec<DataFile>,
    removed_vectors: Vec<DataFile>,
    mut snapshot_properties: HashMap<String, String>,
) -> Result<PreparedAppend> {
    let metadata = table.metadata();
    if metadata.format_version() != FormatVersion::V3
        || !metadata.default_partition_spec().is_unpartitioned()
        || metadata.table_properties()?.encryption_key_id.is_some()
    {
        return Err(Error::IncompatibleTable(
            "upserts require unencrypted, unpartitioned v3 tables",
        ));
    }
    let snapshot_id = i64::from_be_bytes(
        token.as_bytes()[..8]
            .try_into()
            .map_err(|_| Error::IncompatibleTable("invalid publication token"))?,
    ) & i64::MAX;
    let sequence = metadata.next_sequence_number();
    let root = format!(
        "{}/metadata/{token}",
        metadata.location().trim_end_matches('/')
    );
    let mut manifests = Vec::new();
    let mut live_data = data_files
        .iter()
        .map(|file| file.file_path().to_owned())
        .collect::<HashSet<_>>();
    let mut removed = removed_vectors
        .iter()
        .map(|file| (file.file_path().to_owned(), file.content_offset()))
        .collect::<HashSet<_>>();
    let targets = added_vectors
        .iter()
        .filter_map(|file| file.referenced_data_file())
        .collect::<HashSet<_>>();
    let mut totals = [0u64; 5];
    if let Some(snapshot) = metadata.current_snapshot() {
        let list = table.manifest_list_reader(snapshot).load().await?;
        for manifest in list.entries() {
            let entries = manifest.load_manifest(table.file_io()).await?;
            let mut replace = false;
            for entry in entries.entries().iter().filter(|entry| entry.is_alive()) {
                let file = &entry.data_file;
                let deleting =
                    removed.contains(&(file.file_path().to_owned(), file.content_offset()));
                if deleting {
                    replace = true;
                    continue;
                }
                if file.content_type() == DataContentType::Data {
                    live_data.insert(file.file_path().to_owned());
                } else if file.file_format() != DataFileFormat::Puffin {
                    return Err(Error::IncompatibleTable(
                        "external delete files require index reconciliation",
                    ));
                } else if file
                    .referenced_data_file()
                    .is_some_and(|target| targets.contains(&target))
                {
                    return Err(Error::IncompatibleTable(
                        "existing vector differs from the materialized base",
                    ));
                }
                accumulate_file(&mut totals, file);
            }
            if !replace {
                manifests.push(manifest.clone());
                continue;
            }
            let mut writer = ManifestWriterBuilder::new(
                table
                    .file_io()
                    .new_output(format!("{root}-{}.avro", manifests.len()))?,
                Some(snapshot_id),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().as_ref().clone(),
            )
            .build_v3_deletes();
            if manifest.content != ManifestContentType::Deletes {
                return Err(Error::IncompatibleTable(
                    "vector removal referenced a data manifest",
                ));
            }
            for entry in entries.entries().iter().filter(|entry| entry.is_alive()) {
                let file = &entry.data_file;
                let data_sequence = entry
                    .sequence_number
                    .ok_or(Error::IncompatibleTable("missing data sequence"))?;
                if removed.remove(&(file.file_path().to_owned(), file.content_offset())) {
                    writer.add_delete_file(
                        file.clone(),
                        data_sequence,
                        entry.file_sequence_number,
                    )?;
                } else {
                    writer.add_existing_file(
                        file.clone(),
                        entry
                            .snapshot_id
                            .ok_or(Error::IncompatibleTable("missing snapshot ID"))?,
                        data_sequence,
                        entry.file_sequence_number,
                    )?;
                }
            }
            manifests.push(writer.write_manifest_file().await?);
        }
    }
    if !removed.is_empty() || targets.iter().any(|target| !live_data.contains(target)) {
        return Err(Error::IncompatibleTable(
            "upsert base files no longer exist",
        ));
    }
    for (files, deletes) in [(&data_files, false), (&added_vectors, true)] {
        if files.is_empty() {
            continue;
        }
        let builder = ManifestWriterBuilder::new(
            table
                .file_io()
                .new_output(format!("{root}-{}.avro", manifests.len()))?,
            Some(snapshot_id),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        );
        let mut writer = if deletes {
            builder.build_v3_deletes()
        } else {
            builder.build_v3_data()
        };
        for file in files {
            writer.add_file(file.clone(), sequence)?;
            accumulate_file(&mut totals, file);
        }
        manifests.push(writer.write_manifest_file().await?);
    }
    let manifest_list = format!("{root}-list.avro");
    let mut writer = ManifestListWriter::v3(
        table.file_io().new_output(&manifest_list)?.writer().await?,
        snapshot_id,
        metadata.current_snapshot_id(),
        sequence,
        Some(metadata.next_row_id()),
    );
    writer.add_manifests(manifests.into_iter())?;
    let assigned_rows = writer
        .next_row_id()
        .ok_or(Error::IncompatibleTable("missing v3 row IDs"))?
        - metadata.next_row_id();
    writer.close().await?;
    snapshot_properties.insert(PUBLICATION_TOKEN.to_owned(), token.to_string());
    for (key, value) in [
        "total-data-files",
        "total-delete-files",
        "total-records",
        "total-position-deletes",
        "total-files-size",
    ]
    .into_iter()
    .zip(totals)
    {
        snapshot_properties.insert(key.to_owned(), value.to_string());
    }
    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(metadata.current_snapshot_id())
        .with_sequence_number(sequence)
        .with_schema_id(metadata.current_schema_id())
        .with_manifest_list(manifest_list)
        .with_timestamp_ms(DefaultSystemClock::default().now().timestamp_millis())
        .with_row_range(metadata.next_row_id(), assigned_rows)
        .with_summary(Summary {
            operation: Operation::Overwrite,
            additional_properties: snapshot_properties,
        })
        .build();
    Ok(PreparedAppend {
        table_uuid: metadata.uuid(),
        base_snapshot_id: metadata.current_snapshot_id(),
        schema_id: metadata.current_schema_id(),
        partition_spec_id: metadata.default_partition_spec_id(),
        location: metadata.location().to_owned(),
        format_version: metadata.format_version(),
        updates: vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_owned(),
                reference: SnapshotReference::new(
                    snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ],
    })
}

/// Searches only current ancestry. A missing parent is inconclusive, not rejection.
pub fn publication_found(table: &Table, token: Uuid) -> bool {
    let (found, _) = ancestry(table, token, None);
    found
}

impl PreparedAppend {
    /// Checks why the append failed to apply.
    ///
    /// - If token is found in the snapshot ancestry, finish ack the apply.
    /// - Original base compatible, retry apply.
    /// - Token absent from complete ancestry and head or schema changed, replan.
    /// - History is not complete, then it's unknown.
    /// - Table identity changed, this is an error.
    pub fn resolve(&self, table: &Table, token: Uuid) -> Result<Resolution> {
        let metadata = table.metadata();
        if metadata.uuid() != self.table_uuid {
            return Err(Error::IncompatibleTable("table UUID changed"));
        }

        let (published, base_reached) = ancestry(table, token, self.base_snapshot_id);

        if published {
            return Ok(Resolution::Published);
        }

        if !base_reached {
            return Ok(Resolution::Unknown);
        }

        if metadata.default_partition_spec_id() != self.partition_spec_id
            || metadata.location() != self.location
            || metadata.format_version() != self.format_version
        {
            return Err(Error::IncompatibleTable(
                "spec, location, or format changed",
            ));
        }

        // The schema assertion fences the old plan even when main has not moved.
        // Replanning checks retained file schemas and the table's write mode.
        Ok(
            if metadata.current_snapshot_id() == self.base_snapshot_id
                && metadata.current_schema_id() == self.schema_id
            {
                Resolution::Retry
            } else {
                Resolution::Superseded
            },
        )
    }

    /// One guarded submission. No reload, automatic retry, or rebase.
    pub async fn submit(&self, catalog: &dyn Catalog, table: &Table) -> Result<Table> {
        let commit = TableCommit::builder()
            .ident(table.identifier().clone())
            .requirements(vec![
                TableRequirement::UuidMatch {
                    uuid: self.table_uuid,
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: "main".into(),
                    snapshot_id: self.base_snapshot_id,
                },
                TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: self.schema_id,
                },
                TableRequirement::DefaultSpecIdMatch {
                    default_spec_id: self.partition_spec_id,
                },
            ])
            .updates(self.updates.clone())
            .build();

        Ok(catalog.update_table(commit).await?)
    }
}

/// Walks the table's snapshots until one with the publication token is found.
///
/// Returns (published, base_reached).
fn ancestry(table: &Table, token: Uuid, base: Option<i64>) -> (bool, bool) {
    let metadata = table.metadata();
    let token = token.to_string();
    let mut id = metadata.current_snapshot_id();
    let mut seen = HashSet::new();

    while let Some(current_id) = id {
        if !seen.insert(current_id) {
            return (false, false);
        }

        let Some(snapshot) = metadata.snapshot_by_id(current_id) else {
            return (false, false);
        };

        if snapshot
            .summary()
            .additional_properties
            .get(PUBLICATION_TOKEN)
            == Some(&token)
        {
            return (true, true);
        }

        if Some(current_id) == base {
            return (false, true);
        }

        id = snapshot.parent_snapshot_id();
    }

    // An empty current ref after prior snapshots is not proof of a pristine base.
    (
        false,
        base.is_none()
            && (metadata.current_snapshot_id().is_some() || metadata.last_sequence_number() == 0),
    )
}

fn accumulate_file(totals: &mut [u64; 5], file: &DataFile) {
    let values = if file.content_type() == DataContentType::Data {
        [1, 0, file.record_count(), 0, file.file_size_in_bytes()]
    } else {
        [0, 1, 0, file.record_count(), file.file_size_in_bytes()]
    };
    for (total, value) in totals.iter_mut().zip(values) {
        *total += value;
    }
}
