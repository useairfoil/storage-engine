//! Durable append plans submitted once against an exact catalog base.
//!
//! Recovery assumes `main` and schemas move forward, not through rollback or ref
//! replacement. Restoring an old schema ID could re-enable a delayed submission.
//! Location/format changes must be coordinated with publishers: the REST protocol
//! has no requirements to guard those values atomically.

use std::collections::{HashMap, HashSet};

use iceberg::{
    Catalog, TableCommit, TableRequirement, TableUpdate,
    spec::{DataFile, FormatVersion},
    table::Table,
    transaction::Transaction,
};
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
