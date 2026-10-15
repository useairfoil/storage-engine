//! Publication of materialized files to Iceberg tables.

use std::{collections::BTreeMap, sync::Arc};

use iceberg::{
    Catalog,
    io::FileIO,
    spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, Struct},
    table::Table,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use se_common::id::IdGenerator;
use se_iceberg::{
    publication::{PreparedAppend, Resolution, prepare_append, prepare_upsert, publication_found},
    schema::{SchemaExt, TableMode},
};
use se_meta_store::{
    catalog::{CatalogName, CatalogStore, NamespaceIdent, TableIdent},
    table::{
        DeletionVectorReplacement, MaterializedDeletionVector, PendingCommit, PublicationIntent,
        TableDb, TableRead, TableWrite,
    },
};
use tokio::sync::Mutex;
use tracing::info;
use uuid::Uuid;

#[cfg(test)]
#[path = "../tests/committer/mod.rs"]
mod tests;

const PAGE_SIZE: usize = 100;

/// The error type for commit operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("catalog not found: {0}")]
    CatalogNotFound(CatalogName),
    #[error("table not found: {0}")]
    TableNotFound(TableIdent),
    #[error("catalog store error: {0}")]
    CatalogStore(#[from] se_meta_store::catalog::Error),
    #[error("table database error: {0}")]
    TableDb(#[from] se_meta_store::table::Error),
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),
    #[error("publication error: {0}")]
    Publication(#[from] se_iceberg::publication::Error),
    #[error("publication {0} has an unresolved catalog outcome; refusing to resubmit")]
    PublicationOutcomeUnknown(Uuid),
    #[error("pending files are incompatible with the current table: {0}")]
    IncompatibleTable(&'static str),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Creates table-specific committers.
#[derive(Clone)]
pub struct Committer {
    catalog_store: CatalogStore,
}

/// Publishes an existing table's pending commits, holding metadata writer ownership.
#[derive(Clone)]
pub struct TableCommitter {
    table: TableDb,
    table_ident: TableIdent,
    catalog: Arc<dyn Catalog>,
    file_io: FileIO,
    commit_lock: Arc<Mutex<()>>,
}

/// Publishes pending commits through the current materialized head.
///
/// The caller must serialize publication for this table and refresh metadata first.
pub async fn run_commit(
    table: &TableDb,
    table_ident: &TableIdent,
    catalog: &dyn Catalog,
    file_io: &FileIO,
) -> Result<()> {
    info!(table = %table_ident, "starting commit");

    let Some(head) = table.snapshot().await?.get_materialized_sequence().await? else {
        return Ok(());
    };

    loop {
        let iceberg_table = load_table(table, table_ident, file_io).await?;
        let snapshot = table.snapshot().await?;

        if let Some(intent) = snapshot.get_publication_intent().await? {
            let resolution = match &intent.prepared {
                Some(prepared) => prepared.resolve(&iceberg_table, intent.token)?,
                None if publication_found(&iceberg_table, intent.token) => Resolution::Published,
                None => Resolution::Unknown,
            };

            match resolution {
                Resolution::Published => finish_publication(table, table_ident, &intent).await?,
                Resolution::Superseded => {
                    replan_publication(table, &iceberg_table, &intent).await?
                }
                Resolution::Unknown => return Err(Error::PublicationOutcomeUnknown(intent.token)),
                Resolution::Retry => {
                    submit_publication(table, table_ident, catalog, &iceberg_table, &intent).await?
                }
            }

            continue;
        }

        let previous_sequence = snapshot.get_published_sequence().await?;
        let commits: Vec<_> = snapshot
            .read_pending_commits(previous_sequence, PAGE_SIZE)
            .await?
            .into_iter()
            .take_while(|commit| commit.last_sequence <= head)
            .collect();
        let Some(first) = commits.first() else {
            return Ok(());
        };

        // TODO: make token generation deterministic.
        let token = rand::rng().gen_uuid();
        let prepared = prepare_publication(&iceberg_table, token, &commits).await?;
        let intent = PublicationIntent {
            token,
            previous_sequence,
            first_sequence: first.first_sequence,
            // PANIC: The empty prefix is handled above.
            last_sequence: commits.last().expect("nonempty prefix").last_sequence,
            prepared: Some(prepared),
        };
        let tx = table.begin_transaction().await?;
        tx.start_publication(&intent).await?;
        if let Some(handle) = tx
            .commit()
            .await
            .map_err(se_meta_store::table::Error::from)?
        {
            handle
                .await_durable()
                .await
                .map_err(se_meta_store::table::Error::from)?;
        }
        submit_publication(table, table_ident, catalog, &iceberg_table, &intent).await?;
    }
}

impl Committer {
    pub fn new(catalog_store: CatalogStore) -> Self {
        Self { catalog_store }
    }

    /// Creates a committer for a table already linked in the meta store.
    pub async fn for_table(
        &self,
        catalog: CatalogName,
        namespace: NamespaceIdent,
        table_name: String,
    ) -> Result<TableCommitter> {
        let table_ident = TableIdent::new(namespace, table_name);
        let stored_catalog = self
            .catalog_store
            .get(catalog.clone())
            .await?
            .ok_or(Error::CatalogNotFound(catalog))?;
        let table = stored_catalog
            .try_load_table(table_ident.clone())
            .await?
            .ok_or_else(|| Error::TableNotFound(table_ident.clone()))?;
        let catalog = stored_catalog
            .to_catalog_with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            }))
            .await?;
        let iceberg_table = catalog.load_table(&table_ident).await?;
        table.refresh_metadata(iceberg_table.metadata()).await?;
        Ok(TableCommitter::new(
            table,
            table_ident,
            catalog,
            iceberg_table.file_io().clone(),
        ))
    }
}

impl std::fmt::Debug for Committer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Committer").finish_non_exhaustive()
    }
}

impl TableCommitter {
    /// Uses persisted metadata and in-memory catalog and storage configuration.
    pub fn new(
        table: TableDb,
        table_ident: TableIdent,
        catalog: Arc<dyn Catalog>,
        file_io: FileIO,
    ) -> Self {
        Self {
            table,
            table_ident,
            catalog,
            file_io,
            commit_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn table(&self) -> &TableDb {
        &self.table
    }

    pub async fn run_commit(&self) -> Result<()> {
        let _guard = self.commit_lock.lock().await;
        run_commit(
            &self.table,
            &self.table_ident,
            self.catalog.as_ref(),
            &self.file_io,
        )
        .await
    }
}

impl std::fmt::Debug for TableCommitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableCommitter")
            .field("table_ident", &self.table_ident)
            .finish_non_exhaustive()
    }
}

async fn submit_publication(
    db: &TableDb,
    table_ident: &TableIdent,
    catalog: &dyn Catalog,
    table: &Table,
    intent: &PublicationIntent,
) -> Result<()> {
    // PANIC: Only freshly prepared or resolved Retry intents reach this method.
    let prepared = intent.prepared.as_ref().expect("prepared append");

    // On an uncertain outcome, retain the durable intent. The next reconciliation
    // refreshes metadata before resolving or retrying it.
    let published = prepared.submit(catalog, table).await?;
    db.refresh_metadata(published.metadata()).await?;

    finish_publication(db, table_ident, intent).await
}

async fn replan_publication(db: &TableDb, table: &Table, intent: &PublicationIntent) -> Result<()> {
    let snapshot = db.snapshot().await?;

    let mut commits = Vec::new();
    let mut after = intent.previous_sequence;

    while after != Some(intent.last_sequence) {
        let page = snapshot.read_pending_commits(after, PAGE_SIZE).await?;
        if page.is_empty() {
            return Err(se_meta_store::table::Error::InvalidPublicationRange.into());
        }

        for commit in page {
            if commit.last_sequence > intent.last_sequence {
                return Err(se_meta_store::table::Error::InvalidPublicationRange.into());
            }

            after = Some(commit.last_sequence);
            commits.push(commit);
            if after == Some(intent.last_sequence) {
                break;
            }
        }
    }
    // TODO: make this deterministic
    let token = rand::rng().gen_uuid();

    let prepared = prepare_publication(table, token, &commits).await?;
    let replacement = PublicationIntent {
        token,
        prepared: Some(prepared),
        ..intent.clone()
    };

    // Retire and replace atomically, preserving exactly the frozen queue range.
    let tx = db.begin_transaction().await?;

    tx.retire_publication(intent).await?;
    tx.start_publication(&replacement).await?;

    if let Some(handle) = tx
        .commit()
        .await
        .map_err(se_meta_store::table::Error::from)?
    {
        handle
            .await_durable()
            .await
            .map_err(se_meta_store::table::Error::from)?;
    }

    Ok(())
}

async fn load_table(db: &TableDb, table_ident: &TableIdent, file_io: &FileIO) -> Result<Table> {
    Ok(Table::builder()
        .runtime(iceberg::Runtime::try_current()?)
        .identifier(table_ident.clone())
        .metadata(db.snapshot().await?.publication_metadata().await?)
        .file_io(file_io.clone())
        .build()?)
}

async fn finish_publication(
    db: &TableDb,
    table_ident: &TableIdent,
    intent: &PublicationIntent,
) -> Result<()> {
    let tx = db.begin_transaction().await?;
    tx.finish_publication(intent).await?;

    if let Some(handle) = tx
        .commit()
        .await
        .map_err(se_meta_store::table::Error::from)?
    {
        handle
            .await_durable()
            .await
            .map_err(se_meta_store::table::Error::from)?;
    }

    info!(table=%table_ident, token=%intent.token, last_sequence=intent.last_sequence, "published materialized files");

    Ok(())
}

fn data_files(table: &Table, commits: &[PendingCommit]) -> Result<Vec<DataFile>> {
    let metadata = table.metadata();
    let current = metadata
        .schema_by_id(metadata.current_schema_id())
        .ok_or(Error::IncompatibleTable("current schema is missing"))?;
    let table_mode = current.table_mode();

    if !metadata.default_partition_spec().is_unpartitioned() {
        return Err(Error::IncompatibleTable(
            "partitioned tables are not supported",
        ));
    }

    let mut files = Vec::new();

    for commit in commits {
        if commit.table_location.trim_end_matches('/') != metadata.location().trim_end_matches('/')
        {
            return Err(Error::IncompatibleTable("table location changed"));
        }

        if commit.partition_spec_id != metadata.default_partition_spec_id() {
            return Err(Error::IncompatibleTable("default partition spec changed"));
        }

        let schema = metadata
            .schema_by_id(commit.schema_id)
            .ok_or(Error::IncompatibleTable("file schema is not retained"))?;

        if schema.table_mode() != table_mode {
            return Err(Error::IncompatibleTable(
                "table changed between append and upsert",
            ));
        }

        if table_mode == TableMode::Upsert {
            let mut ids = schema.identifier_field_ids().collect::<Vec<_>>();
            let mut current_ids = current.identifier_field_ids().collect::<Vec<_>>();
            ids.sort_unstable();
            current_ids.sort_unstable();
            if ids != current_ids
                || ids.iter().any(|id| {
                    schema.field_by_id(*id).map(|field| &field.field_type)
                        != current.field_by_id(*id).map(|field| &field.field_type)
                })
            {
                return Err(Error::IncompatibleTable(
                    "identifier fields or their types changed",
                ));
            }
        }

        if commit.data_files.is_empty() {
            return Err(Error::IncompatibleTable("publication has no data files"));
        }

        for file in &commit.data_files {
            let sort_order_id = i32::try_from(file.sort_order_id).map_err(|_| {
                Error::IncompatibleTable("sort order ID exceeds Iceberg file range")
            })?;

            files.push(
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(format!(
                        "{}/{}",
                        commit.table_location.trim_end_matches('/'),
                        file.file_path
                    ))
                    .file_format(file.file_format)
                    .record_count(file.record_count)
                    .file_size_in_bytes(file.file_size_in_bytes)
                    .column_sizes(file.column_sizes.clone())
                    .value_counts(file.value_counts.clone())
                    .null_value_counts(file.null_value_counts.clone())
                    .nan_value_counts(file.nan_value_counts.clone())
                    .lower_bounds(file.lower_bounds.clone())
                    .upper_bounds(file.upper_bounds.clone())
                    .partition(Struct::empty())
                    .partition_spec_id(commit.partition_spec_id)
                    .sort_order_id(sort_order_id)
                    // PANIC: All required data-file builder fields are set above.
                    .build()
                    .expect("all required data-file fields are set"),
            );
        }
    }

    Ok(files)
}

async fn prepare_publication(
    table: &Table,
    token: Uuid,
    commits: &[PendingCommit],
) -> Result<PreparedAppend> {
    let files = data_files(table, commits)?;
    let metadata = table.metadata();
    let schema = metadata
        .schema_by_id(metadata.current_schema_id())
        .ok_or(Error::IncompatibleTable("current schema is missing"))?;
    if schema.table_mode() == TableMode::Append {
        if commits
            .iter()
            .any(|commit| !commit.deletion_vectors.is_empty())
        {
            return Err(Error::IncompatibleTable(
                "append work contains deletion vectors",
            ));
        }
        return Ok(prepare_append(table, token, files).await?);
    }
    // Keep the first previous descriptor and last cumulative descriptor per target.
    // Intermediate Puffin blobs were never visible and must not be removed or added.
    let mut vectors = BTreeMap::<&str, DeletionVectorReplacement>::new();
    for commit in commits {
        for replacement in &commit.deletion_vectors {
            match vectors.entry(&replacement.current.data_file_path) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(replacement.clone());
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if replacement.previous.as_ref() != Some(&entry.get().current) {
                        return Err(Error::IncompatibleTable(
                            "noncontiguous vector replacement chain",
                        ));
                    }
                    entry.get_mut().current = replacement.current.clone();
                }
            }
        }
    }
    let mut added = Vec::new();
    let mut removed = Vec::new();
    for replacement in vectors.into_values() {
        added.push(deletion_file(table, &replacement.current)?);
        if let Some(previous) = replacement.previous {
            removed.push(deletion_file(table, &previous)?);
        }
    }
    Ok(prepare_upsert(table, token, files, added, removed).await?)
}

fn deletion_file(table: &Table, vector: &MaterializedDeletionVector) -> Result<DataFile> {
    let prefix = table.metadata().location().trim_end_matches('/');
    Ok(DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(format!("{prefix}/{}", vector.puffin_path))
        .file_format(DataFileFormat::Puffin)
        .partition(Struct::empty())
        .partition_spec_id(table.metadata().default_partition_spec_id())
        .record_count(vector.cardinality)
        .file_size_in_bytes(vector.file_size_in_bytes)
        .referenced_data_file(Some(format!("{prefix}/{}", vector.data_file_path)))
        .content_offset(Some(vector.offset))
        .content_size_in_bytes(Some(vector.length))
        // PANIC: All required delete-file fields are set above.
        .build()
        .expect("all required delete-file fields are set"))
}
