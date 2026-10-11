//! Publication of materialized files to Iceberg tables.

use std::sync::Arc;

use iceberg::{
    Catalog,
    spec::{DataContentType, DataFile, DataFileBuilder, Struct},
    table::Table,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use se_common::id::IdGenerator;
use se_iceberg::{
    publication::{Resolution, prepare_append, publication_found},
    schema::SchemaExt,
};
use se_meta_store::{
    catalog::{CatalogName, CatalogStore, NamespaceIdent, TableIdent},
    table::{PendingCommit, PublicationIntent, TableDb, TableRead, TableWrite},
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
    commit_lock: Arc<Mutex<()>>,
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
        Ok(TableCommitter {
            table,
            table_ident,
            catalog,
            commit_lock: Arc::new(Mutex::new(())),
        })
    }
}

impl std::fmt::Debug for Committer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Committer").finish_non_exhaustive()
    }
}

impl TableCommitter {
    pub fn table(&self) -> &TableDb {
        &self.table
    }

    pub async fn run_commit(&self) -> Result<()> {
        let _guard = self.commit_lock.lock().await;

        self.table.flush().await?;

        let Some(head) = self
            .table
            .snapshot()
            .await?
            .get_materialized_sequence()
            .await?
        else {
            return Ok(());
        };

        loop {
            let iceberg_table = self.load_table().await?;
            let snapshot = self.table.snapshot().await?;

            if let Some(intent) = snapshot.get_publication_intent().await? {
                let resolution = match &intent.prepared {
                    Some(prepared) => prepared.resolve(&iceberg_table, intent.token)?,
                    None if publication_found(&iceberg_table, intent.token) => {
                        Resolution::Published
                    }
                    None => Resolution::Unknown,
                };

                match resolution {
                    Resolution::Published => self.finish_publication(&intent).await?,
                    Resolution::Superseded => {
                        self.replan_publication(&iceberg_table, &intent).await?
                    }
                    Resolution::Unknown => {
                        return Err(Error::PublicationOutcomeUnknown(intent.token));
                    }
                    Resolution::Retry => self.submit_publication(&iceberg_table, &intent).await?,
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

            let files = data_files(&iceberg_table, &commits)?;
            // TODO: make token generation deterministic.
            let token = rand::rng().gen_uuid();

            let prepared = prepare_append(&iceberg_table, token, files).await?;

            let intent = PublicationIntent {
                token,
                previous_sequence,
                first_sequence: first.first_sequence,
                // PANIC: The empty prefix is handled above.
                last_sequence: commits.last().expect("nonempty prefix").last_sequence,
                prepared: Some(prepared),
            };

            let tx = self.table.begin_transaction().await?;

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

            self.submit_publication(&iceberg_table, &intent).await?;
        }
    }

    async fn submit_publication(&self, table: &Table, intent: &PublicationIntent) -> Result<()> {
        // PANIC: Only freshly prepared or resolved Retry intents reach this method.
        let prepared = intent.prepared.as_ref().expect("prepared append");

        if let Err(error) = prepared.submit(self.catalog.as_ref(), table).await {
            let refreshed = self.load_table().await?;
            if prepared.resolve(&refreshed, intent.token)? != Resolution::Published {
                // Keep the durable plan. The next invocation resolves conflicts or retries;
                // never spin on a failing catalog or treat an error as proof of rejection.
                return Err(error.into());
            }
        }

        self.finish_publication(intent).await
    }

    async fn replan_publication(&self, table: &Table, intent: &PublicationIntent) -> Result<()> {
        let snapshot = self.table.snapshot().await?;

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

        let prepared = prepare_append(table, token, data_files(table, &commits)?).await?;
        let replacement = PublicationIntent {
            token,
            prepared: Some(prepared),
            ..intent.clone()
        };

        // Retire and replace atomically, preserving exactly the frozen queue range.
        let tx = self.table.begin_transaction().await?;

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

    async fn load_table(&self) -> Result<Table> {
        let table = self.catalog.load_table(&self.table_ident).await?;
        let stored_uuid = self.table.get_table_uuid().await?;
        if table.metadata().uuid() != stored_uuid {
            return Err(se_meta_store::catalog::Error::UuidMismatch {
                catalog_uuid: table.metadata().uuid(),
                stored_uuid,
            }
            .into());
        }
        Ok(table)
    }

    async fn finish_publication(&self, intent: &PublicationIntent) -> Result<()> {
        let tx = self.table.begin_transaction().await?;
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

        info!(table=%self.table_ident, token=%intent.token, last_sequence=intent.last_sequence, "published materialized files");

        Ok(())
    }
}

impl std::fmt::Debug for TableCommitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableCommitter")
            .field("table_ident", &self.table_ident)
            .finish_non_exhaustive()
    }
}

fn data_files(table: &Table, commits: &[PendingCommit]) -> Result<Vec<DataFile>> {
    let metadata = table.metadata();
    let table_mode = metadata.current_schema().table_mode();

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
