//! Publication of materialized files to Iceberg tables.

use std::{collections::HashMap, sync::Arc};

use iceberg::{
    Catalog,
    spec::{DataContentType, DataFile, DataFileBuilder, Struct},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use se_common::id::IdGenerator;
use se_meta_store::{
    catalog::{CatalogName, CatalogStore, NamespaceIdent, TableIdent},
    table::{PendingCommit, PublicationIntent, TableDb, TableRead, TableWrite},
};
use tokio::sync::Mutex;
use tracing::info;
use uuid::Uuid;

const PAGE_SIZE: usize = 100;
const PUBLICATION_TOKEN: &str = "se.publication-token";

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

    /// Publishes queue prefixes through the captured materialized frontier.
    /// Unresolved intents are reconciled, never resubmitted. A missing token is not
    /// proof of rejection: the catalog request may still be in flight or its snapshot expired.
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
                if !publication_found(&iceberg_table, &intent) {
                    return Err(Error::PublicationOutcomeUnknown(intent.token));
                }
                self.finish_publication(&intent).await?;
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
            let intent = PublicationIntent {
                token: rand::rng().gen_uuid(),
                previous_sequence,
                first_sequence: first.first_sequence,
                // PANIC: The empty prefix is handled above.
                last_sequence: commits.last().expect("nonempty prefix").last_sequence,
                base_snapshot_id: iceberg_table.metadata().current_snapshot_id(),
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
            // Only this invocation created the durable intent, so only it may submit.
            // Keep duplicate-file validation enabled, including during SDK conflict retries.
            let tx = Transaction::new(&iceberg_table);
            let tx = tx
                .fast_append()
                .set_commit_uuid(intent.token)
                .set_snapshot_properties(HashMap::from([(
                    PUBLICATION_TOKEN.into(),
                    intent.token.to_string(),
                )]))
                .add_data_files(files)
                .apply(tx)?;
            if let Err(error) = tx.commit(self.catalog.as_ref()).await {
                let refreshed = self.load_table().await?;
                if !publication_found(&refreshed, &intent) {
                    // Retain the intent even on failure; an error is not proof of non-publication.
                    return Err(error.into());
                }
            }
            self.finish_publication(&intent).await?;
        }
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
        if metadata.schema_by_id(commit.schema_id).is_none() {
            return Err(Error::IncompatibleTable("materialized schema is missing"));
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

fn publication_found(table: &Table, intent: &PublicationIntent) -> bool {
    let token = intent.token.to_string();
    let mut snapshot = table.metadata().current_snapshot();
    while let Some(current) = snapshot {
        if current
            .summary()
            .additional_properties
            .get(PUBLICATION_TOKEN)
            == Some(&token)
        {
            return true;
        }
        if Some(current.snapshot_id()) == intent.base_snapshot_id {
            return false;
        }
        snapshot = current
            .parent_snapshot_id()
            .and_then(|id| table.metadata().snapshot_by_id(id));
    }
    false
}

#[cfg(test)]
mod tests {
    use iceberg::{
        CatalogBuilder, TableCreation,
        memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
        spec::{DataFileFormat, ManifestList, NestedField, PrimitiveType, Schema, Type},
    };
    use object_store::memory::InMemory;
    use se_meta_store::table::MaterializedDataFile;

    use super::*;

    #[tokio::test]
    async fn publishes_queue_pages_and_repeated_runs_do_not_duplicate_files() {
        let committer = committer().await;
        committer.run_commit().await.unwrap();
        let mut queued = Vec::new();
        for sequence in 0..103 {
            let pending = pending(&committer, sequence).await;
            let tx = committer.table.begin_transaction().await.unwrap();
            tx.push_pending_commit(&pending, sequence.checked_sub(1))
                .await
                .unwrap();
            tx.commit().await.unwrap();
            queued.push(pending);
        }
        let clone = committer.clone();
        let (first, second) = tokio::join!(committer.run_commit(), clone.run_commit());
        first.unwrap();
        second.unwrap();
        let table = committer
            .catalog
            .load_table(&committer.table_ident)
            .await
            .unwrap();
        assert_eq!(table.metadata().snapshots().len(), 2);
        let manifest_list = ManifestList::parse_with_version(
            &table
                .file_io()
                .new_input(table.metadata().current_snapshot().unwrap().manifest_list())
                .unwrap()
                .read()
                .await
                .unwrap(),
            table.metadata().format_version(),
        )
        .unwrap();
        let mut actual = Vec::new();
        for manifest in manifest_list.entries() {
            for entry in manifest
                .load_manifest(table.file_io())
                .await
                .unwrap()
                .entries()
            {
                let file = entry.data_file();
                actual.push((
                    file.file_path().to_owned(),
                    file.record_count(),
                    file.file_size_in_bytes(),
                    file.sort_order_id(),
                    file.partition().clone(),
                ));
            }
        }
        actual.sort_by(|a, b| a.0.cmp(&b.0));
        let mut expected: Vec<_> = queued
            .iter()
            .flat_map(|commit| {
                commit.data_files.iter().map(|file| {
                    (
                        format!("{}/{}", commit.table_location, file.file_path),
                        file.record_count,
                        file.file_size_in_bytes,
                        Some(0),
                        Struct::empty(),
                    )
                })
            })
            .collect();
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(actual, expected);
        let snapshot = committer.table.snapshot().await.unwrap();
        assert_eq!(snapshot.get_published_sequence().await.unwrap(), Some(102));
        assert_eq!(
            snapshot.get_materialized_sequence().await.unwrap(),
            Some(102)
        );
        assert_eq!(snapshot.get_publication_intent().await.unwrap(), None);
        assert_eq!(
            snapshot.read_pending_commits(None, 200).await.unwrap(),
            vec![]
        );
        committer.run_commit().await.unwrap();
        assert_eq!(
            committer
                .catalog
                .load_table(&committer.table_ident)
                .await
                .unwrap()
                .metadata()
                .snapshots()
                .len(),
            2
        );
        committer.table.flush_and_close().await.unwrap();
    }

    #[tokio::test]
    async fn recovers_publication_before_ack_even_after_another_catalog_append() {
        let committer = committer().await;
        let first = pending(&committer, 0).await;
        let second = pending(&committer, 1).await;
        let tx = committer.table.begin_transaction().await.unwrap();
        tx.push_pending_commit(&first, None).await.unwrap();
        tx.push_pending_commit(&second, Some(0)).await.unwrap();
        let intent = PublicationIntent {
            token: Uuid::from_u128(17),
            previous_sequence: None,
            first_sequence: 0,
            last_sequence: 0,
            base_snapshot_id: None,
        };
        tx.start_publication(&intent).await.unwrap();
        tx.commit()
            .await
            .unwrap()
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        // Simulate catalog success followed by a crash before local acknowledgement.
        let table = committer
            .catalog
            .load_table(&committer.table_ident)
            .await
            .unwrap();
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .set_snapshot_properties(HashMap::from([(
                PUBLICATION_TOKEN.into(),
                intent.token.to_string(),
            )]))
            .add_data_files(data_files(&table, &[first]).unwrap())
            .apply(tx)
            .unwrap();
        let table = tx.commit(committer.catalog.as_ref()).await.unwrap();
        let mut unrelated = second.clone();
        unrelated.data_files[0].file_path = "data/unrelated.parquet".into();
        let tx = Transaction::new(&table);
        tx.fast_append()
            .add_data_files(data_files(&table, &[unrelated]).unwrap())
            .apply(tx)
            .unwrap()
            .commit(committer.catalog.as_ref())
            .await
            .unwrap();
        committer.run_commit().await.unwrap();
        let table = committer
            .catalog
            .load_table(&committer.table_ident)
            .await
            .unwrap();
        assert_eq!(table.metadata().snapshots().len(), 3);
        let snapshot = committer.table.snapshot().await.unwrap();
        assert_eq!(snapshot.get_published_sequence().await.unwrap(), Some(1));
        assert_eq!(snapshot.get_publication_intent().await.unwrap(), None);
        assert_eq!(
            snapshot.read_pending_commits(None, 10).await.unwrap(),
            vec![]
        );
        committer.table.flush_and_close().await.unwrap();
    }

    #[tokio::test]
    async fn unresolved_intent_is_never_resubmitted_and_invalid_files_stay_queued() {
        let committer = committer().await;
        let mut pending = pending(&committer, 0).await;
        pending.table_location = "memory://other-table".into();
        let tx = committer.table.begin_transaction().await.unwrap();
        tx.push_pending_commit(&pending, None).await.unwrap();
        tx.commit().await.unwrap();
        assert!(matches!(
            committer.run_commit().await,
            Err(Error::IncompatibleTable("table location changed"))
        ));
        assert_eq!(
            committer
                .table
                .snapshot()
                .await
                .unwrap()
                .get_publication_intent()
                .await
                .unwrap(),
            None
        );
        let intent = PublicationIntent {
            token: Uuid::from_u128(99),
            previous_sequence: None,
            first_sequence: 0,
            last_sequence: 0,
            base_snapshot_id: None,
        };
        let tx = committer.table.begin_transaction().await.unwrap();
        tx.start_publication(&intent).await.unwrap();
        tx.commit()
            .await
            .unwrap()
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        for _ in 0..2 {
            assert!(
                matches!(committer.run_commit().await, Err(Error::PublicationOutcomeUnknown(token)) if token == intent.token)
            );
        }
        let snapshot = committer.table.snapshot().await.unwrap();
        assert_eq!(
            snapshot.get_publication_intent().await.unwrap(),
            Some(intent)
        );
        assert_eq!(snapshot.get_published_sequence().await.unwrap(), None);
        assert_eq!(
            snapshot.read_pending_commits(None, 10).await.unwrap(),
            vec![pending]
        );
        assert_eq!(
            committer
                .catalog
                .load_table(&committer.table_ident)
                .await
                .unwrap()
                .metadata()
                .snapshots()
                .len(),
            0
        );
        committer.table.flush_and_close().await.unwrap();
    }

    async fn committer() -> TableCommitter {
        let catalog = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "test",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.into(), "memory://warehouse".into())]),
                )
                .await
                .unwrap(),
        );
        let namespace = NamespaceIdent::new("test".into());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("events".into())
                    .schema(
                        Schema::builder()
                            .with_fields([NestedField::optional(
                                17,
                                "key",
                                Type::Primitive(PrimitiveType::Long),
                            )
                            .into()])
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .await
            .unwrap();
        let table_ident = table.identifier().clone();
        let table = TableDb::load_or_init("metadata".into(), table, Arc::new(InMemory::new()))
            .await
            .unwrap();
        TableCommitter {
            table,
            table_ident,
            catalog,
            commit_lock: Arc::new(Mutex::new(())),
        }
    }

    async fn pending(committer: &TableCommitter, sequence: u64) -> PendingCommit {
        let table = committer
            .catalog
            .load_table(&committer.table_ident)
            .await
            .unwrap();
        PendingCommit {
            first_sequence: sequence,
            last_sequence: sequence,
            schema_id: table.metadata().current_schema_id(),
            table_location: table.metadata().location().into(),
            partition_spec_id: table.metadata().default_partition_spec_id(),
            data_files: vec![MaterializedDataFile {
                file_path: format!("data/{sequence}.parquet"),
                file_format: DataFileFormat::Parquet,
                record_count: sequence + 7,
                file_size_in_bytes: sequence + 1000,
                sort_order_id: 0,
            }],
        }
    }
}
