use std::{collections::HashMap, sync::Arc};

use iceberg::{
    Catalog, CatalogBuilder, MemoryCatalog, NamespaceIdent, TableCommit, TableCreation,
    TableUpdate,
    io::MemoryStorageFactory,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    spec::{DataFileFormat, ManifestList, NestedField, PrimitiveType, Schema},
    table::Table,
};
use object_store::memory::InMemory;
use se_iceberg::publication::prepare_append;
use se_meta_store::table::{
    MaterializedDataFile, PendingCommit, PublicationIntent, TableDb, TableRead, TableWrite,
};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::TableCommitter;

#[tokio::test]
async fn commits_historical_schemas_by_field_id() {
    let (committer, catalog) = fixture(source_schema()).await;
    let table = catalog.load_table(&committer.table_ident).await.unwrap();
    enqueue(&committer, &table, 1, 0).await;

    let evolved = schema(vec![
        NestedField::optional(3, "new_column", PrimitiveType::String.into()),
        NestedField::optional(1, "renamed_key", PrimitiveType::Long.into()),
        // The committer trusts retained schemas, including changed nullability.
        NestedField::required(2, "value", PrimitiveType::String.into()),
    ]);
    let table = evolve(&catalog, &table, evolved).await;
    enqueue(&committer, &table, 2, table.metadata().current_schema_id()).await;

    committer.run_commit().await.unwrap();
    committer.run_commit().await.unwrap();
    let table = catalog.load_table(&committer.table_ident).await.unwrap();
    insta::assert_debug_snapshot!(file_paths(&table).await, @r#"
    [
        "memory://warehouse/table/data/1.parquet",
        "memory://warehouse/table/data/2.parquet",
    ]
    "#);
    assert_eq!(table.metadata().snapshots().count(), 1);
    assert_eq!(
        table.metadata().current_snapshot().unwrap().schema_id(),
        Some(table.metadata().current_schema_id())
    );
    let snapshot = committer.table.snapshot().await.unwrap();
    insta::assert_debug_snapshot!((snapshot.get_published_sequence().await.unwrap(), snapshot.read_pending_commits(None, 100).await.unwrap(), snapshot.get_publication_intent().await.unwrap()), @r#"
    (
        Some(
            2,
        ),
        [],
        None,
    )
    "#);
    committer.table.flush_and_close().await.unwrap();
}

#[tokio::test]
async fn schema_only_conflict_replans_the_frozen_prefix() {
    let (committer, catalog) = fixture(source_schema()).await;
    let table = catalog.load_table(&committer.table_ident).await.unwrap();
    enqueue(&committer, &table, 1, 0).await;
    let intent = reserve(&committer, &table).await;
    let evolved = schema(vec![
        NestedField::required(1, "key", PrimitiveType::Long.into()),
        NestedField::optional(2, "value", PrimitiveType::String.into()),
        NestedField::optional(3, "extra", PrimitiveType::Boolean.into()),
    ]);
    let table = evolve(&catalog, &table, evolved).await;
    assert_eq!(
        table.metadata().current_snapshot_id(),
        intent.prepared.as_ref().unwrap().base_snapshot_id
    );
    // A delayed original request is fenced even though main did not move.
    let delayed = intent
        .prepared
        .as_ref()
        .unwrap()
        .submit(catalog.as_ref(), &table)
        .await;
    assert!(delayed.is_err());
    enqueue(&committer, &table, 2, 0).await;

    committer.run_commit().await.unwrap();
    let table = catalog.load_table(&committer.table_ident).await.unwrap();
    assert_eq!(table.metadata().snapshots().count(), 2);
    assert!(!se_iceberg::publication::publication_found(
        &table,
        intent.token
    ));
    let current = table.metadata().current_snapshot().unwrap();
    let parent = table
        .metadata()
        .snapshot_by_id(current.parent_snapshot_id().unwrap())
        .unwrap();
    assert_eq!(
        parent.schema_id(),
        Some(table.metadata().current_schema_id())
    );
    let bytes = table
        .file_io()
        .new_input(parent.manifest_list())
        .unwrap()
        .read()
        .await
        .unwrap();
    let list = ManifestList::parse_with_version(&bytes, table.metadata().format_version()).unwrap();
    let manifest = list.entries()[0]
        .load_manifest(table.file_io())
        .await
        .unwrap();
    assert_eq!(manifest.entries().len(), 1);
    assert_eq!(
        manifest.entries()[0].data_file().file_path(),
        "memory://warehouse/table/data/1.parquet"
    );
    insta::assert_debug_snapshot!(file_paths(&table).await, @r#"
    [
        "memory://warehouse/table/data/1.parquet",
        "memory://warehouse/table/data/2.parquet",
    ]
    "#);
    assert!(
        intent
            .prepared
            .as_ref()
            .unwrap()
            .submit(catalog.as_ref(), &table)
            .await
            .is_err()
    );
    let snapshot = committer.table.snapshot().await.unwrap();
    assert_eq!(snapshot.get_published_sequence().await.unwrap(), Some(2));
    insta::assert_debug_snapshot!(snapshot.get_publication_intent().await.unwrap(), @"None");
    committer.table.flush_and_close().await.unwrap();
}

#[tokio::test]
async fn accepted_publication_is_acknowledged_after_schema_evolution() {
    let (committer, catalog) = fixture(source_schema()).await;
    let table = catalog.load_table(&committer.table_ident).await.unwrap();
    enqueue(&committer, &table, 1, 0).await;
    let intent = reserve(&committer, &table).await;
    let table = intent
        .prepared
        .as_ref()
        .unwrap()
        .submit(catalog.as_ref(), &table)
        .await
        .unwrap();
    // Even a now-incompatible upsert schema cannot undo an accepted append.
    let upsert = source_schema()
        .into_builder()
        .with_identifier_field_ids([1])
        .build()
        .unwrap();
    let table = evolve(&catalog, &table, upsert).await;
    committer.run_commit().await.unwrap();
    let snapshot = committer.table.snapshot().await.unwrap();
    assert_eq!(snapshot.get_published_sequence().await.unwrap(), Some(1));
    insta::assert_debug_snapshot!(snapshot.get_publication_intent().await.unwrap(), @"None");
    assert_eq!(
        catalog
            .load_table(&committer.table_ident)
            .await
            .unwrap()
            .metadata()
            .current_snapshot_id(),
        table.metadata().current_snapshot_id()
    );
    committer.table.flush_and_close().await.unwrap();
}

#[tokio::test]
async fn compares_only_the_current_and_commit_table_modes() {
    let mut outcomes = Vec::new();
    for (name, set_current, return_to_append) in [
        ("append_to_upsert", true, false),
        ("append_to_upsert_to_append", true, true),
        ("retained_upsert_schema", false, false),
    ] {
        let (committer, catalog) = fixture(source_schema()).await;
        let table = catalog.load_table(&committer.table_ident).await.unwrap();
        enqueue(&committer, &table, 1, 0).await;
        let intent = reserve(&committer, &table).await;
        let upsert = source_schema()
            .into_builder()
            .with_identifier_field_ids([1])
            .build()
            .unwrap();
        let table = if set_current {
            evolve(&catalog, &table, upsert).await
        } else {
            // An unrelated retained schema must not block publication.
            catalog
                .update_table(
                    TableCommit::builder()
                        .ident(table.identifier().clone())
                        .requirements(vec![])
                        .updates(vec![TableUpdate::AddSchema { schema: upsert }])
                        .build(),
                )
                .await
                .unwrap()
        };
        if return_to_append {
            // Use a new schema, not a rollback to the original schema ID.
            let append = schema(vec![NestedField::required(
                1,
                "renamed_key",
                PrimitiveType::Long.into(),
            )]);
            evolve(&catalog, &table, append).await;
        }
        let result = committer.run_commit().await;
        let should_reject = set_current && !return_to_append;
        assert_eq!(result.is_err(), should_reject);
        outcomes.push((name, result.err().map(|error| error.to_string())));
        let snapshot = committer.table.snapshot().await.unwrap();
        if should_reject {
            assert_eq!(
                snapshot.get_publication_intent().await.unwrap(),
                Some(intent)
            );
            assert_eq!(
                snapshot
                    .read_pending_commits(None, 100)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(snapshot.get_published_sequence().await.unwrap(), None);
            assert_eq!(
                catalog
                    .load_table(&committer.table_ident)
                    .await
                    .unwrap()
                    .metadata()
                    .current_snapshot_id(),
                None
            );
        } else {
            assert_eq!(snapshot.get_published_sequence().await.unwrap(), Some(1));
            assert_eq!(snapshot.get_publication_intent().await.unwrap(), None);
            assert_eq!(
                snapshot
                    .read_pending_commits(None, 100)
                    .await
                    .unwrap()
                    .len(),
                0
            );
            let table = catalog.load_table(&committer.table_ident).await.unwrap();
            assert_eq!(
                file_paths(&table).await,
                vec!["memory://warehouse/table/data/1.parquet"]
            );
        }
        committer.table.flush_and_close().await.unwrap();
    }
    insta::assert_debug_snapshot!(outcomes, @r#"
    [
        (
            "append_to_upsert",
            Some(
                "pending files are incompatible with the current table: table changed between append and upsert",
            ),
        ),
        (
            "append_to_upsert_to_append",
            None,
        ),
        (
            "retained_upsert_schema",
            None,
        ),
    ]
    "#);
}

#[tokio::test]
async fn rejects_an_unknown_file_schema_without_dequeuing() {
    let (committer, catalog) = fixture(source_schema()).await;
    let table = catalog.load_table(&committer.table_ident).await.unwrap();
    enqueue(&committer, &table, 1, 42).await;
    insta::assert_snapshot!(committer.run_commit().await.unwrap_err(), @"pending files are incompatible with the current table: file schema is not retained");
    let snapshot = committer.table.snapshot().await.unwrap();
    assert_eq!(
        snapshot
            .read_pending_commits(None, 100)
            .await
            .unwrap()
            .len(),
        1
    );
    insta::assert_debug_snapshot!((snapshot.get_published_sequence().await.unwrap(), snapshot.get_publication_intent().await.unwrap()), @r#"
    (
        None,
        None,
    )
    "#);
    assert_eq!(
        catalog
            .load_table(&committer.table_ident)
            .await
            .unwrap()
            .metadata()
            .current_snapshot_id(),
        None
    );
    committer.table.flush_and_close().await.unwrap();
}

fn schema(fields: Vec<NestedField>) -> Schema {
    Schema::builder()
        .with_fields(fields.into_iter().map(Arc::new))
        .build()
        .unwrap()
}

fn source_schema() -> Schema {
    schema(vec![
        NestedField::required(1, "key", PrimitiveType::Int.into()),
        NestedField::optional(2, "value", PrimitiveType::String.into()),
    ])
}

async fn fixture(schema: Schema) -> (TableCommitter, Arc<MemoryCatalog>) {
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(MemoryStorageFactory))
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
                .name("table".into())
                .location("memory://warehouse/table".into())
                .schema(schema)
                .properties(HashMap::new())
                .build(),
        )
        .await
        .unwrap();
    let table_ident = table.identifier().clone();
    let db = TableDb::load_or_init("table".into(), table, Arc::new(InMemory::new()))
        .await
        .unwrap();
    (
        TableCommitter {
            table: db,
            table_ident,
            catalog: catalog.clone(),
            commit_lock: Arc::new(Mutex::new(())),
        },
        catalog,
    )
}

async fn enqueue(committer: &TableCommitter, table: &Table, sequence: u64, schema_id: i32) {
    let tx = committer.table.begin_transaction().await.unwrap();
    tx.push_pending_commit(
        &PendingCommit {
            first_sequence: sequence,
            last_sequence: sequence,
            schema_id,
            table_location: table.metadata().location().into(),
            partition_spec_id: table.metadata().default_partition_spec_id(),
            data_files: vec![MaterializedDataFile {
                file_path: format!("data/{sequence}.parquet"),
                file_format: DataFileFormat::Parquet,
                record_count: 3,
                file_size_in_bytes: 1024,
                sort_order_id: 0,
            }],
        },
        sequence.checked_sub(1).filter(|sequence| *sequence != 0),
    )
    .await
    .unwrap();
    tx.commit()
        .await
        .unwrap()
        .unwrap()
        .await_durable()
        .await
        .unwrap();
}

async fn reserve(committer: &TableCommitter, table: &Table) -> PublicationIntent {
    let commits = committer
        .table
        .snapshot()
        .await
        .unwrap()
        .read_pending_commits(None, 100)
        .await
        .unwrap();
    let token = Uuid::from_u128(42);
    let intent = PublicationIntent {
        token,
        previous_sequence: None,
        first_sequence: 1,
        last_sequence: 1,
        prepared: Some(
            prepare_append(table, token, super::data_files(table, &commits).unwrap())
                .await
                .unwrap(),
        ),
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
    intent
}

async fn evolve(catalog: &MemoryCatalog, table: &Table, schema: Schema) -> Table {
    catalog
        .update_table(
            TableCommit::builder()
                .ident(table.identifier().clone())
                .requirements(vec![])
                .updates(vec![
                    TableUpdate::AddSchema { schema },
                    TableUpdate::SetCurrentSchema { schema_id: -1 },
                ])
                .build(),
        )
        .await
        .unwrap()
}

async fn file_paths(table: &Table) -> Vec<String> {
    let bytes = table
        .file_io()
        .new_input(table.metadata().current_snapshot().unwrap().manifest_list())
        .unwrap()
        .read()
        .await
        .unwrap();
    let list = ManifestList::parse_with_version(&bytes, table.metadata().format_version()).unwrap();
    let mut paths = Vec::new();
    for entry in list.entries() {
        let manifest = entry.load_manifest(table.file_io()).await.unwrap();
        paths.extend(
            manifest
                .entries()
                .iter()
                .map(|entry| entry.data_file().file_path().to_owned()),
        );
    }
    paths.sort();
    paths
}
