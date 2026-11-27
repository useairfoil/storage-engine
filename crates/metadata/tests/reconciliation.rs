mod common;

use std::collections::{BTreeMap, HashMap};

use chrono::DateTime;
use common::TestEnvironment;
use iceberg::{
    TableCommit, TableIdent, TableRequirement,
    spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, Struct},
    table::Table,
    transaction::Transaction,
};
use se_common::clock::SystemClock;
use se_iceberg::testing::{CommitFault, test_table};
use se_metadata::{
    CHECKPOINT_INTERVAL_PROPERTY, CatalogConfig, CatalogId, DataFileMetadata,
    DeletionVectorMetadata, DeletionVectorReplacement, MaterializationResult, PreparedCommit,
    RECONCILIATION_ID_PROPERTY, REVISION_PROPERTY, ReconciliationInput, RestCatalogConfig, Task,
    TaskAssignment,
};
use uuid::Uuid;

struct ReconciliationFixture {
    env: TestEnvironment,
    catalog: CatalogId,
    table: Table,
}

#[derive(Debug)]
#[allow(dead_code)]
struct Revisions {
    reconciled: u64,
    desired: u64,
}

#[tokio::test]
async fn pins_reconciliation_plan_and_publishes_sequential_data_file_revisions()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = ReconciliationFixture::new().await?;

    // Publish two files in revision 1, then claim its reconciliation plan.
    fixture.materialize_files(&["first", "second"]).await?;
    let first = fixture.claim_reconciliation(2).await?;
    let input = fixture
        .env
        .metadata
        .reconciliation_input(&first.token)
        .await?;

    insta::assert_compact_debug_snapshot!(
        (
            input.plan.base_revision,
            input.plan.target_revision,
            input.plan.base_snapshot_id,
            file_paths(&input),
        ),
        @r#"(0, 1, None, ["memory://table/data/first.parquet", "memory://table/data/second.parquet"])"#
    );

    // Same worker gets the same assignment; other workers are excluded.
    assert_eq!(
        fixture
            .env
            .metadata
            .claim_task(first.token.worker_id)
            .await?,
        Some(first.clone())
    );
    insta::assert_debug_snapshot!(
        fixture.env.metadata.claim_task(Uuid::from_u128(3)).await?,
        @"None"
    );

    // Materializing revision 2 must not alter the pinned plan or its input.
    fixture.materialize_files(&["third"]).await?;
    let fixed = fixture
        .env
        .metadata
        .reconciliation_input(&first.token)
        .await?;
    assert_eq!(fixed.plan, input.plan);
    assert_eq!(fixed.files, input.files);

    let prepared = prepare_append(&fixture.published_table(), &input).await?;
    fixture
        .env
        .metadata
        .complete_reconciliation(&first.token, prepared)
        .await?;

    insta::assert_compact_debug_snapshot!(
        fixture.revisions().await?,
        @"Revisions { reconciled: 1, desired: 2 }"
    );
    assert!(fixture.debug_table().await?.prepared_commit.is_none());

    // The next plan starts from the confirmed snapshot and carries only revision 2.
    let second = fixture.claim_reconciliation(4).await?;
    let second_input = fixture
        .env
        .metadata
        .reconciliation_input(&second.token)
        .await?;
    let published = fixture.published_table();

    assert_eq!(
        second_input.plan.base_snapshot_id,
        published.metadata().current_snapshot_id()
    );
    insta::assert_compact_debug_snapshot!(
        (
            second_input.plan.base_revision,
            second_input.plan.target_revision,
            file_paths(&second_input),
        ),
        @r#"(1, 2, ["memory://table/data/third.parquet"])"#
    );

    let second_prepared = prepare_append(&published, &second_input).await?;
    fixture
        .env
        .metadata
        .complete_reconciliation(&second.token, second_prepared)
        .await?;

    let debug = fixture.debug_table().await?;
    insta::assert_compact_debug_snapshot!(
        fixture.revisions().await?,
        @"Revisions { reconciled: 2, desired: 2 }"
    );
    assert!(debug.state.reconciliation.is_none());
    assert!(debug.state.reconciliation_plan.is_none());
    assert!(debug.prepared_commit.is_none());
    insta::assert_debug_snapshot!(fixture.env.server.commit_count(), @"2");

    let (data_files, vectors) = live_manifest_files(&fixture.published_table()).await?;
    insta::assert_debug_snapshot!(data_files, @r#"
    [
        "memory://table/data/first.parquet",
        "memory://table/data/second.parquet",
        "memory://table/data/third.parquet",
    ]
    "#);
    insta::assert_debug_snapshot!(vectors, @"[]");

    fixture.close().await?;

    Ok(())
}

#[tokio::test]
async fn rejects_invalid_prepared_commits_and_external_branch_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = ReconciliationFixture::new().await?;

    fixture.materialize_files(&["first", "second"]).await?;
    let task = fixture.claim_reconciliation(2).await?;
    let input = fixture
        .env
        .metadata
        .reconciliation_input(&task.token)
        .await?;
    let prepared = prepare_append(&fixture.published_table(), &input).await?;

    // Prepared commits missing required Iceberg assertions are rejected before contacting catalog.
    let mut invalid = prepared.clone();
    invalid.requirements.clear();
    insta::assert_compact_debug_snapshot!(
        fixture
            .env
            .metadata
            .complete_reconciliation(&task.token, invalid)
            .await
            .err(),
        @"Some(InvalidPreparedCommit)"
    );
    insta::assert_debug_snapshot!(fixture.env.server.commit_count(), @"0");

    // An external branch advance causes a conflict rather than rebasing or overwriting.
    let mut external_input = input.clone();
    external_input.plan.id = Uuid::from_u128(99);
    let external = prepare_append(&fixture.table, &external_input).await?;
    let changed = TableCommit::builder()
        .ident(fixture.table.identifier().clone())
        .updates(external.updates)
        .requirements(external.requirements)
        .build()
        .apply(fixture.table.clone())?;
    fixture.env.server.put_table(changed);

    insta::assert_compact_debug_snapshot!(
        fixture
            .env
            .metadata
            .complete_reconciliation(&task.token, prepared)
            .await
            .err(),
        @"Some(ReconciliationConflict)"
    );
    insta::assert_debug_snapshot!(fixture.env.server.commit_count(), @"0");
    insta::assert_compact_debug_snapshot!(
        fixture.revisions().await?,
        @"Revisions { reconciled: 0, desired: 1 }"
    );
    assert!(fixture.debug_table().await?.prepared_commit.is_some());

    fixture.close().await?;

    Ok(())
}

#[tokio::test]
async fn recovers_uncertain_commits_across_restarts_and_retries_pre_apply_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = ReconciliationFixture::new().await?;

    fixture.materialize_files(&["first", "second"]).await?;
    let first = fixture.claim_reconciliation(2).await?;
    let input = fixture
        .env
        .metadata
        .reconciliation_input(&first.token)
        .await?;
    fixture.materialize_files(&["third"]).await?;

    let prepared = prepare_append(&fixture.published_table(), &input).await?;

    // The catalog applies the commit but returns 500; metadata persists the prepared commit.
    fixture
        .env
        .server
        .fail_next_commit(CommitFault::ApplyThenFail);
    let error = fixture
        .env
        .metadata
        .complete_reconciliation(&first.token, prepared.clone())
        .await
        .unwrap_err();
    assert!(matches!(error, se_metadata::Error::Iceberg(_)));
    insta::assert_debug_snapshot!(fixture.env.server.commit_count(), @"1");
    insta::assert_compact_debug_snapshot!(
        fixture.revisions().await?,
        @"Revisions { reconciled: 0, desired: 2 }"
    );
    assert_eq!(
        fixture.debug_table().await?.prepared_commit,
        Some(prepared.clone())
    );

    // Restart and expire the lease so another worker recovers the prepared commit.
    fixture.env.reopen().await?;
    fixture.advance_clock(30_000);
    let recovered = fixture
        .env
        .metadata
        .claim_task(Uuid::from_u128(4))
        .await?
        .ok_or("missing recovered reconciliation")?;

    assert_eq!(recovered.task, first.task);
    assert_ne!(recovered.token.attempt_id, first.token.attempt_id);
    insta::assert_compact_debug_snapshot!(
        fixture
            .env
            .metadata
            .complete_reconciliation(&first.token, prepared.clone())
            .await
            .err(),
        @"Some(TaskFenced)"
    );

    let saved = fixture
        .env
        .metadata
        .reconciliation_input(&recovered.token)
        .await?;
    assert_eq!(saved.prepared.as_ref(), Some(&prepared));

    // Retrying with a different prepared payload is rejected.
    let mut different = prepared;
    different
        .requirements
        .push(TableRequirement::DefaultSpecIdMatch { default_spec_id: 0 });
    insta::assert_compact_debug_snapshot!(
        fixture
            .env
            .metadata
            .complete_reconciliation(&recovered.token, different)
            .await
            .err(),
        @"Some(InvalidPreparedCommit)"
    );

    // Completing with the saved payload observes the marker without sending a duplicate commit.
    fixture
        .env
        .metadata
        .complete_reconciliation(
            &recovered.token,
            saved.prepared.ok_or("missing prepared commit")?,
        )
        .await?;
    insta::assert_debug_snapshot!(fixture.env.server.commit_count(), @"1");
    insta::assert_compact_debug_snapshot!(
        fixture.revisions().await?,
        @"Revisions { reconciled: 1, desired: 2 }"
    );
    assert!(fixture.debug_table().await?.prepared_commit.is_none());

    // A failure before catalog mutation is retried with the exact saved payload.
    let next = fixture.claim_reconciliation(5).await?;
    let next_input = fixture
        .env
        .metadata
        .reconciliation_input(&next.token)
        .await?;
    let next_prepared = prepare_append(&fixture.published_table(), &next_input).await?;

    fixture
        .env
        .server
        .fail_next_commit(CommitFault::FailBeforeApply);
    let error = fixture
        .env
        .metadata
        .complete_reconciliation(&next.token, next_prepared.clone())
        .await
        .unwrap_err();
    assert!(matches!(error, se_metadata::Error::Iceberg(_)));
    insta::assert_debug_snapshot!(fixture.env.server.commit_count(), @"1");

    fixture
        .env
        .metadata
        .complete_reconciliation(&next.token, next_prepared)
        .await?;
    insta::assert_debug_snapshot!(fixture.env.server.commit_count(), @"2");
    insta::assert_compact_debug_snapshot!(
        fixture.revisions().await?,
        @"Revisions { reconciled: 2, desired: 2 }"
    );
    assert!(fixture.debug_table().await?.prepared_commit.is_none());

    fixture.close().await?;

    Ok(())
}

#[tokio::test]
async fn publishes_deletion_vectors_rejects_plain_appends_and_recovers_lost_overwrites()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = ReconciliationFixture::new().await?;
    fixture.publish_data_files(&["first", "second"]).await?;

    let first_vector = sample_vector("first", "first-v1", 1);
    let baseline = sample_vector("second", "second-v1", 1);
    fixture
        .materialize_vectors(vec![
            DeletionVectorReplacement {
                previous: None,
                current: first_vector,
            },
            DeletionVectorReplacement {
                previous: None,
                current: baseline,
            },
        ])
        .await?;

    let task = fixture.claim_reconciliation(6).await?;
    let input = fixture
        .env
        .metadata
        .reconciliation_input(&task.token)
        .await?;

    insta::assert_compact_debug_snapshot!(
        (
            input.plan.base_revision,
            input.plan.target_revision,
            input.files.len(),
            input.added_vectors.len(),
            input.removed_vectors.len(),
        ),
        @"(1, 2, 0, 2, 0)"
    );

    // Even a fenced, marked plain append must not acknowledge deletion vector changes.
    let published = fixture.published_table();
    let append = prepare_append(&published, &input).await?;
    insta::assert_compact_debug_snapshot!(
        fixture
            .env
            .metadata
            .complete_reconciliation(&task.token, append)
            .await
            .err(),
        @"Some(InvalidPreparedCommit)"
    );
    insta::assert_debug_snapshot!(fixture.env.server.commit_count(), @"1");
    insta::assert_compact_debug_snapshot!(
        fixture.revisions().await?,
        @"Revisions { reconciled: 1, desired: 2 }"
    );
    assert!(fixture.debug_table().await?.prepared_commit.is_none());

    // A lost overwrite response is recovered by its marker without duplicating vectors.
    let prepared = prepare_vectors(&published, &input).await?;
    fixture
        .env
        .server
        .fail_next_commit(CommitFault::ApplyThenFail);
    let error = fixture
        .env
        .metadata
        .complete_reconciliation(&task.token, prepared.clone())
        .await
        .unwrap_err();
    assert!(matches!(error, se_metadata::Error::Iceberg(_)));
    insta::assert_debug_snapshot!(fixture.env.server.commit_count(), @"2");
    assert_eq!(
        fixture
            .env
            .metadata
            .reconciliation_input(&task.token)
            .await?
            .prepared,
        Some(prepared.clone())
    );

    fixture
        .env
        .metadata
        .complete_reconciliation(&task.token, prepared)
        .await?;
    insta::assert_debug_snapshot!(fixture.env.server.commit_count(), @"2");
    insta::assert_compact_debug_snapshot!(
        fixture.revisions().await?,
        @"Revisions { reconciled: 2, desired: 2 }"
    );
    assert!(fixture.debug_table().await?.prepared_commit.is_none());

    let (data_files, vectors) = live_manifest_files(&fixture.published_table()).await?;
    insta::assert_debug_snapshot!(data_files, @r#"
    [
        "memory://table/data/first.parquet",
        "memory://table/data/second.parquet",
    ]
    "#);
    insta::assert_debug_snapshot!(vectors, @r#"
    [
        DeletionVectorMetadata {
            puffin_path: "memory://table/deletes/first-v1.puffin",
            target_file: "memory://table/data/first.parquet",
            cardinality: 1,
            file_size: 80,
            offset: 4,
            length: 20,
        },
        DeletionVectorMetadata {
            puffin_path: "memory://table/deletes/second-v1.puffin",
            target_file: "memory://table/data/second.parquet",
            cardinality: 1,
            file_size: 80,
            offset: 4,
            length: 20,
        },
    ]
    "#);

    fixture.close().await?;

    Ok(())
}

#[tokio::test]
async fn pins_and_batches_mvcc_deletion_vector_replacements()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = ReconciliationFixture::new().await?;
    fixture.publish_data_files(&["first", "second"]).await?;

    let initial = sample_vector("first", "first-v1", 1);
    let baseline = sample_vector("second", "second-v1", 1);
    fixture
        .publish_vectors(vec![
            DeletionVectorReplacement {
                previous: None,
                current: initial.clone(),
            },
            DeletionVectorReplacement {
                previous: None,
                current: baseline,
            },
        ])
        .await?;

    // Revision 3 replaces first-v1 with first-v2; pin that plan before newer revisions arrive.
    let replacement = sample_vector("first", "first-v2", 2);
    fixture
        .materialize_vectors(vec![DeletionVectorReplacement {
            previous: Some(initial),
            current: replacement.clone(),
        }])
        .await?;

    let pinned = fixture.claim_reconciliation(7).await?;
    let pinned_input = fixture
        .env
        .metadata
        .reconciliation_input(&pinned.token)
        .await?;
    insta::assert_debug_snapshot!(
        (
            pinned_input.plan.base_revision,
            pinned_input.plan.target_revision,
            &pinned_input.added_vectors,
            &pinned_input.removed_vectors,
        ),
        @r#"
    (
        2,
        3,
        [
            VersionedDeletionVector {
                metadata: DeletionVectorMetadata {
                    puffin_path: "memory://table/deletes/first-v2.puffin",
                    target_file: "memory://table/data/first.parquet",
                    cardinality: 2,
                    file_size: 80,
                    offset: 4,
                    length: 20,
                },
                added_revision: 3,
                removed_revision: None,
            },
        ],
        [
            VersionedDeletionVector {
                metadata: DeletionVectorMetadata {
                    puffin_path: "memory://table/deletes/first-v1.puffin",
                    target_file: "memory://table/data/first.parquet",
                    cardinality: 1,
                    file_size: 80,
                    offset: 4,
                    length: 20,
                },
                added_revision: 2,
                removed_revision: Some(
                    3,
                ),
            },
        ],
    )
    "#
    );

    // Two later materializations supersede first-v2 while the pinned plan is leased.
    let intermediate = sample_vector("first", "first-v3", 3);
    let latest = sample_vector("first", "first-v4", 4);
    for (previous, current) in [(replacement, intermediate.clone()), (intermediate, latest)] {
        fixture
            .materialize_vectors(vec![DeletionVectorReplacement {
                previous: Some(previous),
                current,
            }])
            .await?;
        fixture.env.metadata.heartbeat_task(&pinned.token).await?;
    }

    // MVCC removed_revision advances to Some(4), but visibility at target revision 3 is unchanged.
    let fixed = fixture
        .env
        .metadata
        .reconciliation_input(&pinned.token)
        .await?;
    assert_eq!(fixed.plan, pinned_input.plan);
    assert_eq!(fixed.removed_vectors, pinned_input.removed_vectors);
    insta::assert_debug_snapshot!(fixed.added_vectors, @r#"
    [
        VersionedDeletionVector {
            metadata: DeletionVectorMetadata {
                puffin_path: "memory://table/deletes/first-v2.puffin",
                target_file: "memory://table/data/first.parquet",
                cardinality: 2,
                file_size: 80,
                offset: 4,
                length: 20,
            },
            added_revision: 3,
            removed_revision: Some(
                4,
            ),
        },
    ]
    "#);

    let prepared_pinned = prepare_vectors(&fixture.published_table(), &fixed).await?;
    fixture
        .env
        .metadata
        .complete_reconciliation(&pinned.token, prepared_pinned)
        .await?;
    insta::assert_compact_debug_snapshot!(
        fixture.revisions().await?,
        @"Revisions { reconciled: 3, desired: 5 }"
    );
    assert!(fixture.debug_table().await?.prepared_commit.is_none());

    let (_, live_after_pinned) = live_manifest_files(&fixture.published_table()).await?;
    insta::assert_debug_snapshot!(live_after_pinned, @r#"
    [
        DeletionVectorMetadata {
            puffin_path: "memory://table/deletes/first-v2.puffin",
            target_file: "memory://table/data/first.parquet",
            cardinality: 2,
            file_size: 80,
            offset: 4,
            length: 20,
        },
        DeletionVectorMetadata {
            puffin_path: "memory://table/deletes/second-v1.puffin",
            target_file: "memory://table/data/second.parquet",
            cardinality: 1,
            file_size: 80,
            offset: 4,
            length: 20,
        },
    ]
    "#);

    // Next plan batches revisions 4 and 5: skips unpublished first-v3 and untouched baseline.
    let batched = fixture.claim_reconciliation(8).await?;
    let batched_input = fixture
        .env
        .metadata
        .reconciliation_input(&batched.token)
        .await?;
    insta::assert_debug_snapshot!(
        (
            batched_input.plan.base_revision,
            batched_input.plan.target_revision,
            batched_input.files.len(),
            &batched_input.added_vectors,
            &batched_input.removed_vectors,
        ),
        @r#"
    (
        3,
        5,
        0,
        [
            VersionedDeletionVector {
                metadata: DeletionVectorMetadata {
                    puffin_path: "memory://table/deletes/first-v4.puffin",
                    target_file: "memory://table/data/first.parquet",
                    cardinality: 4,
                    file_size: 80,
                    offset: 4,
                    length: 20,
                },
                added_revision: 5,
                removed_revision: None,
            },
        ],
        [
            VersionedDeletionVector {
                metadata: DeletionVectorMetadata {
                    puffin_path: "memory://table/deletes/first-v2.puffin",
                    target_file: "memory://table/data/first.parquet",
                    cardinality: 2,
                    file_size: 80,
                    offset: 4,
                    length: 20,
                },
                added_revision: 3,
                removed_revision: Some(
                    4,
                ),
            },
        ],
    )
    "#
    );

    let prepared_batched = prepare_vectors(&fixture.published_table(), &batched_input).await?;
    fixture
        .env
        .metadata
        .complete_reconciliation(&batched.token, prepared_batched)
        .await?;

    insta::assert_compact_debug_snapshot!(
        fixture.revisions().await?,
        @"Revisions { reconciled: 5, desired: 5 }"
    );
    assert!(fixture.debug_table().await?.prepared_commit.is_none());
    insta::assert_debug_snapshot!(fixture.env.server.commit_count(), @"4");

    let (_, final_vectors) = live_manifest_files(&fixture.published_table()).await?;
    insta::assert_debug_snapshot!(final_vectors, @r#"
    [
        DeletionVectorMetadata {
            puffin_path: "memory://table/deletes/first-v4.puffin",
            target_file: "memory://table/data/first.parquet",
            cardinality: 4,
            file_size: 80,
            offset: 4,
            length: 20,
        },
        DeletionVectorMetadata {
            puffin_path: "memory://table/deletes/second-v1.puffin",
            target_file: "memory://table/data/second.parquet",
            cardinality: 1,
            file_size: 80,
            offset: 4,
            length: 20,
        },
    ]
    "#);

    fixture.close().await?;

    Ok(())
}

#[tokio::test]
async fn reconciliation_completion_clears_previous_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = ReconciliationFixture::new().await?;
    fixture.materialize_files(&["a.parquet"]).await?;

    let first = fixture.claim_reconciliation(1).await?;
    fixture
        .env
        .metadata
        .fail_task(&first.token, se_metadata::TaskFailureCode::CatalogError)
        .await?;

    let failed_status = fixture
        .env
        .metadata
        .table_status(&fixture.catalog, fixture.table.metadata().uuid())
        .await?;
    insta::assert_yaml_snapshot!((&failed_status.health, &failed_status.ingestion.reconciliation), @r#"
    - degraded
    - state: retrying
      eligible_at: "1970-01-01T00:00:26Z"
      lease_expires_at: ~
      failure:
        code: catalog_error
        message: Failed to communicate with the Iceberg catalog.
        failed_at: "1970-01-01T00:00:21Z"
        consecutive_failures: 1
    "#);

    let second = fixture.claim_reconciliation(2).await?;
    let input = fixture
        .env
        .metadata
        .reconciliation_input(&second.token)
        .await?;
    let prepared = prepare_append(&fixture.published_table(), &input).await?;
    fixture
        .env
        .metadata
        .complete_reconciliation(&second.token, prepared)
        .await?;

    let recovered = fixture
        .env
        .metadata
        .table_status(&fixture.catalog, fixture.table.metadata().uuid())
        .await?;
    insta::assert_yaml_snapshot!(
        (
            &recovered.health,
            &recovered.ingestion.iceberg.reconciled_revision,
            &recovered.ingestion.reconciliation,
        ),
        @r#"
    - healthy
    - 1
    - state: idle
      eligible_at: ~
      lease_expires_at: ~
      failure: ~
    "#
    );

    fixture.close().await?;

    Ok(())
}

impl ReconciliationFixture {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let env = TestEnvironment::new().await?;
        let catalog = CatalogId::from(Uuid::from_u128(1));
        let table = test_table(
            TableIdent::from_strs(["test", "table"])?,
            Uuid::from_u128(42),
            FormatVersion::V3,
            HashMap::from([(CHECKPOINT_INTERVAL_PROPERTY.into(), "15".into())]),
        )?;
        env.server.put_table(table.clone());

        let config = CatalogConfig::Rest(RestCatalogConfig {
            uri: env.server.uri(),
            warehouse: None,
            properties: HashMap::new(),
        });
        env.metadata.create_catalog(catalog.clone(), config).await?;
        env.metadata
            .resolve_table(&catalog, table.identifier())
            .await?;

        Ok(Self {
            env,
            catalog,
            table,
        })
    }

    fn advance_clock(&self, delta_ms: i64) {
        self.env
            .clock
            .set(self.env.clock.now().timestamp_millis() + delta_ms);
    }

    fn published_table(&self) -> Table {
        self.env
            .server
            .table(self.table.identifier())
            .expect("table registered in mock catalog")
    }

    async fn debug_table(&self) -> Result<se_metadata::TableDebugState, se_metadata::Error> {
        self.env
            .metadata
            .debug_table(&self.catalog, self.table.metadata().uuid())
            .await
    }

    async fn revisions(&self) -> Result<Revisions, se_metadata::Error> {
        let debug = self.debug_table().await?;
        Ok(Revisions {
            reconciled: debug.state.reconciled_revision,
            desired: debug.state.desired_revision,
        })
    }

    async fn materialize_files(&self, names: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        self.materialize(
            names.iter().map(|name| sample_file(name)).collect(),
            Vec::new(),
        )
        .await
    }

    async fn materialize_vectors(
        &self,
        vectors: Vec<DeletionVectorReplacement>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.materialize(Vec::new(), vectors).await
    }

    async fn materialize(
        &self,
        data_files: Vec<DataFileMetadata>,
        deletion_vectors: Vec<DeletionVectorReplacement>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.env
            .metadata
            .wal_append(
                &self.catalog,
                self.table.metadata().uuid(),
                0,
                "wal",
                DateTime::UNIX_EPOCH,
                0,
            )
            .await?;
        self.advance_clock(15_000);
        let task = common::claim_foreground(&self.env.metadata, Uuid::from_u128(1))
            .await?
            .ok_or("missing materialization")?;
        let Task::Materialize {
            range, schema_id, ..
        } = task.task
        else {
            return Err("expected materialization task".into());
        };
        self.env
            .metadata
            .complete_materialization(
                &task.token,
                MaterializationResult {
                    range,
                    schema_id,
                    index_delta: None,
                    data_files,
                    deletion_vectors,
                },
            )
            .await?;

        Ok(())
    }

    async fn claim_reconciliation(
        &self,
        worker: u128,
    ) -> Result<TaskAssignment, Box<dyn std::error::Error>> {
        self.advance_clock(5_000);
        let task = common::claim_foreground(&self.env.metadata, Uuid::from_u128(worker))
            .await?
            .ok_or("missing reconciliation")?;
        assert!(matches!(task.task, Task::Reconcile { .. }));

        Ok(task)
    }

    async fn publish_data_files(&self, names: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        self.materialize_files(names).await?;
        let task = self.claim_reconciliation(100).await?;
        let input = self.env.metadata.reconciliation_input(&task.token).await?;
        let prepared = prepare_append(&self.published_table(), &input).await?;
        self.env
            .metadata
            .complete_reconciliation(&task.token, prepared)
            .await?;

        Ok(())
    }

    async fn publish_vectors(
        &self,
        vectors: Vec<DeletionVectorReplacement>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.materialize_vectors(vectors).await?;
        let task = self.claim_reconciliation(101).await?;
        let input = self.env.metadata.reconciliation_input(&task.token).await?;
        let prepared = prepare_vectors(&self.published_table(), &input).await?;
        self.env
            .metadata
            .complete_reconciliation(&task.token, prepared)
            .await?;

        Ok(())
    }

    async fn close(self) -> Result<(), se_metadata::Error> {
        self.env.metadata.close().await
    }
}

fn file_paths(input: &ReconciliationInput) -> Vec<String> {
    let mut paths: Vec<String> = input
        .files
        .iter()
        .map(|file| file.metadata.path.clone())
        .collect();
    paths.sort();
    paths
}

fn sample_file(name: &str) -> DataFileMetadata {
    DataFileMetadata {
        path: format!("memory://table/data/{name}.parquet"),
        format: DataFileFormat::Parquet,
        size: 100,
        num_rows: 10,
        partition_spec_id: 0,
        partition: BTreeMap::new(),
        sort_order_id: Some(0),
        column_sizes: BTreeMap::new(),
        value_counts: BTreeMap::new(),
        null_value_counts: BTreeMap::new(),
        nan_value_counts: BTreeMap::new(),
        lower_bounds: BTreeMap::new(),
        upper_bounds: BTreeMap::new(),
    }
}

fn sample_vector(target: &str, name: &str, cardinality: u64) -> DeletionVectorMetadata {
    DeletionVectorMetadata {
        puffin_path: format!("memory://table/deletes/{name}.puffin"),
        target_file: sample_file(target).path,
        cardinality,
        file_size: 80,
        offset: 4,
        length: 20,
    }
}

fn vector_file(vector: &DeletionVectorMetadata) -> Result<DataFile, Box<dyn std::error::Error>> {
    Ok(DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(vector.puffin_path.clone())
        .file_format(DataFileFormat::Puffin)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .record_count(vector.cardinality)
        .file_size_in_bytes(vector.file_size)
        .referenced_data_file(Some(vector.target_file.clone()))
        .content_offset(Some(vector.offset.try_into()?))
        .content_size_in_bytes(Some(vector.length.try_into()?))
        .build()?)
}

async fn prepare_append(
    table: &Table,
    input: &ReconciliationInput,
) -> Result<PreparedCommit, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    for file in &input.files {
        files.push(
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(file.metadata.path.clone())
                .file_format(file.metadata.format)
                .partition(Struct::empty())
                .partition_spec_id(0)
                .record_count(file.metadata.num_rows)
                .file_size_in_bytes(file.metadata.size)
                .build()?,
        );
    }

    let mut action = Transaction::new(table)
        .fast_append()
        .set_commit_uuid(input.plan.id)
        .set_snapshot_properties(HashMap::from([
            (RECONCILIATION_ID_PROPERTY.into(), input.plan.id.to_string()),
            (
                REVISION_PROPERTY.into(),
                input.plan.target_revision.to_string(),
            ),
        ]))
        .add_data_files(files)
        .prepare(table)
        .await?;

    let mut requirements = action.take_requirements();
    requirements.push(TableRequirement::CurrentSchemaIdMatch {
        current_schema_id: table.metadata().current_schema_id(),
    });

    Ok(PreparedCommit {
        updates: action.take_updates(),
        requirements,
    })
}

async fn prepare_vectors(
    table: &Table,
    input: &ReconciliationInput,
) -> Result<PreparedCommit, Box<dyn std::error::Error>> {
    let prepared = se_iceberg::publication::prepare_upsert(
        table,
        input.plan.id,
        Vec::new(),
        input
            .added_vectors
            .iter()
            .map(|vector| vector_file(&vector.metadata))
            .collect::<Result<Vec<_>, _>>()?,
        input
            .removed_vectors
            .iter()
            .map(|vector| vector_file(&vector.metadata))
            .collect::<Result<Vec<_>, _>>()?,
        HashMap::from([
            (RECONCILIATION_ID_PROPERTY.into(), input.plan.id.to_string()),
            (
                REVISION_PROPERTY.into(),
                input.plan.target_revision.to_string(),
            ),
        ]),
    )
    .await?;

    Ok(PreparedCommit {
        updates: prepared.updates,
        requirements: vec![
            TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".into(),
                snapshot_id: input.plan.base_snapshot_id,
            },
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: table.metadata().current_schema_id(),
            },
        ],
    })
}

async fn live_manifest_files(
    table: &Table,
) -> Result<(Vec<String>, Vec<DeletionVectorMetadata>), Box<dyn std::error::Error>> {
    let snapshot = table
        .metadata()
        .current_snapshot()
        .ok_or("missing snapshot")?;
    let manifests = table.manifest_list_reader(snapshot).load().await?;
    let mut data_files = Vec::new();
    let mut vectors = Vec::new();

    for manifest in manifests.entries() {
        let manifest = manifest.load_manifest(table.file_io()).await?;
        for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            let file = entry.data_file();
            if file.content_type() == DataContentType::Data {
                data_files.push(file.file_path().to_owned());
            } else {
                assert_eq!(file.content_type(), DataContentType::PositionDeletes);
                assert_eq!(file.file_format(), DataFileFormat::Puffin);
                vectors.push(DeletionVectorMetadata {
                    puffin_path: file.file_path().to_owned(),
                    target_file: file
                        .referenced_data_file()
                        .ok_or("missing target")?
                        .to_owned(),
                    cardinality: file.record_count(),
                    file_size: file.file_size_in_bytes(),
                    offset: file.content_offset().ok_or("missing offset")?.try_into()?,
                    length: file
                        .content_size_in_bytes()
                        .ok_or("missing length")?
                        .try_into()?,
                });
            }
        }
    }

    data_files.sort();
    vectors.sort_by(|left, right| left.target_file.cmp(&right.target_file));

    Ok((data_files, vectors))
}
