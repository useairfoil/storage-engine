#![allow(dead_code)]

use std::{collections::HashMap, sync::Arc};

use iceberg::{TableIdent, spec::FormatVersion};
use object_store::memory::InMemory;
use se_common::clock::MockSystemClock;
use se_iceberg::testing::{MockRestCatalog, test_table};
use se_metadata::{
    CHECKPOINT_INTERVAL_PROPERTY, CatalogConfig, CatalogId, Error, MaterializationResult, Metadata,
    RestCatalogConfig, Task, TaskAssignment,
};
use se_secret_store::testing::MockSecretStore;
use slatedb::Db;
use uuid::Uuid;

pub struct TestEnvironment {
    pub store: Arc<InMemory>,
    pub db: Db,
    pub secrets: Arc<MockSecretStore>,
    pub clock: Arc<MockSystemClock>,
    pub server: MockRestCatalog,
    pub metadata: Metadata,
}

/// Resolves a test table through the provided mock Iceberg REST catalog.
pub async fn resolve(
    metadata: &Metadata,
    server: &MockRestCatalog,
    catalog: &CatalogId,
    uuid: Uuid,
) -> Result<TableIdent, Box<dyn std::error::Error>> {
    resolve_with_properties(
        metadata,
        server,
        catalog,
        uuid,
        HashMap::from([(CHECKPOINT_INTERVAL_PROPERTY.into(), "15".into())]),
    )
    .await
}

/// Resolves a test table with custom Iceberg properties through the provided mock catalog.
pub async fn resolve_with_properties(
    metadata: &Metadata,
    server: &MockRestCatalog,
    catalog: &CatalogId,
    uuid: Uuid,
    properties: HashMap<String, String>,
) -> Result<TableIdent, Box<dyn std::error::Error>> {
    let ident = TableIdent::from_strs(["test", &uuid.to_string()])?;
    let table = test_table(ident.clone(), uuid, FormatVersion::V2, properties)?;
    server.put_table(table);

    if metadata.get_catalog(catalog).await?.is_none() {
        let config = CatalogConfig::Rest(RestCatalogConfig {
            uri: server.uri(),
            warehouse: None,
            properties: HashMap::new(),
        });
        metadata.create_catalog(catalog.clone(), config).await?;
    }

    assert_eq!(metadata.resolve_table(catalog, &ident).await?.uuid(), uuid);

    Ok(ident)
}

/// Completes independent WAL cleanup before returning foreground work.
pub async fn claim_foreground(
    metadata: &Metadata,
    worker: Uuid,
) -> Result<Option<TaskAssignment>, se_metadata::Error> {
    loop {
        let assignment = metadata.claim_task(worker).await?;

        match assignment {
            Some(assignment) if matches!(assignment.task, Task::GarbageCollectWal { .. }) => {
                metadata.complete_wal_cleanup(&assignment.token).await?;
            }
            assignment => return Ok(assignment),
        }
    }
}

/// Builds an empty materialization result matching the supplied assignment.
pub fn empty_materialization_result(
    assignment: &TaskAssignment,
) -> Result<MaterializationResult, Error> {
    let Task::Materialize {
        range, schema_id, ..
    } = assignment.task
    else {
        return Err(Error::InvalidTaskResult);
    };

    Ok(MaterializationResult {
        range,
        schema_id,
        index_delta: None,
        data_files: Vec::new(),
        deletion_vectors: Vec::new(),
    })
}

/// Completes a materialization assignment with no data files or deletion vectors.
pub async fn complete_empty_materialization(
    metadata: &Metadata,
    assignment: &TaskAssignment,
) -> Result<(), Error> {
    let result = empty_materialization_result(assignment)?;
    metadata
        .complete_materialization(&assignment.token, result)
        .await
}

impl TestEnvironment {
    /// Creates an isolated test environment with its mock clock initialized to `1000` ms.
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_time(1_000).await
    }

    /// Creates an isolated test environment with its mock clock initialized to `timestamp_ms`.
    pub async fn with_time(timestamp_ms: i64) -> Result<Self, Box<dyn std::error::Error>> {
        let store = Arc::new(InMemory::new());
        let db = Db::open("metadata", store.clone()).await?;
        let secrets = Arc::new(MockSecretStore::new());
        let clock = Arc::new(MockSystemClock::with_time(timestamp_ms));
        let server = MockRestCatalog::start().await?;
        let metadata = Metadata::from_db(db.clone(), secrets.clone(), clock.clone());

        Ok(Self {
            store,
            db,
            secrets,
            clock,
            server,
            metadata,
        })
    }

    /// Resolves a test table through this environment's mock catalog.
    pub async fn resolve(
        &self,
        catalog: &CatalogId,
        uuid: Uuid,
    ) -> Result<TableIdent, Box<dyn std::error::Error>> {
        resolve(&self.metadata, &self.server, catalog, uuid).await
    }

    /// Resolves a test table with custom Iceberg properties through this environment's mock catalog.
    pub async fn resolve_with_properties(
        &self,
        catalog: &CatalogId,
        uuid: Uuid,
        properties: HashMap<String, String>,
    ) -> Result<TableIdent, Box<dyn std::error::Error>> {
        resolve_with_properties(&self.metadata, &self.server, catalog, uuid, properties).await
    }

    /// Closes and reopens the metadata database against the same object store, secrets, and clock.
    pub async fn reopen(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.metadata.close().await?;
        let db = Db::open("metadata", self.store.clone()).await?;
        self.metadata = Metadata::from_db(db.clone(), self.secrets.clone(), self.clock.clone());
        self.db = db;

        Ok(())
    }
}
