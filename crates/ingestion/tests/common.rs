use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use arrow_array::{RecordBatch, StringArray};
use async_trait::async_trait;
use iceberg::{
    NamespaceIdent, TableIdent,
    spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
        TableMetadata as IcebergMetadata, TableMetadataBuilder, Type,
    },
};
use object_store::{ObjectStore, memory::InMemory};
use se_common::{clock::MockSystemClock, id::IdGenerator, rand::SeRand};
use se_ingestion::Ingestor;
use se_metadata::CatalogId;
use se_metadata_client::{
    CatalogConfig, Heartbeat, MaterializationResult, MetadataClient, TaskAssignment, TaskToken,
};
use se_wal::{
    BatchCommit, BatchDescriptor, BatchMetadata, BatchRead, InMemoryWal, test_util::BlockedCommit,
};
use uuid::Uuid;

pub struct TestContext {
    pub metadata: IcebergMetadata,
    pub store: Arc<dyn ObjectStore>,
    pub clock: Arc<MockSystemClock>,
    pub ingestor: Ingestor,
    pub client: Arc<TestMetadata>,
}

pub struct TestMetadata {
    metadata: IcebergMetadata,
    wals: Mutex<HashMap<(CatalogId, Uuid), InMemoryWal>>,
    blocked: Option<Arc<BlockedCommit>>,
}

pub fn catalog() -> CatalogId {
    CatalogId::new("catalog").unwrap()
}

pub fn namespace() -> NamespaceIdent {
    NamespaceIdent::from_vec(vec!["namespace".into(), "nested".into()]).unwrap()
}

pub fn batch(metadata: &IcebergMetadata, values: Vec<&str>) -> RecordBatch {
    let schema = iceberg::arrow::schema_to_arrow_schema(metadata.current_schema()).unwrap();
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(StringArray::from(values))]).unwrap()
}

impl Default for TestContext {
    fn default() -> Self {
        let rng = Arc::new(SeRand::new(42));
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "legacy",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .unwrap();
        let current_schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "value",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "s3://bucket/table".into(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .assign_uuid(rng.rng().gen_uuid())
        .add_current_schema(current_schema)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let client = Arc::new(TestMetadata::new(metadata.clone(), None));
        let store = Arc::new(InMemory::new());
        let clock = Arc::new(MockSystemClock::with_time(1_700_000_000_000));
        let ingestor = Ingestor::new(client.clone(), store.clone(), clock.clone(), rng);

        Self {
            metadata,
            store,
            clock,
            ingestor,
            client,
        }
    }
}

impl TestContext {
    pub fn wal(&self, catalog: CatalogId) -> InMemoryWal {
        self.client.wals.lock().unwrap()[&(catalog, self.metadata.uuid())].clone()
    }
}

impl TestMetadata {
    pub fn new(metadata: IcebergMetadata, blocked: Option<Arc<BlockedCommit>>) -> Self {
        Self {
            metadata,
            wals: Mutex::new(HashMap::new()),
            blocked,
        }
    }
}

#[async_trait]
impl MetadataClient for TestMetadata {
    async fn get_catalog(
        &self,
        _: &CatalogId,
    ) -> se_metadata_client::Result<Option<CatalogConfig>> {
        // PANIC: Ingestion does not fetch catalog configuration.
        unreachable!("ingestion does not fetch catalog configuration")
    }

    async fn resolve_table(
        &self,
        catalog: &CatalogId,
        table: &TableIdent,
    ) -> se_metadata_client::Result<IcebergMetadata> {
        if table.namespace() != &namespace() || table.name() != "table" {
            return Err(se_metadata_client::Error::TableNotFound(table.clone()));
        }

        self.wals
            .lock()
            .unwrap()
            .entry((catalog.clone(), self.metadata.uuid()))
            .or_default();

        Ok(self.metadata.clone())
    }

    async fn claim_task(&self, _: Uuid) -> se_metadata_client::Result<Option<TaskAssignment>> {
        // PANIC: Ingestion does not claim tasks.
        unreachable!("ingestion does not claim tasks")
    }

    fn heartbeat_task(&self, _: TaskToken) -> Heartbeat {
        // PANIC: Ingestion does not heartbeat tasks.
        unreachable!("ingestion does not heartbeat tasks")
    }

    async fn complete_materialization(
        &self,
        _: TaskToken,
        _: MaterializationResult,
    ) -> se_metadata_client::Result<()> {
        // PANIC: Ingestion does not complete materialization tasks.
        unreachable!("ingestion does not complete materialization tasks")
    }

    async fn fail_task(&self, _: TaskToken) -> se_metadata_client::Result<()> {
        // PANIC: Ingestion does not fail tasks.
        unreachable!("ingestion does not fail tasks")
    }

    async fn materialization_input(
        &self,
        _: TaskToken,
        _: u64,
    ) -> se_metadata_client::Result<se_metadata_client::MaterializationInput> {
        // PANIC: Ingestion does not materialize tables.
        unreachable!("ingestion does not materialize tables")
    }

    async fn reconciliation_input(
        &self,
        _: TaskToken,
    ) -> se_metadata_client::Result<se_metadata_client::ReconciliationInput> {
        // PANIC: Ingestion does not reconcile tables.
        unreachable!("ingestion does not reconcile tables")
    }

    async fn complete_reconciliation(
        &self,
        _: TaskToken,
        _: se_metadata_client::PreparedCommit,
    ) -> se_metadata_client::Result<()> {
        // PANIC: Ingestion does not reconcile tables.
        unreachable!("ingestion does not reconcile tables")
    }

    async fn wal_append(
        &self,
        catalog: &CatalogId,
        uuid: Uuid,
        batch: BatchMetadata,
    ) -> se_wal::Result<()> {
        if let Some(blocked) = &self.blocked {
            return blocked.commit(batch).await;
        }

        let wal = self.wals.lock().unwrap()[&(catalog.clone(), uuid)].clone();
        wal.commit(batch).await
    }

    async fn wal_read_after(
        &self,
        catalog: &CatalogId,
        uuid: Uuid,
        sequence: Option<u64>,
        max: usize,
    ) -> se_wal::Result<Vec<BatchDescriptor>> {
        let wal = self.wals.lock().unwrap()[&(catalog.clone(), uuid)].clone();
        wal.read_after(sequence, max).await
    }
}
