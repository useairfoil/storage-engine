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
use se_wal::{InMemoryWal, WalClient, WalClientFactory, schema::TableMetadata};
use uuid::Uuid;

pub struct TestContext {
    pub metadata: IcebergMetadata,
    pub store: Arc<dyn ObjectStore>,
    pub clock: Arc<MockSystemClock>,
    pub wals: Arc<TestWalFactory>,
    pub ingestor: Ingestor,
}

pub struct FixedMetadata(pub IcebergMetadata);

#[derive(Default)]
pub struct TestWalFactory(Mutex<HashMap<(CatalogId, Uuid), InMemoryWal>>);

pub fn catalog() -> CatalogId {
    CatalogId::new("catalog").unwrap()
}

pub fn namespace() -> NamespaceIdent {
    NamespaceIdent::new("namespace".into())
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
        let wals = Arc::new(TestWalFactory::default());
        let store = Arc::new(InMemory::new());
        let clock = Arc::new(MockSystemClock::with_time(1_700_000_000_000));
        let ingestor = Ingestor::new(
            Arc::new(FixedMetadata(metadata.clone())),
            wals.clone(),
            store.clone(),
            clock.clone(),
            rng,
        );

        Self {
            metadata,
            store,
            clock,
            wals,
            ingestor,
        }
    }
}

impl TestContext {
    pub fn wal(&self, catalog: CatalogId) -> InMemoryWal {
        self.wals.0.lock().unwrap()[&(catalog, self.metadata.uuid())].clone()
    }
}

#[async_trait]
impl TableMetadata for FixedMetadata {
    async fn load_table(
        &self,
        _: CatalogId,
        table: TableIdent,
    ) -> iceberg::Result<IcebergMetadata> {
        if table.name() == "missing" {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::TableNotFound,
                "missing table",
            ));
        }

        Ok(self.0.clone())
    }
}

impl WalClientFactory for TestWalFactory {
    fn wal_client(&self, catalog_id: CatalogId, table_uuid: Uuid) -> Arc<dyn WalClient> {
        Arc::new(
            self.0
                .lock()
                .unwrap()
                .entry((catalog_id, table_uuid))
                .or_default()
                .clone(),
        )
    }
}
