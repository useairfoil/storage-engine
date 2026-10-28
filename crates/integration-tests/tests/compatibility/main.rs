use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use iceberg::{
    TableCreation,
    spec::{DataContentType, NestedField, PrimitiveType, Schema, Type},
};
use se_integration_tests::TestEnvironment;

#[tokio::test]
async fn reconciles_a_durable_batch() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::from_env().await?;
    let result: Result<(), Box<dyn std::error::Error>> = async {
        let ident = env.table_ident("events");
        let schema = Schema::builder()
            .with_fields([
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "value", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?;

        let table = env
            .catalog
            .create_table(
                &ident.namespace,
                TableCreation::builder()
                    .name(ident.name.clone())
                    .schema(schema)
                    .properties([("format-version".to_owned(), "3".to_owned())])
                    .build(),
            )
            .await?;

        let ingestor = env
            .ingestor
            .for_table(
                env.catalog_id.clone(),
                ident.namespace.clone(),
                ident.name.clone(),
            )
            .await?;

        let batch = RecordBatch::try_new(
            ingestor.table_schema(),
            vec![
                Arc::new(Int64Array::from(vec![3, 1, 7])),
                Arc::new(StringArray::from(vec!["three", "one", "seven"])),
            ],
        )?;

        // Acknowledgment means WAL durability; the worker materializes and publishes separately.
        ingestor.write(batch).await?;

        env.wait_for_publication(&table).await?;

        let published = env.catalog.load_table(&ident).await?;
        let snapshot = published
            .metadata()
            .current_snapshot()
            .ok_or("missing published snapshot")?;

        let manifests = published.manifest_list_reader(snapshot).load().await?;
        let mut rows = 0;
        for manifest in manifests.entries() {
            let manifest = manifest.load_manifest(published.file_io()).await?;
            for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
                assert_eq!(entry.data_file().content_type(), DataContentType::Data);
                rows += entry.data_file().record_count();
            }
        }

        assert_eq!(rows, 3);

        Ok(())
    }
    .await;

    let closed = env.close().await;

    result?;
    closed?;

    Ok(())
}
