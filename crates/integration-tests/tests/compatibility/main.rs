use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use iceberg::{
    TableCreation,
    spec::{NestedField, PrimitiveType, Schema, Type},
};
use se_integration_tests::TestEnvironment;

#[tokio::test]
async fn reconciles_a_durable_batch() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::from_env().await?;
    let ident = env.table_ident("events");

    let catalog = env
        .catalog_store
        .get(env.catalog_name.clone())
        .await?
        .ok_or("test catalog not found")?
        .to_catalog()
        .await?;

    let schema = Schema::builder()
        .with_fields([
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    catalog
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
            env.catalog_name.clone(),
            ident.namespace.clone(),
            ident.name.clone(),
        )
        .await?;

    let reconciler = env
        .reconciler
        .for_table(env.catalog_name.clone(), ident.namespace, ident.name)
        .await?;

    let batch = RecordBatch::try_new(
        ingestor.table_schema(),
        vec![
            Arc::new(Int64Array::from(vec![3, 1, 7])),
            Arc::new(StringArray::from(vec!["three", "one", "seven"])),
        ],
    )?;

    let result: Result<(), Box<dyn std::error::Error>> = async {
        // A successful write acknowledgement means the batch is durable in the WAL.
        ingestor.write(batch).await?;
        reconciler.run_reconciliation().await?;
        Ok(())
    }
    .await;

    let ingestion_close = env.ingestor.close().await;
    let table_close = reconciler.table().clone().flush_and_close().await;

    result?;
    ingestion_close?;
    table_close?;

    Ok(())
}
