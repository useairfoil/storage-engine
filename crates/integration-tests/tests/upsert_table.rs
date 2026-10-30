use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray};
use iceberg::{
    TableCreation,
    spec::{NestedField, PrimitiveType, Schema, Type},
};
use se_integration_tests::{TestEnvironment, query_rows, snapshot_shape};

#[tokio::test]
async fn reconciles_two_upsert_batches() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::from_env("upsert_table").await?;
    let result: Result<(), Box<dyn std::error::Error>> = async {
        let ident = env.table_ident("users");
        let schema = Schema::builder()
            .with_fields([
                NestedField::required(1, "email", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .with_identifier_field_ids([1])
            .build()?;

        let table = env
            .catalog
            .create_table(
                &ident.namespace,
                TableCreation::builder()
                    .name(ident.name.clone())
                    .schema(schema)
                    .properties([
                        ("format-version".to_owned(), "3".to_owned()),
                        ("airfoil.checkpoint.interval-sec".to_owned(), "0".to_owned()),
                    ])
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
                Arc::new(StringArray::from(vec!["carol@example.com", "alice@example.com", "grace@example.com"])),
                Arc::new(StringArray::from(vec!["Carol", "Alice", "Grace"])),
            ],
        )?;
        ingestor.write(batch).await?;
        env.wait_for_publication(&table).await?;

        // The current query reader supports data files but not Puffin deletion vectors.
        let first = env.catalog.load_table(&ident).await?;

        insta::assert_yaml_snapshot!(snapshot_shape(&first).await?, @"
        snapshot:
          manifest_list:
            manifests:
              - content: Data
                entries:
                  - data_file:
                      content: Data
                      format: parquet
                      path: data-1.parquet
                      rows: 3
                    sequence_number: 1
                    status: Added
                files:
                  added: 1
                  deleted: 0
                  existing: 0
                sequence_number: 1
          operation: append
          sequence_number: 1
        ");

        insta::assert_snapshot!(query_rows(first, "SELECT email, name FROM users ORDER BY email").await?, @"
        +-------------------+-------+
        | email             | name  |
        +-------------------+-------+
        | alice@example.com | Alice |
        | carol@example.com | Carol |
        | grace@example.com | Grace |
        +-------------------+-------+
        ");

        let batch = RecordBatch::try_new(
            ingestor.table_schema(),
            vec![
                Arc::new(StringArray::from(vec!["alice@example.com", "ivan@example.com"])),
                Arc::new(StringArray::from(vec!["Alice Updated", "Ivan"])),
            ],
        )?;
        ingestor.write(batch).await?;
        env.wait_for_publication(&table).await?;

        let published = env.catalog.load_table(&ident).await?;

        insta::assert_yaml_snapshot!(snapshot_shape(&published).await?, @"
        snapshot:
          manifest_list:
            manifests:
              - content: Data
                entries:
                  - data_file:
                      content: Data
                      format: parquet
                      path: data-1.parquet
                      rows: 3
                    sequence_number: 1
                    status: Added
                files:
                  added: 1
                  deleted: 0
                  existing: 0
                sequence_number: 1
              - content: Data
                entries:
                  - data_file:
                      content: Data
                      format: parquet
                      path: data-2.parquet
                      rows: 2
                    sequence_number: 2
                    status: Added
                files:
                  added: 1
                  deleted: 0
                  existing: 0
                sequence_number: 2
              - content: Deletes
                entries:
                  - data_file:
                      content: PositionDeletes
                      format: puffin
                      path: delete-1.puffin
                      referenced_data_file: data-1.parquet
                      rows: 1
                    sequence_number: 2
                    status: Added
                files:
                  added: 1
                  deleted: 0
                  existing: 0
                sequence_number: 2
          operation: overwrite
          sequence_number: 2
        ");

        // TODO: Enable once iceberg-datafusion supports Puffin deletion vectors.
        // insta::assert_snapshot!(query_rows(published, "SELECT email, name FROM users ORDER BY email").await?, @"
        // +-------------------+---------------+
        // | email             | name          |
        // +-------------------+---------------+
        // | alice@example.com | Alice Updated |
        // | carol@example.com | Carol         |
        // | grace@example.com | Grace         |
        // | ivan@example.com  | Ivan          |
        // +-------------------+---------------+
        // ");
        Ok(())
    }
    .await;

    let closed = env.close().await;

    result?;
    closed?;

    Ok(())
}
