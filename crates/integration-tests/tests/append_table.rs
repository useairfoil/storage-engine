use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use iceberg::{
    TableCreation,
    spec::{NestedField, PrimitiveType, Schema, Type},
};
use se_integration_tests::{TestEnvironment, query_rows, snapshot_shape};

#[tokio::test]
async fn reconciles_two_append_batches() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::from_env("append_table").await?;
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
                Arc::new(Int64Array::from(vec![3, 1, 7])),
                Arc::new(StringArray::from(vec!["three", "one", "seven"])),
            ],
        )?;

        // Acknowledgment means WAL durability; the worker materializes and publishes separately.
        ingestor.write(batch).await?;

        env.wait_for_publication(&table).await?;

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

        let batch = RecordBatch::try_new(
            ingestor.table_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 9])),
                Arc::new(StringArray::from(vec!["one again", "nine"])),
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
          operation: append
          sequence_number: 2
        ");

        insta::assert_snapshot!(query_rows(published, "SELECT id, value FROM events ORDER BY id, value").await?, @"
        +----+-----------+
        | id | value     |
        +----+-----------+
        | 1  | one       |
        | 1  | one again |
        | 3  | three     |
        | 7  | seven     |
        | 9  | nine      |
        +----+-----------+
        ");

        Ok(())
    }
    .await;

    let closed = env.close().await;

    result?;
    closed?;

    Ok(())
}
