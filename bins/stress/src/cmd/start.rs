use std::{num::NonZeroU64, ops::RangeInclusive, sync::Arc};

use clap::Args;
use rand::rngs::StdRng;
use se_common::{clock::DefaultSystemClock, id::IdGenerator};
use se_meta_store::catalog::{IcebergError, NamespaceIdent};
use thiserror::Error;
use tokio::task::JoinSet;
use tracing::{debug, info};

use crate::{
    catalog, flight,
    r#gen::{DataGenerator, Mode, SortColumn},
    helpers::{parse_batch_size_range, parse_row_data_size_range},
};

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Catalog(#[from] catalog::Error),
    #[error("iceberg error: {0}")]
    Iceberg(#[from] IcebergError),
    #[error(transparent)]
    Flight(#[from] flight::Error),
    #[error("upload task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error("failed to listen for shutdown: {0}")]
    Signal(#[from] std::io::Error),
}

#[derive(Debug, Args)]
pub struct StartArgs {
    /// Base URL of the storage engine HTTP API.
    #[arg(long, default_value = "http://localhost:7777")]
    engine_url: String,
    /// ID of the catalog containing the table.
    #[arg(long)]
    catalog_id: String,
    /// Dot-separated namespace containing the table (e.g. sales.eu).
    #[arg(long, value_parser = parse_namespace)]
    namespace: NamespaceIdent,
    /// Table name; a random name will be used when omitted.
    #[arg(long)]
    table: Option<String>,
    /// Whether the generated table schema has identifier fields.
    #[arg(long, value_enum, default_value = "upsert")]
    mode: Mode,
    /// Column to sort the table by (ascending); omitted for an unsorted table.
    #[arg(long, value_enum)]
    sort: Option<SortColumn>,
    /// Number of distinct keys to draw from.
    #[arg(long, default_value = "1000")]
    keys_cardinality: NonZeroU64,
    /// Size of the variable data column, e.g. 1MiB..10MiB or 5MiB.
    #[arg(long, default_value = "1KiB", value_parser = parse_row_data_size_range)]
    row_data_size_range: RangeInclusive<u64>,
    /// Rows per batch, e.g. 1..10 or 5.
    #[arg(long, default_value = "100", value_parser = parse_batch_size_range)]
    batch_size_range: RangeInclusive<u64>,
}

impl StartArgs {
    pub async fn run(self) -> Result<(), Error> {
        let catalog = catalog::load(&self.engine_url, &self.catalog_id).await?;
        debug!(namespace = %self.namespace, "checking namespace");
        if !catalog.namespace_exists(&self.namespace).await? {
            return Err(catalog::Error::NamespaceNotFound(self.namespace.to_string()).into());
        }
        debug!(namespace = %self.namespace, "namespace exists");
        let generator = DataGenerator::new(
            self.mode,
            self.sort,
            self.keys_cardinality,
            self.row_data_size_range,
            self.batch_size_range,
            Arc::new(DefaultSystemClock::new()),
            rand::make_rng::<StdRng>(),
        )?;
        let table_name = self
            .table
            .unwrap_or_else(|| format!("stress_{}", rand::rng().gen_uuid().simple()));
        let _table = catalog::ensure_table(
            catalog.as_ref(),
            &self.namespace,
            &table_name,
            generator.schema(),
            generator.sort_order(),
        )
        .await?;
        debug!(table = %table_name, "table ready for ingestion");
        let client = flight::IngestorClient::connect(
            &self.engine_url,
            &self.catalog_id,
            &self.namespace,
            &table_name,
            generator.arrow_schema(),
        )
        .await?;
        let (task, publisher) = client.start();
        info!(table = %table_name, "pushing batches; press Ctrl+C to stop");

        let shutdown = tokio::signal::ctrl_c();
        tokio::pin!(shutdown);
        let mut publications = JoinSet::new();
        let mut failure = loop {
            while publications.len() < 32 {
                publications.spawn(publisher.publish(generator.next_batch()));
            }
            tokio::select! {
                biased;
                result = &mut shutdown => break result.err().map(Error::from),
                result = publications.join_next() => {
                    match result {
                        Some(Ok(Ok(()))) => {}
                        Some(Ok(Err(error))) => break Some(error.into()),
                        Some(Err(error)) => break Some(error.into()),
                        None => break None,
                    }
                }
            }
        };

        debug!(
            in_flight = publications.len(),
            "stopping generation and draining batches"
        );
        drop(publisher);
        while let Some(result) = publications.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) if failure.is_none() => failure = Some(error.into()),
                Err(error) if failure.is_none() => failure = Some(error.into()),
                _ => {}
            }
        }
        task.await??;
        if let Some(error) = failure {
            return Err(error);
        }
        info!(table = %table_name, "all pending batches published");
        Ok(())
    }
}

fn parse_namespace(value: &str) -> Result<NamespaceIdent, IcebergError> {
    NamespaceIdent::from_strs(value.split('.'))
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::Cli;

    #[test]
    fn parses_all_arguments() {
        let cli = Cli::try_parse_from([
            "se-stress",
            "start",
            "--engine-url",
            "http://127.0.0.1:8888",
            "--catalog-id",
            "cat",
            "--namespace",
            "sales.eu",
            "--table",
            "orders",
            "--mode",
            "append",
            "--sort",
            "key",
            "--keys-cardinality",
            "20",
            "--row-data-size-range",
            "1MiB..10MiB",
            "--batch-size-range",
            "2..9",
        ])
        .unwrap();

        insta::assert_debug_snapshot!(cli, @r#"
        Cli {
            command: Start(
                StartArgs {
                    engine_url: "http://127.0.0.1:8888",
                    catalog_id: "cat",
                    namespace: NamespaceIdent(
                        [
                            "sales",
                            "eu",
                        ],
                    ),
                    table: Some(
                        "orders",
                    ),
                    mode: Append,
                    sort: Some(
                        Key,
                    ),
                    keys_cardinality: 20,
                    row_data_size_range: 1048576..=10485760,
                    batch_size_range: 2..=9,
                },
            ),
        }
        "#);
    }

    #[test]
    fn parses_defaults() {
        let cli = Cli::try_parse_from([
            "se-stress",
            "start",
            "--catalog-id",
            "cat",
            "--namespace",
            "ns",
        ])
        .unwrap();

        insta::assert_debug_snapshot!(cli, @r#"
        Cli {
            command: Start(
                StartArgs {
                    engine_url: "http://localhost:7777",
                    catalog_id: "cat",
                    namespace: NamespaceIdent(
                        [
                            "ns",
                        ],
                    ),
                    table: None,
                    mode: Upsert,
                    sort: None,
                    keys_cardinality: 1000,
                    row_data_size_range: 1024..=1024,
                    batch_size_range: 100..=100,
                },
            ),
        }
        "#);
    }

    #[test]
    fn parses_fixed_ranges() {
        let cli = Cli::try_parse_from([
            "se-stress",
            "start",
            "--catalog-id",
            "cat",
            "--namespace",
            "ns",
            "--row-data-size-range",
            "5MiB",
            "--batch-size-range",
            "5",
        ])
        .unwrap();

        insta::assert_debug_snapshot!(cli, @r#"
        Cli {
            command: Start(
                StartArgs {
                    engine_url: "http://localhost:7777",
                    catalog_id: "cat",
                    namespace: NamespaceIdent(
                        [
                            "ns",
                        ],
                    ),
                    table: None,
                    mode: Upsert,
                    sort: None,
                    keys_cardinality: 1000,
                    row_data_size_range: 5242880..=5242880,
                    batch_size_range: 5..=5,
                },
            ),
        }
        "#);
    }

    #[test]
    fn parses_decimal_and_binary_size_units() {
        let cli = Cli::try_parse_from([
            "se-stress",
            "start",
            "--catalog-id",
            "cat",
            "--namespace",
            "ns",
            "--row-data-size-range",
            "1MB..1.5MiB",
        ])
        .unwrap();

        insta::assert_debug_snapshot!(cli, @r#"
        Cli {
            command: Start(
                StartArgs {
                    engine_url: "http://localhost:7777",
                    catalog_id: "cat",
                    namespace: NamespaceIdent(
                        [
                            "ns",
                        ],
                    ),
                    table: None,
                    mode: Upsert,
                    sort: None,
                    keys_cardinality: 1000,
                    row_data_size_range: 1000000..=1572864,
                    batch_size_range: 100..=100,
                },
            ),
        }
        "#);
    }

    #[test]
    fn parses_modes_and_rejects_unknown_mode() {
        for value in ["append", "upsert", "invalid"] {
            let result = Cli::try_parse_from([
                "se-stress",
                "start",
                "--catalog-id",
                "cat",
                "--namespace",
                "ns",
                "--mode",
                value,
            ]);
            match value {
                "append" | "upsert" => {
                    let crate::Command::Start(args) = result.unwrap().command;
                    assert_eq!(
                        args.mode,
                        if value == "append" {
                            super::Mode::Append
                        } else {
                            super::Mode::Upsert
                        }
                    );
                }
                _ => assert_eq!(
                    result.unwrap_err().kind(),
                    clap::error::ErrorKind::InvalidValue
                ),
            }
        }
    }

    #[test]
    fn parses_sort_columns_and_rejects_unknown_column() {
        for (value, expected) in [
            ("key", Some(super::SortColumn::Key)),
            ("ts", Some(super::SortColumn::Ts)),
            ("data", None),
        ] {
            let result = Cli::try_parse_from([
                "se-stress",
                "start",
                "--catalog-id",
                "cat",
                "--namespace",
                "ns",
                "--sort",
                value,
            ]);
            if let Some(expected) = expected {
                let crate::Command::Start(args) = result.unwrap().command;
                assert_eq!(args.sort, Some(expected));
            } else {
                assert_eq!(
                    result.unwrap_err().kind(),
                    clap::error::ErrorKind::InvalidValue
                );
            }
        }
    }

    #[test]
    fn rejects_missing_required_arguments() {
        let error = Cli::try_parse_from(["se-stress", "start", "--catalog-id", "cat"]).unwrap_err();
        insta::assert_snapshot!(error.to_string(), @"
        error: the following required arguments were not provided:
          --namespace <NAMESPACE>

        Usage: se-stress start --catalog-id <CATALOG_ID> --namespace <NAMESPACE>

        For more information, try '--help'.
        ");
    }

    #[test]
    fn rejects_invalid_ranges() {
        let mut errors = Vec::new();
        for (flag, value) in [
            ("--batch-size-range", "0"),
            ("--batch-size-range", "9..2"),
            ("--batch-size-range", "1..2..3"),
            ("--keys-cardinality", "0"),
            ("--row-data-size-range", "10MiB..1MiB"),
            ("--row-data-size-range", "1MiB..wat"),
        ] {
            let error = Cli::try_parse_from([
                "se-stress",
                "start",
                "--catalog-id",
                "cat",
                "--namespace",
                "ns",
                flag,
                value,
            ])
            .unwrap_err();
            errors.push(format!("{flag} {value}: {error}"));
        }
        insta::assert_snapshot!(errors.join("\n"), @r#"
        --batch-size-range 0: error: invalid value '0' for '--batch-size-range <BATCH_SIZE_RANGE>': batch size range must be positive and ascending

        For more information, try '--help'.

        --batch-size-range 9..2: error: invalid value '9..2' for '--batch-size-range <BATCH_SIZE_RANGE>': batch size range must be positive and ascending

        For more information, try '--help'.

        --batch-size-range 1..2..3: error: invalid value '1..2..3' for '--batch-size-range <BATCH_SIZE_RANGE>': invalid digit found in string

        For more information, try '--help'.

        --keys-cardinality 0: error: invalid value '0' for '--keys-cardinality <KEYS_CARDINALITY>': number would be zero for non-zero type

        For more information, try '--help'.

        --row-data-size-range 10MiB..1MiB: error: invalid value '10MiB..1MiB' for '--row-data-size-range <ROW_DATA_SIZE_RANGE>': row data size range must be ascending

        For more information, try '--help'.

        --row-data-size-range 1MiB..wat: error: invalid value '1MiB..wat' for '--row-data-size-range <ROW_DATA_SIZE_RANGE>': couldn't parse "wat" into a ByteSize, cannot parse float from empty string

        For more information, try '--help'.
        "#);
    }
}
