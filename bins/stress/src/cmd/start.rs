use std::{num::NonZeroU64, ops::RangeInclusive, sync::Arc, time::Duration};

use bytesize::ByteSize;
use clap::{Args, builder::TypedValueParser};
use iceberg::{Error as IcebergError, NamespaceIdent};
use reqwest::Url;
use se_common::{
    clock::{DefaultSystemClock, SystemClock},
    id::IdGenerator,
    rand::SeRand,
};
use se_iceberg::schema::TableMode;
use se_metadata_client::{HttpMetadataClient, MetadataClient};
use thiserror::Error;
use tokio::task::JoinSet;
use tracing::{debug, info};

use crate::{
    catalog, flight,
    r#gen::{DataGenerator, SortColumn},
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
    /// Base URL of the Flight server.
    #[arg(long, default_value = "http://localhost:7777")]
    engine_url: Url,
    /// Base URL of the metadata server; defaults to the engine URL.
    #[arg(long)]
    metadata_url: Option<Url>,
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
    #[arg(long, default_value = "upsert", value_parser = table_mode_parser())]
    mode: TableMode,
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
    /// Target production rate in bytes per second, e.g. 8MiB; unlimited when omitted.
    #[arg(long, default_value = "1MiB", value_parser = parse_rate)]
    rate: NonZeroU64,
    /// Stop after producing this many batch bytes, e.g. 128MiB (rounded up to a whole batch).
    #[arg(long, default_value = "128MiB")]
    total_size: ByteSize,
}

impl StartArgs {
    /// Creates or loads a table and publishes generated batches until completion or interruption.
    pub async fn run(self) -> Result<(), Error> {
        let clock = Arc::new(DefaultSystemClock::new());
        let rng = Arc::new(SeRand::default());

        let metadata: Arc<dyn MetadataClient> = Arc::new(HttpMetadataClient::new(
            self.metadata_url.unwrap_or_else(|| self.engine_url.clone()),
        ));
        let catalog = catalog::load(metadata, &self.catalog_id).await?;
        debug!(namespace = %self.namespace, "checking namespace");

        if !catalog.namespace_exists(&self.namespace).await? {
            debug!(namespace = %self.namespace, "creating namespace");
            catalog
                .create_namespace(&self.namespace, Default::default())
                .await?;
        }

        debug!(namespace = %self.namespace, "namespace ready");

        let generator = DataGenerator::new(
            self.mode,
            self.sort,
            self.keys_cardinality,
            self.row_data_size_range,
            self.batch_size_range,
            clock.clone(),
            rng.clone(),
        )?;

        let table_name = self
            .table
            .unwrap_or_else(|| format!("stress_{}", rng.rng().gen_uuid().simple()));

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
            self.engine_url.as_str(),
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

        let mut publications = JoinSet::<Result<(), flight::Error>>::new();

        let started = clock.now();

        let mut produced_rows = 0_u64;
        let mut produced_bytes = 0_u64;

        let mut failure = loop {
            if produced_bytes >= self.total_size.as_u64() {
                break None;
            }

            // Schedule by cumulative batch size so generation and upload time count
            // toward the target rate, rather than adding a fixed delay per batch.
            let delay = {
                let target =
                    Duration::from_secs_f64(produced_bytes as f64 / self.rate.get() as f64);
                let elapsed = (clock.now() - started).to_std().unwrap_or_default();
                target.saturating_sub(elapsed)
            };

            tokio::select! {
                biased;
                result = &mut shutdown => break result.err().map(Error::from),
                result = publications.join_next(), if !publications.is_empty() => {
                    match result {
                        Some(Ok(Ok(()))) => {}
                        Some(Ok(Err(error))) => break Some(error.into()),
                        Some(Err(error)) => break Some(error.into()),
                        None => break None,
                    }
                }
                _ = async {
                    if !delay.is_zero() {
                        clock.sleep(delay).await;
                    }
                }, if publications.len() < 32 => {
                    let batch = generator.next_batch();

                    produced_rows = produced_rows.saturating_add(batch.num_rows() as u64);
                    produced_bytes = produced_bytes.saturating_add(batch.get_array_memory_size() as u64);

                    publications.spawn(publisher.publish(batch));

                    let elapsed = (clock.now() - started).to_std().unwrap_or_default().as_secs_f64();

                    let rate = if elapsed > 0.0 {
                        (produced_bytes as f64 / elapsed) as u64
                    } else {
                        0
                    };

                    info!(
                        total_rows = produced_rows,
                        total_bytes = %ByteSize::b(produced_bytes),
                        rate = %format_args!("{}/s", ByteSize::b(rate)),
                        "stress test progress"
                    );
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

fn table_mode_parser() -> impl TypedValueParser<Value = TableMode> {
    clap::builder::PossibleValuesParser::new(["append", "upsert"]).map(|mode| {
        if mode == "append" {
            TableMode::Append
        } else {
            TableMode::Upsert
        }
    })
}

fn parse_rate(value: &str) -> Result<NonZeroU64, String> {
    let bytes = value.parse::<ByteSize>()?.as_u64();
    NonZeroU64::new(bytes).ok_or_else(|| "production rate must be positive".to_owned())
}
