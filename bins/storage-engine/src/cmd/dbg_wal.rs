use std::{fs::File, ops::RangeInclusive, path::PathBuf, sync::Arc, time::Duration};

use clap::{Args, Subcommand};
use datafusion::arrow::util::pretty::pretty_format_batches;
use iceberg::TableIdent;
use parquet::arrow::ArrowWriter;
use reqwest::Url;
use se_common::clock::SystemClock;
use se_metadata_client::{CatalogId, HttpMetadataClient, MetadataClient, WalClient};
use se_wal::{BatchRead, Consumer};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::object_store::ObjectStoreArgs;

#[derive(Debug, Args)]
pub struct DbgWalArgs {
    #[command(subcommand)]
    command: DbgWalCommand,
}

#[derive(Debug, Subcommand)]
enum DbgWalCommand {
    /// Print retained batches, then wait for new ones without acknowledging them.
    Tail(TailArgs),
    /// Follow WAL descriptors silently and acknowledge the consumed range on exit.
    Consume(ConsumeArgs),
    /// Write retained WAL batches to a local Parquet file without acknowledging them.
    ToParquet(ToParquetArgs),
}

#[derive(Debug, Args)]
struct TailArgs {
    #[command(flatten)]
    target: WalTargetArgs,

    /// Fetch and print batch data in addition to metadata.
    #[arg(long)]
    print_data: bool,
}

#[derive(Debug, Args)]
struct ConsumeArgs {
    #[command(flatten)]
    target: WalTargetArgs,

    /// Stop and acknowledge through this inclusive sequence instead of waiting for Ctrl-C.
    #[arg(long)]
    end_sequence: Option<u64>,
}

#[derive(Debug, Args)]
struct ToParquetArgs {
    #[command(flatten)]
    target: WalTargetArgs,

    /// Local Parquet filename to write.
    #[arg(long)]
    output: PathBuf,

    /// Inclusive WAL sequence range, e.g. 1..5.
    #[arg(long, value_parser = parse_sequence_range)]
    sequence_range: Option<RangeInclusive<u64>>,
}

#[derive(Debug, Args)]
struct WalTargetArgs {
    #[command(flatten)]
    object_store: ObjectStoreArgs,

    /// Base URL of the metadata HTTP server.
    #[arg(
        long = "metadata.url",
        env = "SE_METADATA_URL",
        default_value = "http://127.0.0.1:7777"
    )]
    metadata_url: Url,

    /// ID of the catalog containing the table.
    #[arg(long)]
    catalog_id: String,

    /// Fully qualified table name, e.g. analytics.events.page_views.
    #[arg(long, value_parser = parse_table)]
    table: TableIdent,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("metadata client error: {0}")]
    MetadataClient(#[from] se_metadata_client::Error),

    #[error("metadata error: {0}")]
    Metadata(#[from] se_metadata::Error),

    #[error("wal error: {0}")]
    Wal(#[from] se_wal::Error),

    #[error("arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("output error: {0}")]
    Io(#[from] std::io::Error),

    #[error("no WAL batches match the requested sequence range")]
    NoMatchingBatches,
}

impl DbgWalArgs {
    /// Inspects, consumes, or exports a table's retained WAL batches.
    pub async fn run(self, ct: CancellationToken, clock: &dyn SystemClock) -> Result<(), Error> {
        match self.command {
            DbgWalCommand::Consume(args) => args.run(ct, clock).await,
            DbgWalCommand::Tail(args) => ct
                .run_until_cancelled(args.run(clock))
                .await
                .unwrap_or(Ok(())),
            DbgWalCommand::ToParquet(args) => {
                ct.run_until_cancelled(args.run()).await.unwrap_or(Ok(()))
            }
        }
    }
}

impl ConsumeArgs {
    async fn run(self, ct: CancellationToken, clock: &dyn SystemClock) -> Result<(), Error> {
        let Some((wal, epoch)) = ct
            .run_until_cancelled(async {
                let wal = self.target.wal_client().await?;
                let epoch = wal.claim_reader().await?;

                Ok::<_, Error>((wal, epoch))
            })
            .await
            .transpose()?
        else {
            return Ok(());
        };
        let mut last_sequence = None;

        'consume: loop {
            let Some(descriptors) = ct
                .run_until_cancelled(wal.read_after(last_sequence, 128))
                .await
                .transpose()?
            else {
                break;
            };

            if descriptors.is_empty() {
                if ct
                    .run_until_cancelled(clock.sleep(Duration::from_secs(1)))
                    .await
                    .is_none()
                {
                    break;
                }

                continue;
            }

            for descriptor in descriptors {
                if let Some(end) = self.end_sequence
                    && descriptor.sequence >= end
                {
                    last_sequence = Some(end);
                    break 'consume;
                }

                last_sequence = Some(descriptor.sequence);
            }
        }

        if let Some(sequence) = last_sequence {
            wal.ack_through(epoch, sequence).await?;
        }

        Ok(())
    }
}

impl TailArgs {
    async fn run(self, clock: &dyn SystemClock) -> Result<(), Error> {
        let mut consumer = self.target.consumer(None).await?;
        let fetch = consumer.fetch_handle();

        loop {
            let descriptors = consumer.next_descriptors(128).await?;

            if descriptors.is_empty() {
                clock.sleep(Duration::from_secs(1)).await;
                continue;
            }

            for descriptor in descriptors {
                println!(
                    "sequence={} metadata={:?}",
                    descriptor.sequence, descriptor.metadata
                );

                if self.print_data {
                    let batch = fetch.fetch(descriptor).await?;

                    println!("{}", pretty_format_batches(&batch.batches)?);
                }
            }
        }
    }
}

impl ToParquetArgs {
    async fn run(self) -> Result<(), Error> {
        let last_sequence = self
            .sequence_range
            .as_ref()
            .and_then(|range| range.start().checked_sub(1));
        let mut consumer = self.target.consumer(last_sequence).await?;
        let fetch = consumer.fetch_handle();
        let mut writer: Option<ArrowWriter<File>> = None;

        for descriptor in consumer.next_descriptors(usize::MAX).await? {
            if self
                .sequence_range
                .as_ref()
                .is_some_and(|range| descriptor.sequence > *range.end())
            {
                break;
            }

            let batch = fetch.fetch(descriptor).await?;

            for batch in batch.batches {
                let writer = match &mut writer {
                    Some(writer) => writer,
                    None => writer.insert(ArrowWriter::try_new(
                        File::create(&self.output)?,
                        batch.schema(),
                        None,
                    )?),
                };

                writer.write(&batch)?;
            }
        }

        let writer = writer.ok_or(Error::NoMatchingBatches)?;
        writer.close()?;

        Ok(())
    }
}

impl WalTargetArgs {
    async fn consumer(self, last_sequence: Option<u64>) -> Result<Consumer, Error> {
        let wal = self.wal_client().await?;

        Ok(Consumer::new_read_only(
            Arc::new(wal),
            self.object_store.create_object_store()?,
            last_sequence,
        ))
    }

    async fn wal_client(&self) -> Result<WalClient, Error> {
        let catalog_id = CatalogId::new(self.catalog_id.clone())?;
        let metadata: Arc<dyn MetadataClient> =
            Arc::new(HttpMetadataClient::new(self.metadata_url.clone()));

        let table = metadata.resolve_table(&catalog_id, &self.table).await?;
        let table_uuid = table.uuid();

        debug!(table = %self.table, uuid = %table_uuid, "loaded table metadata");

        Ok(WalClient::new(metadata, catalog_id, table_uuid))
    }
}

fn parse_sequence_range(value: &str) -> Result<RangeInclusive<u64>, String> {
    let (start, end) = value
        .split_once("..")
        .ok_or_else(|| "sequence range must be START..END".to_string())?;
    let start = start
        .parse::<u64>()
        .map_err(|_| "sequence range must be START..END with unsigned integers".to_string())?;
    let end = end
        .parse::<u64>()
        .map_err(|_| "sequence range must be START..END with unsigned integers".to_string())?;

    if start > end {
        return Err("sequence range start must not exceed end".into());
    }

    Ok(start..=end)
}

fn parse_table(value: &str) -> Result<TableIdent, String> {
    if value.split('.').any(str::is_empty) {
        return Err("table must be a dot-separated namespace and table name".into());
    }

    TableIdent::from_strs(value.split('.'))
        .map_err(|_| "table must be a dot-separated namespace and table name".into())
}
