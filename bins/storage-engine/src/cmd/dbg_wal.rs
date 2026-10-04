use std::{fs::File, io::Cursor, ops::RangeInclusive, path::PathBuf, sync::Arc, time::Duration};

use clap::{Args, Subcommand};
use datafusion::arrow::{
    array::{ArrayRef, StringArray, UInt64Array},
    ipc::reader::StreamReader,
    record_batch::RecordBatch,
    util::pretty::pretty_format_batches,
};
use object_store::{ObjectStoreExt, path::Path as ObjectPath};
use parquet::arrow::ArrowWriter;
use se_common::clock::SystemClock;
use se_meta_store::{
    catalog::{CatalogName, CatalogStore, TableIdent},
    table::StoredTable,
};
use tokio_util::sync::CancellationToken;

use crate::{object_store::ObjectStoreArgs, secret_store::SecretStoreArgs};

/// Inspect the WAL of a stored table without changing its manifest.
#[derive(Debug, Args)]
pub struct DbgWalArgs {
    #[command(subcommand)]
    command: DbgWalCommand,
}

#[derive(Debug, Subcommand)]
enum DbgWalCommand {
    /// Print the current WAL manifest.
    Manifest(ManifestArgs),
    /// Print retained batches, then wait for new ones (without acknowledging them).
    Tail(TailArgs),
    /// Write retained WAL batches to a local Parquet file.
    ToParquet(ToParquetArgs),
}

#[derive(Debug, Args)]
struct ManifestArgs {
    #[command(flatten)]
    target: WalTargetArgs,
}

#[derive(Debug, Args)]
struct TailArgs {
    #[command(flatten)]
    target: WalTargetArgs,
}

#[derive(Debug, Args)]
struct WalTargetArgs {
    #[command(flatten)]
    object_store: ObjectStoreArgs,
    #[command(flatten)]
    secret_store: SecretStoreArgs,
    /// ID of the catalog containing the table.
    #[arg(long)]
    catalog_id: String,
    /// Fully qualified table name, e.g. analytics.events.page_views.
    #[arg(long, value_parser = parse_table)]
    table: TableIdent,
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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("secret store error: {0}")]
    SecretStore(#[from] se_secret_store::Error),
    #[error("catalog store error: {0}")]
    CatalogStore(#[from] se_meta_store::catalog::Error),
    #[error("wal error: {0}")]
    Wal(#[from] se_ingestion::WalError),
    #[error("arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("output error: {0}")]
    Io(#[from] std::io::Error),
    #[error("no WAL batches match the requested sequence range")]
    NoMatchingBatches,
    #[error("invalid catalog id: {0}")]
    InvalidCatalogId(String),
    #[error("catalog not found: {0}")]
    CatalogNotFound(CatalogName),
    #[error("table not found: {0}")]
    TableNotFound(TableIdent),
}

impl DbgWalArgs {
    pub async fn run(self, ct: CancellationToken, clock: &dyn SystemClock) -> Result<(), Error> {
        match self.command {
            DbgWalCommand::Manifest(args) => args.run(ct, clock).await,
            DbgWalCommand::Tail(args) => args.run(ct, clock).await,
            DbgWalCommand::ToParquet(args) => args.run(ct, clock).await,
        }
    }
}

impl ManifestArgs {
    async fn run(self, _ct: CancellationToken, _clock: &dyn SystemClock) -> Result<(), Error> {
        let table = self.target.load_table().await?;
        let manifest = se_ingestion::wal_tailer(&table).manifest().await?;
        let entries = manifest.entries();
        let batch = RecordBatch::try_from_iter(vec![
            (
                "sequence",
                Arc::new(UInt64Array::from_iter_values(
                    entries.iter().map(|entry| entry.sequence),
                )) as ArrayRef,
            ),
            (
                "location",
                Arc::new(StringArray::from_iter_values(
                    entries.iter().map(|entry| entry.location.as_str()),
                )) as ArrayRef,
            ),
            (
                "num_entries",
                Arc::new(UInt64Array::from_iter_values(
                    entries.iter().map(|entry| entry.metadata.len() as u64),
                )) as ArrayRef,
            ),
        ])?;
        println!(
            "version={} epoch={} next_sequence={}",
            manifest.version, manifest.epoch, manifest.next_sequence
        );
        println!("{}", pretty_format_batches(&[batch])?);
        Ok(())
    }
}

impl TailArgs {
    async fn run(self, ct: CancellationToken, clock: &dyn SystemClock) -> Result<(), Error> {
        let table = self.target.load_table().await?;
        let mut tailer = se_ingestion::wal_tailer(&table);
        loop {
            let batch = tokio::select! {
                _ = ct.cancelled() => return Ok(()),
                batch = tailer.next_batch() => batch?,
            };
            if let Some(batch) = batch {
                println!(
                    "sequence={} location={} metadata={:?}",
                    batch.sequence, batch.location, batch.metadata
                );
                println!("{}", pretty_format_batches(&batch.batches)?);
            } else {
                tokio::select! {
                    _ = ct.cancelled() => return Ok(()),
                    _ = clock.sleep(Duration::from_secs(1)) => {},
                }
            }
        }
    }
}

impl ToParquetArgs {
    async fn run(self, _ct: CancellationToken, _clock: &dyn SystemClock) -> Result<(), Error> {
        let table = self.target.load_table().await?;
        let manifest = se_ingestion::wal_tailer(&table).manifest().await?;
        let selected: Vec<_> = manifest
            .entries()
            .iter()
            .filter(|entry| {
                self.sequence_range
                    .as_ref()
                    .is_none_or(|range| range.contains(&entry.sequence))
            })
            .collect();
        if selected.is_empty() {
            return Err(Error::NoMatchingBatches);
        }

        let object_store = table.object_store();
        let mut writer = None;
        for entry in selected {
            let data = object_store
                .get(&ObjectPath::from(entry.location.as_str()))
                .await?
                .bytes()
                .await?;
            let reader = StreamReader::try_new(Cursor::new(data), None)?;
            let writer = match &mut writer {
                Some(writer) => writer,
                None => writer.insert(ArrowWriter::try_new(
                    File::create(&self.output)?,
                    reader.schema(),
                    None,
                )?),
            };
            for batch in reader {
                writer.write(&batch?)?;
            }
        }
        if let Some(writer) = writer {
            writer.close()?;
        }
        Ok(())
    }
}

impl WalTargetArgs {
    async fn load_table(self) -> Result<StoredTable, Error> {
        let catalog_id = CatalogName::new(&self.catalog_id)
            .map_err(|_| Error::InvalidCatalogId(self.catalog_id.clone()))?;
        let object_store = self.object_store.create_object_store()?;
        let secret_store = self
            .secret_store
            .create_secret_store(object_store.clone())
            .await?;
        let catalog_store = CatalogStore::new(secret_store, object_store);
        let catalog = catalog_store
            .get(catalog_id.clone())
            .await?
            .ok_or(Error::CatalogNotFound(catalog_id))?;
        catalog
            .try_load_table(self.table.clone())
            .await?
            .ok_or(Error::TableNotFound(self.table))
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
