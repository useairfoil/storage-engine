use std::{sync::Arc, time::Duration};

use clap::{Args, Subcommand};
use datafusion::arrow::{
    array::{ArrayRef, StringArray, UInt64Array},
    record_batch::RecordBatch,
    util::pretty::pretty_format_batches,
};
use se_common::clock::SystemClock;
use se_meta_store::catalog::{CatalogName, CatalogStore, TableIdent};
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
    Manifest(WalTargetArgs),
    /// Print retained batches, then wait for new ones (without acknowledging them).
    Tail(WalTargetArgs),
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
    #[error("failed to format WAL batches: {0}")]
    Format(#[from] datafusion::arrow::error::ArrowError),
    #[error("invalid catalog id: {0}")]
    InvalidCatalogId(String),
    #[error("catalog not found: {0}")]
    CatalogNotFound(CatalogName),
    #[error("table not found: {0}")]
    TableNotFound(TableIdent),
}

impl DbgWalArgs {
    pub async fn run(self, ct: CancellationToken, clock: &dyn SystemClock) -> Result<(), Error> {
        let (tail, args) = match self.command {
            DbgWalCommand::Manifest(args) => (false, args),
            DbgWalCommand::Tail(args) => (true, args),
        };
        let catalog_id = CatalogName::new(&args.catalog_id)
            .map_err(|_| Error::InvalidCatalogId(args.catalog_id.clone()))?;
        let object_store = args.object_store.create_object_store()?;
        let secret_store = args
            .secret_store
            .create_secret_store(object_store.clone())
            .await?;
        let catalog_store = CatalogStore::new(secret_store, object_store);
        let catalog = catalog_store
            .get(catalog_id.clone())
            .await?
            .ok_or(Error::CatalogNotFound(catalog_id))?;
        let table = catalog
            .try_load_table(args.table.clone())
            .await?
            .ok_or(Error::TableNotFound(args.table))?;
        let mut tailer = se_ingestion::wal_tailer(&table);

        if !tail {
            let manifest = tailer.manifest().await?;
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
            return Ok(());
        }

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

fn parse_table(value: &str) -> Result<TableIdent, String> {
    if value.split('.').any(str::is_empty) {
        return Err("table must be a dot-separated namespace and table name".into());
    }
    TableIdent::from_strs(value.split('.'))
        .map_err(|_| "table must be a dot-separated namespace and table name".into())
}
