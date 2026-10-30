use std::{error::Error, sync::Arc};

use clap::{Parser, Subcommand};
use se::{
    cmd::{DbgWalArgs, DevArgs, FlightArgs, MetadataArgs, SqlArgs, WorkerArgs},
    handle_shutdown_signal,
};
use se_common::clock::DefaultSystemClock;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(name = "se")]
#[command(about = "Storage Engine CLI")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the metadata HTTP server.
    Metadata(MetadataArgs),
    /// Start the Flight SQL server using remote metadata.
    Flight(FlightArgs),
    /// Start metadata and Flight SQL on the same listener.
    Dev(DevArgs),
    /// Start workers using remote metadata.
    Worker(WorkerArgs),
    /// Inspect, consume, or export a table's ingestion WAL.
    DbgWal(DbgWalArgs),
    /// Execute a SQL query through Arrow Flight SQL.
    Sql(SqlArgs),
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let clock: Arc<_> = DefaultSystemClock::default().into();

    se_observability::init_observability(
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        clock.clone(),
    )?;

    let ct = CancellationToken::new();

    tokio::spawn({
        let ct = ct.clone();
        handle_shutdown_signal(ct)
    });

    let cli = Cli::parse();

    match cli.command {
        Command::Metadata(args) => args.run(clock, ct).await?,
        Command::Flight(args) => args.run(clock, ct).await?,
        Command::Dev(args) => args.run(clock, ct).await?,
        Command::Worker(args) => args.run(clock, ct).await?,
        Command::DbgWal(args) => args.run(clock.as_ref(), ct).await?,
        Command::Sql(args) => args.run(ct).await?,
    }

    Ok(())
}
