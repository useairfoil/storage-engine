use std::{error::Error, sync::Arc};

use clap::{Parser, Subcommand};
use se::{
    cmd::{CommitArgs, DbgWalArgs, DevArgs, MaterializeArgs, ReconcileArgs, SqlArgs, StartArgs},
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
    /// Start a service.
    Start(StartArgs),
    /// Run the development server.
    Dev(DevArgs),
    /// Inspect, tail, or export a table's ingestion WAL without acknowledging batches.
    DbgWal(DbgWalArgs),
    /// Start materialization of a table's ingestion WAL.
    Materialize(MaterializeArgs),
    /// Publish a table's materialized files to Iceberg.
    Commit(CommitArgs),
    /// Materialize a table's ingestion WAL and publish it to Iceberg.
    Reconcile(ReconcileArgs),
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
        Command::Start(args) => args.run(ct).await?,
        Command::Dev(args) => args.run(ct).await?,
        Command::DbgWal(args) => args.run(ct, clock.as_ref()).await?,
        Command::Materialize(args) => args.run(clock).await?,
        Command::Commit(args) => args.run().await?,
        Command::Reconcile(args) => args.run().await?,
        Command::Sql(args) => args.run(ct).await?,
    };

    Ok(())
}
