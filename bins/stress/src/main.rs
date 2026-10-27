// mod catalog;
// mod cmd;
// mod flight;
// mod r#gen;
// mod helpers;

use std::sync::Arc;

use clap::{Parser, Subcommand};
use se_common::clock::DefaultSystemClock;
// use tracing::error;

// use crate::cmd::start::StartArgs;

#[derive(Debug, Parser)]
#[command(name = "se-stress")]
#[command(about = "Storage Engine stress testing CLI")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    // /// Continuously push generated batches to a table until interrupted.
    // Start(StartArgs),
}

#[tokio::main]
async fn main() -> Result<(), se_observability::ObservabilityError> {
    se_observability::init_observability(
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        Arc::new(DefaultSystemClock::new()),
    )?;

    match Cli::parse().command {
        // Command::Start(args) => {
        //     if let Err(error) = args.run().await {
        //         error!(%error, "stress test failed");
        //         std::process::exit(1);
        //     }
        // }
    }

    // Ok(())
}
