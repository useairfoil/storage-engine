mod catalog;
mod cmd;
mod flight;
mod r#gen;
mod helpers;

use std::sync::Arc;

use clap::{Parser, Subcommand};
use se_common::clock::DefaultSystemClock;
use tracing::error;

use crate::cmd::start::StartArgs;

#[derive(Debug, Parser)]
#[command(name = "se-stress")]
#[command(about = "Storage Engine stress testing CLI")]
#[command(version = se_common::VERSION)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Continuously push generated batches to a table until interrupted.
    Start(StartArgs),
}

#[tokio::main]
async fn main() -> Result<(), se_observability::ObservabilityError> {
    se_observability::init_observability(
        env!("CARGO_PKG_NAME"),
        se_common::VERSION,
        Arc::new(DefaultSystemClock::new()),
    )?;

    match Cli::parse().command {
        Command::Start(args) => {
            if args.run().await.is_err() {
                error!("stress test failed");
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
