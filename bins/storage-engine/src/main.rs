use std::{error::Error, sync::Arc};

use clap::{Parser, Subcommand};
use se::{cmd::DevArgs, handle_shutdown_signal};
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
    /// Run the development server.
    Dev(DevArgs),
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
        Command::Dev(args) => args.run(ct).await?,
    };

    Ok(())
}
