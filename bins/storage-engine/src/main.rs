use std::{error::Error, sync::Arc};

use clap::{Parser, Subcommand};
use se::{
    cmd::{DbgWalArgs, DevArgs},
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
    /// Run the development server.
    Dev(DevArgs),
    /// Inspect, tail, or export a table's ingestion WAL without acknowledging batches.
    DbgWal(DbgWalArgs),
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
        Command::DbgWal(args) => args.run(ct, clock.as_ref()).await?,
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::error::ErrorKind;

    use super::*;

    #[test]
    fn dbg_wal_subcommands_accept_target_flags() {
        for command in ["manifest", "tail"] {
            let parsed = Cli::try_parse_from([
                "se",
                "dbg-wal",
                command,
                "--catalog-id",
                "example",
                "--table",
                "analytics.events.page_views",
            ])
            .ok()
            .unwrap();
            assert!(matches!(parsed.command, Command::DbgWal(_)));
        }

        let parsed = Cli::try_parse_from([
            "se",
            "dbg-wal",
            "to-parquet",
            "--catalog-id",
            "example",
            "--table",
            "analytics.events.page_views",
            "--output",
            "export.parquet",
            "--sequence-range",
            "1..5",
        ])
        .unwrap();
        assert!(matches!(parsed.command, Command::DbgWal(_)));

        let parsed = Cli::try_parse_from([
            "se",
            "dbg-wal",
            "to-parquet",
            "--catalog-id",
            "example",
            "--table",
            "analytics.events.page_views",
            "--output",
            "export.parquet",
        ])
        .unwrap();
        assert!(matches!(parsed.command, Command::DbgWal(_)));

        let missing = Cli::try_parse_from(["se", "dbg-wal", "tail", "--catalog-id", "example"])
            .err()
            .unwrap();
        assert_eq!(missing.kind(), ErrorKind::MissingRequiredArgument);

        let missing_output = Cli::try_parse_from([
            "se",
            "dbg-wal",
            "to-parquet",
            "--catalog-id",
            "example",
            "--table",
            "analytics.events.page_views",
        ])
        .err()
        .unwrap();
        assert_eq!(missing_output.kind(), ErrorKind::MissingRequiredArgument);

        let invalid = Cli::try_parse_from([
            "se",
            "dbg-wal",
            "manifest",
            "--catalog-id",
            "example",
            "--table",
            "views",
        ])
        .err()
        .unwrap();
        assert_eq!(invalid.kind(), ErrorKind::ValueValidation);

        let invalid_range = Cli::try_parse_from([
            "se",
            "dbg-wal",
            "to-parquet",
            "--catalog-id",
            "example",
            "--table",
            "analytics.events.page_views",
            "--output",
            "export.parquet",
            "--sequence-range",
            "5..1",
        ])
        .err()
        .unwrap();
        assert_eq!(invalid_range.kind(), ErrorKind::ValueValidation);
    }
}
