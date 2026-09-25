use clap::{CommandFactory, Parser};

#[derive(Parser)]
#[command(name = "se-stress")]
#[command(about = "Storage Engine stress testing CLI")]
#[command(version)]
struct Cli {}

fn main() -> std::io::Result<()> {
    Cli::parse();
    Cli::command().print_help()
}
