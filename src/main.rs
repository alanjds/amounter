use std::{path::PathBuf, process::ExitCode};

use amounter::{Store, process, write_report};
use clap::Parser;
use tokio::{
    fs::File,
    io::{self, AsyncRead, AsyncWriteExt, BufWriter},
};

/// Reads transactions from a CSV, stores them in a database, and prints
/// per-client account balances as CSV on stdout.
#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    /// Input CSV (columns: type,client,tx,amount). Use `-` or `--` for stdin.
    ///
    /// clap consumes a bare `--` as its end-of-options marker, so it arrives
    /// here as "no input given", which also means stdin (like `cat`).
    input: Option<PathBuf>,

    /// Database URL, e.g. `sqlite://ledger.db?mode=rwc` or `postgres://user:pw@host/db`.
    /// Defaults to a throwaway in-memory SQLite database.
    #[arg(long, value_name = "URL", env = "DATABASE_URL")]
    database_url: Option<String>,

    /// Don't report rejected records on stderr.
    #[arg(short, long)]
    quiet: bool,
}

async fn run(cli: Cli) -> amounter::Result<()> {
    let input: Box<dyn AsyncRead + Send + Unpin> = match &cli.input {
        Some(path) if path.as_os_str() != "-" => Box::new(File::open(path).await?),
        _ => Box::new(io::stdin()),
    };
    let store = match &cli.database_url {
        Some(url) => Store::connect(url).await?,
        None => Store::in_memory().await?,
    };

    let summary = process(input, &store, |row, reason| {
        if !cli.quiet {
            eprintln!("row {row}: rejected: {reason}");
        }
    })
    .await?;
    if !cli.quiet && summary.rejected > 0 {
        eprintln!("{} applied, {} rejected", summary.applied, summary.rejected);
    }

    let mut out = BufWriter::new(io::stdout());
    write_report(&store, &mut out).await?;
    out.shutdown().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
