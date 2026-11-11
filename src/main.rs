use std::{path::PathBuf, process::ExitCode};

mod telemetry;

use amounter::{Store, process, write_report};
use clap::Parser;
use telemetry::LogFormat;
use tokio::{
    fs::File,
    io::{self, AsyncRead, AsyncWriteExt, BufWriter},
};
use tracing::Instrument;

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

    #[arg(
        long,
        value_enum,
        ignore_case = true,
        env = "LOG_FORMAT",
        default_value = "text"
    )]
    log_format: LogFormat,
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
        tracing::warn!(row, reason = %reason, "rejected");
    })
    .await?;
    let (applied, duplicates, rejected) = (summary.applied, summary.duplicates, summary.rejected);
    tracing::info!(applied, duplicates, rejected, "done");

    let mut out = BufWriter::new(io::stdout());
    write_report(&store, &mut out).await?;
    out.shutdown().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Err(e) = telemetry::init(cli.log_format) {
        eprintln!("error: {e}");
        return ExitCode::FAILURE;
    }
    let span = tracing::info_span!("run");
    match run(cli).instrument(span).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("{e}");
            ExitCode::FAILURE
        }
    }
}
