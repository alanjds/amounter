use std::{path::PathBuf, process::ExitCode};

mod telemetry;

use amounter::{
    IngestionReport, Outcome, Store,
    entity::account,
    format::csv,
    pipeline::{self},
};
use clap::Parser;
use futures_util::TryStreamExt;
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
    input: Option<PathBuf>,

    /// Database URL, e.g. `sqlite://ledger.db?mode=rwc` or `postgres://user:pw@host/db`.
    /// Defaults to an in-memory SQLite database.
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

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let input: Box<dyn AsyncRead + Send + Unpin> = match &cli.input {
        Some(path) if path.as_os_str() != "-" => Box::new(File::open(path).await?),
        _ => Box::new(io::stdin()),
    };
    let store = match &cli.database_url {
        Some(url) => Store::connect(url).await?,
        None => Store::in_memory().await?,
    };

    // Stage 1 decodes, stage 2 applies; both are lazy and run in lockstep.
    let operations = csv::decode(input);
    let mut outcomes = Box::pin(pipeline::ingest(store.clone(), operations));
    let mut ingestion = IngestionReport::default();
    async {
        while let Some((row, outcome)) = outcomes.try_next().await? {
            ingestion.record(&outcome);
            if let Outcome::Rejected(reason) = outcome {
                tracing::warn!(row, reason = %reason, "rejected");
            }
        }
        Ok::<_, csv::Error>(())
    }
    .instrument(tracing::info_span!("ingest"))
    .await?;
    tracing::info!(
        applied = ingestion.applied,
        duplicates = ingestion.duplicates,
        rejected = ingestion.rejected,
        "done"
    );

    // Stage 3 is independent of the ingest above.
    let mut stdout = BufWriter::new(io::stdout());
    let summaries = account::Model::summaries(&store).await?;
    csv::encode_report(Box::pin(summaries), &mut stdout)
        .instrument(tracing::info_span!("write_report"))
        .await?;
    stdout.shutdown().await?;
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
