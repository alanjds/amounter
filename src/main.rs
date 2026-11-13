use std::{
    path::{Path, PathBuf},
    process::ExitCode,
};

mod telemetry;

use amounter::{
    IngestionReport, Outcome, Store,
    entity::{account, journal},
    format::csv,
    pipeline::{self},
};
use clap::{Args, Parser, Subcommand};
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
#[command(version, about, args_conflicts_with_subcommands = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<SubCommand>,

    /// Allows `amounter FILE` automatic conversion to `amounter ingest FILE`.
    #[command(flatten)]
    ingest: IngestArgs,

    /// Database URL, e.g. `sqlite://ledger.db?mode=rwc` or `postgres://user:pw@host/db`.
    /// Defaults to an in-memory SQLite database.
    #[arg(long, value_name = "URL", env = "DATABASE_URL", global = true)]
    database_url: Option<String>,

    #[arg(
        long,
        value_enum,
        ignore_case = true,
        env = "LOG_FORMAT",
        default_value = "text",
        global = true
    )]
    log_format: LogFormat,
}

#[derive(Debug, Clone, Subcommand)]
enum SubCommand {
    /// Ingests a CSV and prints all account status. Default command
    Ingest(IngestArgs),
    /// Checks the journal and the accounts against it, read-only.
    Verify,
}

#[derive(Debug, Clone, Args)]
struct IngestArgs {
    /// Input CSV (columns: type,client,tx,amount). Allows `-` or `--` for stdin.
    input: Option<PathBuf>,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

async fn run(cli: Cli) -> Result<(), BoxError> {
    // No subcommand: defaults to `ingest` w/ arguments.
    let command = (cli.command.clone()).unwrap_or(SubCommand::Ingest(cli.ingest.clone()));
    match command {
        SubCommand::Verify => verify(&get_store(&cli).await?).await,
        SubCommand::Ingest(args) => ingest(&cli, args.input.as_deref()).await,
    }
}

async fn get_store(cli: &Cli) -> amounter::Result<Store> {
    match &cli.database_url {
        Some(url) => Store::connect(url).await,
        None => Store::in_memory().await,
    }
}

async fn ingest(cli: &Cli, input: Option<&Path>) -> Result<(), BoxError> {
    let input: Box<dyn AsyncRead + Send + Unpin> = match input {
        Some(path) if path.as_os_str() != "-" => Box::new(File::open(path).await?),
        _ => Box::new(io::stdin()),
    };
    let store = get_store(cli).await?;

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

async fn verify(store: &Store) -> Result<(), BoxError> {
    let found = journal::Model::verify(store)
        .instrument(tracing::info_span!("verify"))
        .await?;
    for line in &found {
        println!("{line}");
    }
    match found.len() {
        0 => Ok(()),
        n => Err(format!("verify found {n} discrepancies").into()),
    }
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
