//! Helpers shared by the integration tests (`mod common;`).
//!
//! Each test file is its own crate and uses only some of these.
#![allow(dead_code)]

use amounter::{
    IngestionReport, Outcome, Rejection, Seq, Store, entity::account, format::csv, pipeline,
};
use futures_util::TryStreamExt;
use rstest::fixture;
use std::{
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::io::{AsyncRead, AsyncWrite};

/// A scratch SQLite database file, deleted (with its WAL sidecars) on drop,
/// even when the test panics.
pub struct TempDb {
    path: PathBuf,
}

impl TempDb {
    pub fn url(&self) -> String {
        format!("sqlite://{}?mode=rwc", self.path.display())
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let mut file = self.path.clone().into_os_string();
            file.push(suffix);
            std::fs::remove_file(file).ok();
        }
    }
}

#[fixture]
pub fn temp_db() -> TempDb {
    // Unique per test even when tests share a process (plain `cargo test`).
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("amounter-test-{}-{n}.db", std::process::id()));
    TempDb { path }
}

/// Asserts two CSV reports match: same header line first, then the same rows
/// in any order (the report makes no ordering promise).
#[track_caller]
pub fn assert_report_eq(actual: &str, expected: &str) {
    fn split(csv: &str) -> (&str, Vec<&str>) {
        let mut lines = csv.lines();
        let header = lines.next().unwrap_or_default();
        let mut rows: Vec<_> = lines.collect();
        rows.sort_unstable();
        (header, rows)
    }
    assert_eq!(split(actual), split(expected), "actual:\n{actual}");
}

pub fn bin() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_amounter"));
    // Don't inherit the developer's shell.
    for var in ["DATABASE_URL", "RUST_LOG", "LOG_FORMAT"] {
        c.env_remove(var);
    }
    c
}

/// A finished run with its output already decoded as text.
pub struct CliResult {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

/// Spawns `cmd`, pipes `stdin` to it, closes the pipe, and waits for it to exit.
pub fn run_cli(cmd: &mut Command, stdin: &str) -> CliResult {
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // `take()` hands the pipe to this statement; dropping it closes stdin (EOF).
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    CliResult {
        success: out.status.success(),
        stdout: String::from_utf8(out.stdout).unwrap(),
        stderr: String::from_utf8(out.stderr).unwrap(),
    }
}

/// Reads a whole CSV input into the store: `decode`, then `pipeline::ingest`.
/// Rule violations are skipped and passed to `on_reject(row, reason)`; malformed
/// CSV or a database failure aborts with an error.
pub async fn ingest<R: AsyncRead + Unpin + Send>(
    input: R,
    store: &Store,
    mut on_reject: impl FnMut(Seq, &Rejection),
) -> Result<IngestionReport, csv::Error> {
    let mut outcomes = std::pin::pin!(pipeline::ingest(store.clone(), csv::decode(input)));
    let mut report = IngestionReport::default();
    while let Some((row, outcome)) = outcomes.try_next().await? {
        report.record(&outcome);
        if let Outcome::Rejected(reason) = outcome {
            on_reject(row, &reason);
        }
    }
    Ok(report)
}

/// The account report as CSV: `Model::summaries` into `encode_report`.
pub async fn write_report<W: AsyncWrite + Unpin + Send>(
    store: &Store,
    out: W,
) -> Result<(), csv::Error> {
    let summaries = account::Model::summaries(store).await?;
    csv::encode_report(Box::pin(summaries), out).await
}
