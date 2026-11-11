//! Helpers shared by the integration tests (`mod common;`).

use rstest::fixture;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

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
