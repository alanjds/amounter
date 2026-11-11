use indoc::indoc;
use rstest::rstest;
use std::{
    io::Write,
    process::{Command, Stdio},
};

mod common;
use common::{TempDb, assert_report_eq, temp_db};

fn bin() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_amounter"));
    c.env_remove("DATABASE_URL"); // don't inherit the developer's shell
    c
}

/// A finished run with its output already decoded as text.
struct CliResult {
    success: bool,
    stdout: String,
    stderr: String,
}

/// Spawns `cmd`, pipes `stdin` to it, closes the pipe, and waits for it to exit.
fn run_cli(cmd: &mut Command, stdin: &str) -> CliResult {
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

#[rstest]
#[case::dash("-")]
#[case::double_dash("--")]
fn reads_stdin_and_writes_csv_to_stdout(#[case] stdin_mark: &str) {
    let input = indoc! {"
        type,client,tx,amount
        deposit,1,1,1.0
        withdrawal,1,2,5.0
    "};
    let result = run_cli(bin().arg(stdin_mark), input);
    assert!(result.success);
    assert_eq!(
        result.stdout,
        indoc! {"
            client,available,held,total,locked
            1,1.0000,0.0000,1.0000,false
        "}
    );
    assert!(result.stderr.contains("row 2: rejected: insufficient"));
}

#[test]
fn missing_file_fails() {
    let result = run_cli(bin().arg("/nonexistent.csv"), "");
    assert!(!result.success);
    assert!(result.stderr.starts_with("error:"));
}

#[rstest]
fn database_url_env_persists_state_between_runs(temp_db: TempDb) {
    let url = temp_db.url();
    let run_csv = |csv: &str| {
        let result = run_cli(bin().arg("-").env("DATABASE_URL", &url), csv);
        assert!(result.success);
        result.stdout
    };
    run_csv(indoc! {"
        type,client,tx,amount
        deposit,1,1,1.0
    "});
    let stdout = run_csv(indoc! {"
        type,client,tx,amount
        deposit,1,2,2.0
    "});
    assert_eq!(
        stdout,
        indoc! {"
            client,available,held,total,locked
            1,3.0000,0.0000,3.0000,false
        "}
    );
}

#[test]
fn processes_fixture_file() {
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/transactions.csv"
    );
    let result = run_cli(bin().arg(fixture), "");
    assert!(result.success);
    assert_report_eq(
        &result.stdout,
        indoc! {"
            client,available,held,total,locked
            1,1.5000,0.0000,1.5000,false
            2,2.0000,0.0000,2.0000,false
        "},
    );
    // Client 2 only has 2.0, so the 3.0 withdrawal (row 5) is rejected.
    assert!(
        result.stderr.contains("row 5: rejected: insufficient"),
        "{}",
        result.stderr
    );
}
