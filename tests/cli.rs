use indoc::indoc;
use rstest::rstest;

mod common;
use common::{TempDb, assert_report_eq, bin, run_cli, temp_db};

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
    assert!(result.stderr.contains("rejected row=2 reason=insufficient"));
}

#[test]
fn missing_file_fails() {
    let result = run_cli(bin().arg("/nonexistent.csv"), "");
    assert!(!result.success);
    assert!(result.stderr.contains("ERROR"));
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
        result.stderr.contains("rejected row=5 reason=insufficient"),
        "{}",
        result.stderr
    );
}
