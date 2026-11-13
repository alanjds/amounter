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

#[test]
fn missing_amount_is_fatal() {
    let result = run_cli(
        bin().arg("-"),
        indoc! {"
            type,client,tx,amount
            deposit,1,1,
        "},
    );
    assert!(!result.success);
    assert!(result.stderr.contains("ERROR"), "{}", result.stderr);
    assert!(result.stderr.contains("amount"), "{}", result.stderr);
    assert_eq!(result.stdout, "");
}

/// Not atomic, and no report on a fatal error: rows before the bad one stay.
#[rstest]
fn decode_error_keeps_earlier_rows_and_prints_no_report(temp_db: TempDb) {
    let url = temp_db.url();
    let failed = run_cli(
        bin().arg("-").env("DATABASE_URL", &url),
        indoc! {"
            type,client,tx,amount
            deposit,1,1,1.0
            bogus,1,2,1.0
        "},
    );
    assert!(!failed.success);
    assert_eq!(failed.stdout, "");
    assert!(failed.stderr.contains("ERROR"), "{}", failed.stderr);

    let next = run_cli(
        bin().arg("-").env("DATABASE_URL", &url),
        indoc! {"
            type,client,tx,amount
            deposit,1,2,1.0
        "},
    );
    assert!(next.success);
    assert_eq!(
        next.stdout,
        indoc! {"
            client,available,held,total,locked
            1,2.0000,0.0000,2.0000,false
        "}
    );
}

/// Changes the database behind the binary's back.
fn edit(url: &str, change: impl AsyncFnOnce(&sea_orm::DatabaseConnection)) {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = sea_orm::Database::connect(url).await.unwrap();
        change(&db).await;
        db.close().await.unwrap();
    });
}

#[rstest]
fn verify_is_silent_when_clean_and_prints_each_discrepancy(temp_db: TempDb) {
    use amounter::entity::account;
    use sea_orm::{ActiveModelTrait, ActiveValue::Set};
    let url = temp_db.url();
    let ingest = run_cli(
        bin().arg("-").env("DATABASE_URL", &url),
        indoc! {"
            type,client,tx,amount
            deposit,1,1,2.0
            dispute,1,1,
        "},
    );
    assert!(ingest.success, "{}", ingest.stderr);
    let verify = || run_cli(bin().arg("verify").env("DATABASE_URL", &url), "");
    let clean = verify();
    assert!(clean.success, "{}", clean.stderr);
    assert_eq!((clean.stdout.as_str(), clean.stderr.as_str()), ("", ""));

    edit(&url, async |db| {
        let edited = account::ActiveModel {
            client: Set(1),
            held: Set(amounter::Amount::ZERO),
            ..Default::default()
        };
        edited.update(db).await.unwrap();
    });
    let r = verify();
    assert!(!r.success);
    assert_eq!(
        r.stdout,
        "accounts client 1: held should be 2.0000, is 0.0000\n"
    );
    assert!(r.stderr.contains("1 discrepancies"), "{}", r.stderr);
}

#[test]
fn a_subcommand_takes_no_input() {
    let r = run_cli(bin().args(["file.csv", "verify"]), "");
    assert!(!r.success);
    assert!(r.stderr.contains("cannot be used with"), "{}", r.stderr);
}
