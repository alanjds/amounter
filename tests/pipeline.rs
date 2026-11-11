//! End-to-end checks of the lib pipeline (CSV in -> CSV out) over in-memory SQLite.

use amounter::{Store, process, write_report};
use indoc::indoc;
use rstest::rstest;

mod common;
use common::{TempDb, assert_report_eq, temp_db};

async fn run(input: &str) -> (String, u64) {
    let store = Store::in_memory().await.unwrap();
    let summary = process(input.as_bytes(), &store, |_, _| {}).await.unwrap();
    let mut out = Vec::new();
    write_report(&store, &mut out).await.unwrap();
    (String::from_utf8(out).unwrap(), summary.rejected)
}

const HEADER: &str = "client,available,held,total,locked\n";

#[rstest]
#[case::deposit_withdraw(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,3.0
        withdrawal,1,2,1.5
    "},
    indoc! {"
        1,1.5000,0.0000,1.5000,false
    "},
    0
)]
#[case::overdraw_rejected(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,1.0
        withdrawal,1,2,2.0
    "},
    indoc! {"
        1,1.0000,0.0000,1.0000,false
    "},
    1
)]
#[case::dispute_holds_funds(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,2.0
        deposit,1,2,1.0
        dispute,1,1,
    "},
    indoc! {"
        1,1.0000,2.0000,3.0000,false
    "},
    0
)]
#[case::dispute_without_trailing_comma(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,2.0
        dispute,1,1
    "},
    indoc! {"
        1,0.0000,2.0000,2.0000,false
    "},
    0
)]
#[case::resolve_releases(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,2.0
        dispute,1,1,
        resolve,1,1,
    "},
    indoc! {"
        1,2.0000,0.0000,2.0000,false
    "},
    0
)]
#[case::chargeback_locks(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,2.0
        dispute,1,1,
        chargeback,1,1,
        deposit,1,2,5.0
    "},
    indoc! {"
        1,0.0000,0.0000,0.0000,true
    "},
    1
)]
#[case::dispute_other_clients_tx_ignored(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,2.0
        deposit,2,2,1.0
        dispute,2,1,
    "},
    indoc! {"
        1,2.0000,0.0000,2.0000,false
        2,1.0000,0.0000,1.0000,false
    "},
    1
)]
#[case::resolve_without_dispute_ignored(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,2.0
        resolve,1,1,
    "},
    indoc! {"
        1,2.0000,0.0000,2.0000,false
    "},
    1
)]
#[case::duplicate_tx_ignored(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,2.0
        deposit,1,1,2.0
    "},
    indoc! {"
        1,2.0000,0.0000,2.0000,false
    "},
    1
)]
#[case::whitespace_and_order(
    indoc! {"
        type, client, tx, amount
        deposit, 2, 1, 1.0
        deposit, 1, 2, 2.0
    "},
    indoc! {"
        1,2.0000,0.0000,2.0000,false
        2,1.0000,0.0000,1.0000,false
    "},
    0
)]
#[case::zero_amount_rejected(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,0
    "},
    "",
    1
)]
#[tokio::test]
async fn processes(#[case] input: &str, #[case] rows: &str, #[case] rejected: u64) {
    let (out, rej) = run(input).await;
    assert_report_eq(&out, &format!("{HEADER}{rows}"));
    assert_eq!(rej, rejected);
}

#[rstest]
#[case::unknown_type(indoc! {"
    type,client,tx,amount
    bogus,1,1,1.0
"})]
#[case::negative_amount(indoc! {"
    type,client,tx,amount
    deposit,1,1,-1.0
"})]
#[case::non_numeric_client(indoc! {"
    type,client,tx,amount
    deposit,notanumber,1,1.0
"})]
#[tokio::test]
async fn malformed_input_is_fatal(#[case] input: &str) {
    let store = Store::in_memory().await.unwrap();
    assert!(process(input.as_bytes(), &store, |_, _| {}).await.is_err());
}

#[rstest]
#[tokio::test]
async fn file_database_persists_across_connections(temp_db: TempDb) {
    let url = temp_db.url();

    let first = Store::connect(&url).await.unwrap();
    process(
        indoc! {"
            type,client,tx,amount
            deposit,1,1,2.0
        "}
        .as_bytes(),
        &first,
        |_, _| {},
    )
    .await
    .unwrap();
    drop(first);

    // Reopening must not fail on existing tables, and must see earlier state.
    let second = Store::connect(&url).await.unwrap();
    let s = process(
        indoc! {"
            type,client,tx,amount
            deposit,1,1,9.0
            withdrawal,1,2,0.5
        "}
        .as_bytes(),
        &second,
        |_, _| {},
    )
    .await
    .unwrap();
    assert_eq!((s.applied, s.rejected), (1, 1)); // tx 1 is a duplicate from the first run
    let mut out = Vec::new();
    write_report(&second, &mut out).await.unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        indoc! {"
            client,available,held,total,locked
            1,1.5000,0.0000,1.5000,false
        "}
    );
}
