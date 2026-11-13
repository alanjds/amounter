//! End-to-end checks of the lib pipeline (CSV in -> CSV out) over in-memory SQLite.

use amounter::{Store, entity::account};
use futures_util::TryStreamExt;
use indoc::indoc;
use rstest::rstest;

mod common;
use common::{TempDb, assert_report_eq, ingest, temp_db, write_report};

async fn run(input: &str) -> (String, u64) {
    let store = Store::in_memory().await.unwrap();
    let summary = ingest(input.as_bytes(), &store, |_, _| {}).await.unwrap();
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
#[case::dispute_unknown_tx_ignored(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,2.0
        dispute,1,99,
    "},
    indoc! {"
        1,2.0000,0.0000,2.0000,false
    "},
    1
)]
#[case::dispute_unknown_tx_creates_no_account(
    indoc! {"
        type,client,tx,amount
        dispute,7,99,
    "},
    "",
    1
)]
#[case::resolve_unknown_client_creates_no_account(
    indoc! {"
        type,client,tx,amount
        resolve,9,1,
    "},
    "",
    1
)]
#[case::chargeback_unknown_client_creates_no_account(
    indoc! {"
        type,client,tx,amount
        chargeback,9,1,
    "},
    "",
    1
)]
#[case::dispute_withdrawal_ignored(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,2.0
        withdrawal,1,2,0.5
        dispute,1,2,
    "},
    indoc! {"
        1,1.5000,0.0000,1.5000,false
    "},
    1
)]
#[case::dispute_may_drive_available_negative(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,2.0
        withdrawal,1,2,1.5
        dispute,1,1,
    "},
    indoc! {"
        1,-1.5000,2.0000,0.5000,false
    "},
    0
)]
#[case::withdrawal_still_rejected_while_available_negative(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,2.0
        withdrawal,1,2,1.5
        dispute,1,1,
        withdrawal,1,3,0.0001
    "},
    indoc! {"
        1,-1.5000,2.0000,0.5000,false
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
#[case::identical_replay_ignored(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,2.0
        deposit,1,1,2.0
    "},
    indoc! {"
        1,2.0000,0.0000,2.0000,false
    "},
    0
)]
#[case::conflicting_duplicate_rejected(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,2.0
        deposit,1,1,5.0
        deposit,2,1,2.0
    "},
    indoc! {"
        1,2.0000,0.0000,2.0000,false
    "},
    2
)]
#[case::rejected_withdrawal_keeps_tx_id_taken(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,1.0
        withdrawal,1,2,5.0
        deposit,1,2,1.0
    "},
    indoc! {"
        1,1.0000,0.0000,1.0000,false
    "},
    2
)]
#[case::rejected_tx_is_not_disputable(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,1.0
        withdrawal,1,2,5.0
        dispute,1,2,
    "},
    indoc! {"
        1,1.0000,0.0000,1.0000,false
    "},
    2
)]
#[case::rejected_withdrawal_creates_account_row(
    indoc! {"
        type,client,tx,amount
        withdrawal,3,1,1.0
    "},
    indoc! {"
        3,0.0000,0.0000,0.0000,false
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
#[case::zero_amount_accepted(
    indoc! {"
        type,client,tx,amount
        deposit,1,1,0
        withdrawal,1,2,0.0
    "},
    indoc! {"
        1,0.0000,0.0000,0.0000,false
    "},
    0
)]
#[tokio::test]
async fn processes(#[case] input: &str, #[case] rows: &str, #[case] rejected: u64) {
    let (out, rej) = run(input).await;
    assert_report_eq(&out, &format!("{HEADER}{rows}"));
    assert_eq!(rej, rejected);
}

#[tokio::test]
async fn replays_are_counted_as_duplicates_not_rejections() {
    let store = Store::in_memory().await.unwrap();
    let summary = ingest(
        indoc! {"
            type,client,tx,amount
            deposit,1,1,2.0
            deposit,1,1,2.0
            withdrawal,1,2,9.0
            withdrawal,1,2,9.0
        "}
        .as_bytes(),
        &store,
        |_, _| {},
    )
    .await
    .unwrap();
    // The rejected withdrawal is remembered, so its replay is a duplicate too.
    assert_eq!(
        (summary.applied, summary.duplicates, summary.rejected),
        (1, 2, 1)
    );
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
#[case::missing_amount_on_deposit(indoc! {"
    type,client,tx,amount
    deposit,1,1,
"})]
#[case::missing_amount_on_withdrawal(indoc! {"
    type,client,tx,amount
    withdrawal,1,1,
"})]
#[case::missing_amount_field_omitted(indoc! {"
    type,client,tx,amount
    deposit,1,1
"})]
#[tokio::test]
async fn malformed_input_is_fatal(#[case] input: &str) {
    let store = Store::in_memory().await.unwrap();
    assert!(ingest(input.as_bytes(), &store, |_, _| {}).await.is_err());
}

#[tokio::test]
async fn missing_amount_error_names_the_row_and_keeps_earlier_rows() {
    let store = Store::in_memory().await.unwrap();
    let err = ingest(
        indoc! {"
            type,client,tx,amount
            deposit,1,1,1.0
            deposit,1,2,2.0
            deposit,1,3,
        "}
        .as_bytes(),
        &store,
        |_, _| {},
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("row 3") && err.contains("deposit"), "{err}");
    let mut out = Vec::new();
    write_report(&store, &mut out).await.unwrap();
    assert_report_eq(
        &String::from_utf8(out).unwrap(),
        &format!("{HEADER}1,3.0000,0.0000,3.0000,false\n"),
    );
}

#[rstest]
#[tokio::test]
async fn file_database_persists_across_connections(temp_db: TempDb) {
    let url = temp_db.url();

    let first = Store::connect(&url).await.unwrap();
    ingest(
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
    let s = ingest(
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

async fn clients(store: &Store) -> Vec<(i32, String)> {
    let mut rows: Vec<_> = account::Model::summaries(store)
        .await
        .unwrap()
        .map_ok(|s| (s.client, s.total.to_string()))
        .try_collect()
        .await
        .unwrap();
    rows.sort();
    rows
}

#[tokio::test]
async fn summaries_of_empty_store_is_empty() {
    assert!(clients(&Store::in_memory().await.unwrap()).await.is_empty());
}

#[tokio::test]
async fn summaries_yield_one_row_per_account() {
    let store = Store::in_memory().await.unwrap();
    let csv = "type,client,tx,amount\ndeposit,2,1,1.0\ndeposit,1,2,2.0\n";
    ingest(csv.as_bytes(), &store, |_, _| {}).await.unwrap();
    assert_eq!(
        clients(&store).await,
        [(1, "2.0000".to_owned()), (2, "1.0000".to_owned())]
    );
}

#[rstest]
#[tokio::test]
async fn summaries_after_reconnect_need_no_ingest(temp_db: TempDb) {
    let first = Store::connect(&temp_db.url()).await.unwrap();
    let csv = "type,client,tx,amount\ndeposit,1,1,2.0\n";
    ingest(csv.as_bytes(), &first, |_, _| {}).await.unwrap();
    drop(first);
    let second = Store::connect(&temp_db.url()).await.unwrap();
    assert_eq!(clients(&second).await, [(1, "2.0000".to_owned())]);
}
