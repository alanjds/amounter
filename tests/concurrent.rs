//! Several independent streams sharing one `Store`, as a TCP server would.

use amounter::{Store, process, write_report};
use indoc::indoc;

const CSV_HEADER: &str = "type,client,tx,amount\n";

/// `n` deposit rows of 1.0 for `client`, with consecutive tx ids; no header.
fn deposit_rows(first_tx: u32, n: u32, client: u16) -> String {
    (0..n)
        .map(|i| format!("deposit,{client},{},1.0\n", first_tx + i))
        .collect()
}

/// A complete CSV document of [`deposit_rows`].
fn deposits(first_tx: u32, n: u32, client: u16) -> String {
    format!("{CSV_HEADER}{}", deposit_rows(first_tx, n, client))
}

#[tokio::test]
async fn concurrent_streams_on_same_client_lose_no_updates() {
    let store = Store::in_memory().await.unwrap();
    let handles: Vec<_> = (0..4u32)
        .map(|k| {
            let store = store.clone();
            tokio::spawn(async move {
                let csv = deposits(k * 100, 25, 1);
                process(csv.as_bytes(), &store, |_, _| {}).await.unwrap()
            })
        })
        .collect();
    for h in handles {
        assert_eq!(h.await.unwrap().applied, 25);
    }

    let mut out = Vec::new();
    write_report(&store, &mut out).await.unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        indoc! {"
            client,available,held,total,locked
            1,100.0000,0.0000,100.0000,false
        "}
    );
}

#[tokio::test]
async fn tx_ids_are_unique_across_streams() {
    let store = Store::in_memory().await.unwrap();
    let a = deposits(1, 10, 1);
    let b = deposits(1, 10, 2); // same tx ids, different client
    let (ra, rb) = tokio::join!(
        process(a.as_bytes(), &store, |_, _| {}),
        process(b.as_bytes(), &store, |_, _| {}),
    );
    let (ra, rb) = (ra.unwrap(), rb.unwrap());
    assert_eq!(ra.applied + rb.applied, 10);
    assert_eq!(ra.rejected + rb.rejected, 10);
}

/// Runs only when `AMOUNTER_TEST_POSTGRES` holds a `postgres://` URL of a
/// scratch database (its tables are emptied first). Exercises real
/// multi-connection row locking, which in-memory SQLite cannot.
#[tokio::test]
async fn postgres_concurrent_streams_lose_no_updates() {
    use sea_orm::ConnectionTrait;
    let Ok(url) = std::env::var("AMOUNTER_TEST_POSTGRES") else {
        eprintln!("skipped: AMOUNTER_TEST_POSTGRES not set");
        return;
    };
    let store = Store::connect(&url).await.unwrap();
    store
        .db()
        .execute_unprepared("TRUNCATE accounts, transactions")
        .await
        .unwrap();

    // 8 streams hammer the SAME client concurrently, plus 8 distinct clients.
    let handles: Vec<_> = (0..8u32)
        .map(|k| {
            let store = store.clone();
            tokio::spawn(async move {
                let csv = format!(
                    "{CSV_HEADER}{}{}",
                    deposit_rows(k * 1000, 50, 1),
                    deposit_rows(k * 1000 + 500, 50, 10 + k as u16),
                );
                process(csv.as_bytes(), &store, |_, _| {}).await.unwrap()
            })
        })
        .collect();
    for h in handles {
        assert_eq!(h.await.unwrap().rejected, 0);
    }
    let mut out = Vec::new();
    write_report(&store, &mut out).await.unwrap();
    let out = String::from_utf8(out).unwrap();
    assert!(
        out.contains("\n1,400.0000,0.0000,400.0000,false\n"),
        "{out}"
    );
    for k in 10..18 {
        assert!(
            out.contains(&format!("\n{k},50.0000,0.0000,50.0000,false\n")),
            "{out}"
        );
    }
}
