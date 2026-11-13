//! The audit journal: one append-only row per state-changing operation, written
//! in the same database transaction as its effect. Each row is also a snapshot
//! of the client's account right after the operation, so the previous state of
//! a client is its previous row (or the all-zero account for its first one).
//!
//! Not journaled: identical replays (`Outcome::Duplicate`) and the rejections
//! that roll back (dispute-like ones, conflicting duplicate ids). They change
//! nothing.
//!
//! [`Model::verify`] audits it, read-only, and reports every discrepancy as a
//! plain string:
//! 1. journal continuity: in `seq` order, each row's operation, applied with
//!    the account rules to the client's previous row (or the all-zero
//!    account), gives the row's outcome and after-state. An edited, deleted or
//!    inserted row that moves a balance breaks that chain. Memory O(clients).
//! 2. live accounts: every `accounts` row equals its client's last journal row;
//!    a client without journal rows must have an all-zero account.
//!
//! Not checked: the `transactions` table, and the chain is by continuity only
//! (a consistent rewrite from some row to the end is not detected, nor is a
//! deleted row that changed no balance).

use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use futures_util::TryStreamExt;
use sea_orm::{
    ActiveValue::NotSet, ConnectionTrait, QueryOrder, TransactionTrait, entity::prelude::*,
};

use crate::{Amount, Operation, Rejection, Store, entity::account};

pub use crate::model::OpKind;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "journal")]
pub struct Model {
    /// The global order, assigned by the database on insert.
    #[sea_orm(primary_key)]
    pub seq: i64,
    pub client: i32,
    pub kind: OpKind,
    pub tx: i64,
    /// Operation amount for a dispute-likes: tx amount
    pub amount: Amount,
    /// `None` when applied, else `Rejection::code`
    pub rejected_reason: Option<String>,
    pub available_after: Amount,
    pub held_after: Amount,
    pub locked_after: bool,
    /// ms since epoch
    pub at: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

impl Model {
    /// Marks the `op` applied to an account, and the account post-state
    /// `seq` is set by the DB on insertion.
    pub fn new(op: &Operation, amount: Amount, after: &account::Model) -> Self {
        Model {
            seq: 0,
            client: i32::from(op.client()),
            kind: op.into(),
            tx: i64::from(op.tx()),
            amount,
            rejected_reason: None,
            available_after: after.available,
            held_after: after.held,
            locked_after: after.locked,
            at: now_millis(),
        }
    }

    /// Marks the row as a deposit/withdrawal tombstone.
    pub fn rejected(self, reason: Rejection) -> Self {
        Model {
            rejected_reason: Some(reason.code().to_owned()),
            ..self
        }
    }

    /// Appends this row to the journal; the database assigns its `seq`.
    pub async fn append<C: ConnectionTrait>(self, db: &C) -> crate::Result<()> {
        let row = ActiveModel {
            seq: NotSet,
            ..ActiveModel::from(self).reset_all()
        };
        Entity::insert(row).exec_without_returning(db).await?;
        Ok(())
    }

    /// The account status after this row.
    pub fn account_after(&self) -> account::Model {
        account::Model {
            client: self.client,
            available: self.available_after,
            held: self.held_after,
            locked: self.locked_after,
        }
    }

    /// Runs this row's operation on `account` with the account rules and the
    /// stored amount. Leaves `account` unchanged on `Err`, like the rules.
    pub fn apply_to(&self, account: &mut account::Model) -> Result<(), Rejection> {
        match self.kind {
            OpKind::Deposit => account.deposit(self.amount),
            OpKind::Withdrawal => account.withdraw(self.amount),
            OpKind::Dispute => account.hold(self.amount),
            OpKind::Resolve => account.release(self.amount),
            OpKind::Chargeback => account.chargeback(self.amount),
        }
    }

    /// Every discrepancy found, one per line
    pub async fn verify(store: &Store) -> crate::Result<Vec<String>> {
        let db = store.db().begin().await?;
        let mut found = Vec::new();
        let mut last: BTreeMap<i32, account::Model> = BTreeMap::new();
        {
            let mut rows = Entity::find().order_by_asc(Column::Seq).stream(&db).await?;
            while let Some(row) = rows.try_next().await? {
                let before = last.get(&row.client).cloned();
                let before = before.unwrap_or_else(|| account::Model::new(row.client));
                for issue in continuity(before, &row) {
                    let (seq, client, tx) = (row.seq, row.client, row.tx);
                    found.push(format!(
                        "journal seq {seq} (client {client}, tx {tx}): {issue}"
                    ));
                }
                last.insert(row.client, row.account_after());
            }
        }
        {
            let mut stored = account::Entity::find()
                .order_by_asc(account::Column::Client)
                .stream(&db)
                .await?;
            while let Some(got) = stored.try_next().await? {
                let want = last.remove(&got.client);
                let want = want.unwrap_or_else(|| account::Model::new(got.client));
                for what in account_diff(&want, &got) {
                    found.push(format!("accounts client {}: {what}", got.client));
                }
            }
        }
        for client in last.into_keys() {
            found.push(format!("accounts client {client}: row missing"));
        }
        db.rollback().await?;
        Ok(found)
    }
}

/// How `row` departs from `before` plus its operation.
fn continuity(mut before: account::Model, row: &Model) -> Vec<String> {
    let result = row.apply_to(&mut before);
    let (wanted, got) = (outcome(result), recorded(row));
    let mut issues = account_diff(&before, &row.account_after());
    if wanted != got {
        issues.insert(0, format!("outcome should be {wanted}, is {got}"));
    }
    issues
}

fn outcome(result: Result<(), Rejection>) -> &'static str {
    result.err().map_or("Applied", |r| r.code())
}

fn recorded(row: &Model) -> &str {
    row.rejected_reason.as_deref().unwrap_or("Applied")
}

/// The differing fields; allocates only when there is one.
fn account_diff(wanted: &account::Model, got: &account::Model) -> Vec<String> {
    if wanted == got {
        return Vec::new();
    }
    let fields = [
        (
            "available",
            wanted.available.to_string(),
            got.available.to_string(),
        ),
        ("held", wanted.held.to_string(), got.held.to_string()),
        ("locked", wanted.locked.to_string(), got.locked.to_string()),
    ];
    fields
        .into_iter()
        .filter(|(_, w, g)| w != g)
        .map(|(field, w, g)| format!("{field} should be {w}, is {g}"))
        .collect()
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use indoc::indoc;
    use rstest::rstest;
    use sea_orm::ActiveValue::Set;

    use super::*;
    use crate::{IngestionReport, format::csv, pipeline};

    const A: Amount = Amount::from_raw(20_000);

    #[test]
    fn a_row_is_the_account_after_the_operation() {
        let after = account::Model {
            available: A,
            ..account::Model::new(7)
        };
        let op = Operation::Deposit {
            client: 7,
            tx: 3,
            amount: A,
        };
        let row = Model::new(&op, A, &after);
        assert_eq!((row.kind, row.tx, row.amount), (OpKind::Deposit, 3, A));
        assert_eq!(row.account_after(), after);
        assert_eq!(row.rejected_reason, None);
        let tombstone = row.rejected(Rejection::AccountLocked);
        assert_eq!(tombstone.rejected_reason.as_deref(), Some("AccountLocked"));
    }

    #[test]
    fn apply_to_runs_the_account_rules() {
        let op = Operation::Dispute { client: 7, tx: 3 };
        let row = Model::new(&op, A, &account::Model::new(7));
        let mut account = account::Model {
            available: A,
            ..account::Model::new(7)
        };
        assert_eq!(row.apply_to(&mut account), Ok(()));
        assert_eq!((account.available, account.held), (Amount::ZERO, A));

        let row = Model {
            kind: OpKind::Withdrawal,
            ..row
        };
        assert_eq!(
            row.apply_to(&mut account),
            Err(Rejection::InsufficientFunds)
        );
        assert_eq!(account.held, A, "unchanged on Err");
    }

    #[tokio::test]
    async fn append_leaves_seq_to_the_database() {
        let store = crate::Store::in_memory().await.unwrap();
        let op = Operation::Resolve { client: 1, tx: 2 };
        for _ in 0..2 {
            Model::new(&op, A, &account::Model::new(1))
                .append(store.db())
                .await
                .unwrap();
        }
        let rows = Entity::find().all(store.db()).await.unwrap();
        assert_eq!(rows.iter().map(|r| r.seq).collect::<Vec<_>>(), [1, 2]);
        assert!(rows.iter().all(|r| r.amount == A));
    }

    async fn ingest(input: &str, store: &Store) -> IngestionReport {
        let ops = csv::decode(input.as_bytes());
        let mut outcomes = std::pin::pin!(pipeline::ingest::<_, csv::Error>(store.clone(), ops));
        let mut report = IngestionReport::default();
        while let Some(item) = outcomes.next().await {
            report.record(&item.unwrap().1);
        }
        report
    }

    /// Journal: 1 deposit c1 (2, 0), 2 deposit c2 (1, 0), 3 withdrawal c1
    /// (1.5, 0), 4 dispute c1 (-0.5, 2), 5 tombstone c2 (1, 0).
    async fn store_with_history() -> Store {
        let store = Store::in_memory().await.unwrap();
        ingest(
            indoc! {"
                type,client,tx,amount
                deposit,1,1,2.0
                deposit,2,2,1.0
                withdrawal,1,3,0.5
                dispute,1,1,
                withdrawal,2,4,5.0
            "},
            &store,
        )
        .await;
        store
    }

    async fn journal_row(store: &Store, seq: i64) -> ActiveModel {
        let row = Entity::find_by_id(seq).one(store.db()).await;
        row.unwrap().unwrap().into()
    }

    const fn amt(raw: i64) -> crate::Amount {
        crate::Amount::from_raw(raw)
    }

    #[tokio::test]
    async fn a_clean_history_has_no_findings() {
        let store = store_with_history().await;
        assert_eq!(Model::verify(&store).await.unwrap(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn an_edited_balance_is_found() {
        let store = store_with_history().await;
        account::ActiveModel {
            client: Set(1),
            available: Set(amt(90_000)),
            ..Default::default()
        }
        .update(store.db())
        .await
        .unwrap();
        assert_eq!(
            Model::verify(&store).await.unwrap(),
            ["accounts client 1: available should be -0.5000, is 9.0000"]
        );
    }

    /// With the first deposit gone, the next row of its client no longer follows.
    #[tokio::test]
    async fn a_deleted_journal_row_is_found() {
        let store = store_with_history().await;
        Entity::delete_by_id(1).exec(store.db()).await.unwrap();
        assert_eq!(
            Model::verify(&store).await.unwrap(),
            [
                "journal seq 3 (client 1, tx 3): outcome should be InsufficientFunds, is Applied",
                "journal seq 3 (client 1, tx 3): available should be 0.0000, is 1.5000",
            ]
        );
    }

    /// An edited after-state breaks the row and the next one of its client.
    #[tokio::test]
    async fn an_edited_journal_row_is_found() {
        let store = store_with_history().await;
        let mut row = journal_row(&store, 3).await;
        row.available_after = Set(amt(90_000));
        row.update(store.db()).await.unwrap();
        assert_eq!(
            Model::verify(&store).await.unwrap(),
            [
                "journal seq 3 (client 1, tx 3): available should be 1.5000, is 9.0000",
                "journal seq 4 (client 1, tx 1): available should be 7.0000, is -0.5000",
            ]
        );
    }

    /// A consistent row appended at the end passes continuity; only the
    /// accounts tell (the limit of a chain without hashes).
    #[tokio::test]
    async fn an_appended_journal_row_shows_against_the_accounts() {
        let store = store_with_history().await;
        let op = crate::Operation::Deposit {
            client: 2,
            tx: 99,
            amount: amt(10_000),
        };
        let after = account::Model {
            available: amt(20_000),
            ..account::Model::new(2)
        };
        let forged = Model::new(&op, amt(10_000), &after);
        forged.append(store.db()).await.unwrap();
        assert_eq!(
            Model::verify(&store).await.unwrap(),
            ["accounts client 2: available should be 2.0000, is 1.0000"]
        );
    }

    /// Known blind spot: the tombstone of tx 4 changed no balance, so its
    /// deletion keeps the chain and the accounts consistent.
    #[tokio::test]
    async fn a_deleted_row_that_moved_nothing_is_not_seen() {
        let store = store_with_history().await;
        Entity::delete_by_id(5).exec(store.db()).await.unwrap();
        assert_eq!(Model::verify(&store).await.unwrap(), Vec::<String>::new());
    }

    /// Deterministic splitmix64.
    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            (z ^ (z >> 31)) % n
        }

        /// Mostly one of `seen`, else `fresh`.
        fn earlier(&mut self, seen: &[(u64, u64)], fresh: (u64, u64)) -> (u64, u64) {
            match self.below(4) {
                0 => fresh,
                _ if seen.is_empty() => fresh,
                _ => seen[self.below(seen.len() as u64) as usize],
            }
        }
    }

    /// A CSV of `rows` operations over few clients and tx ids, so that
    /// redeliveries, conflicting ids, disputes of every kind, wrong clients,
    /// locks and insufficient funds all happen.
    fn generated(seed: u64, rows: usize) -> String {
        let mut rng = Rng(seed);
        let mut csv = String::from("type,client,tx,amount\n");
        let mut sent: Vec<String> = Vec::new();
        // Dispute-like rows mostly reference an earlier deposit or dispute.
        let (mut deposited, mut disputed): (Vec<_>, Vec<_>) = (Vec::new(), Vec::new());
        for _ in 0..rows {
            let (client, tx) = (1 + rng.below(6), 1 + rng.below(rows as u64 / 3 + 1));
            let amount = format!("{}.{:04}", rng.below(6), rng.below(10_000));
            let line = match rng.below(20) {
                0..=7 => {
                    deposited.push((client, tx));
                    format!("deposit,{client},{tx},{amount}")
                }
                8..=11 => format!("withdrawal,{client},{tx},{amount}"),
                12 if !sent.is_empty() => {
                    sent[rng.below(sent.len() as u64) as usize].clone() // redelivery
                }
                12..=15 => {
                    let (client, tx) = rng.earlier(&deposited, (client, tx));
                    disputed.push((client, tx));
                    format!("dispute,{client},{tx},")
                }
                16..=18 => {
                    let (client, tx) = rng.earlier(&disputed, (client, tx));
                    format!("resolve,{client},{tx},")
                }
                _ => {
                    let (client, tx) = rng.earlier(&disputed, (client, tx));
                    format!("chargeback,{client},{tx},")
                }
            };
            csv.push_str(&line);
            csv.push('\n');
            sent.push(line);
        }
        csv
    }

    /// Generated histories, every kind of row, leave no findings.
    #[rstest]
    #[tokio::test]
    async fn generated_histories_verify_clean(#[values(1, 2, 3, 4, 5, 6, 7, 8)] seed: u64) {
        let store = Store::in_memory().await.unwrap();
        let tally = ingest(&generated(seed, 400), &store).await;
        assert!(
            tally.applied > 0 && tally.duplicates > 0 && tally.rejected > 0,
            "{tally:?}"
        );
        let rows = Entity::find().all(store.db()).await.unwrap();
        for kind in [
            OpKind::Deposit,
            OpKind::Withdrawal,
            OpKind::Dispute,
            OpKind::Resolve,
            OpKind::Chargeback,
        ] {
            assert!(rows.iter().any(|r| r.kind == kind), "no {kind:?}");
        }
        assert!(rows.iter().any(|r| r.rejected_reason.is_some()));
        assert_eq!(Model::verify(&store).await.unwrap(), Vec::<String>::new());
    }
}
