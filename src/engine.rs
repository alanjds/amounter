//! Orchestration of one operation: lock the account, run the rules, save.
//! The rules themselves are methods on the entity models
//! ([`account::Model`], [`transaction::Model`]).
//!
//! Each record runs in its own database transaction that locks the client's
//! account row (`SELECT … FOR UPDATE` on PostgreSQL), so concurrent writers
//! for the same client are serialized and no balance update is lost.
//!
//! A dispute-like record rejected by a rule rolls back and leaves no trace. A
//! deposit/withdrawal rejected by a rule is kept as a tombstone row in
//! `transactions` (with `rejected_reason` set, balances untouched), so its tx id
//! stays taken.
//!
//! Rules:
//! - Deposits/withdrawals carry an amount (zero is fine; the decoder rejects a
//!   missing one) and need an unused tx id.
//!   A replay of an identical, already-seen deposit/withdrawal is an ignored
//!   [`Outcome::Duplicate`]; the same id with different content is rejected.
//! - Withdrawals need sufficient *available* funds.
//! - Locked accounts reject deposits and withdrawals.
//! - Only real (not rejected) deposits can be disputed, resolved or charged back.
//! - Dispute-like records must reference a tx belonging to the same client.

use sea_orm::{
    ActiveModelTrait, ConnectionTrait, DbErr, EntityTrait, QuerySelect, TransactionSession,
    TransactionTrait, TryInsertResult,
};

use crate::{
    Rejection, Result,
    entity::{
        account,
        transaction::{self, TxKind},
    },
    model::Operation,
};

/// What happened to an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The operation changed the ledger.
    Applied,
    /// Expecting at-lest-once deliveries, which can produce duplicates.
    Duplicate,
    /// A business rule refused it; see the module docs for what is kept.
    Rejected(Rejection),
}

/// Applies one operation in its own transaction
///
/// Rule violations are `Ok(Outcome::Rejected(_))`.
/// `Err` is fatal, possibly from a database error;
pub(crate) async fn apply<C: TransactionTrait>(conn: &C, op: &Operation) -> Result<Outcome> {
    let db = conn.begin().await?;
    // On `Err` the dropped transaction rolls back.
    match apply_in(&db, op).await? {
        Ok(outcome) => {
            db.commit().await?;
            Ok(outcome)
        }
        Err(reason) => {
            db.rollback().await?;
            Ok(Outcome::Rejected(reason))
        }
    }
}

/// `Ok(Ok(_))`: commit
///     (including `Outcome::Rejected` from a deposit/withdrawal tombstone).
/// `Ok(Err(_))`: rejected, the caller must roll back.
async fn apply_in<T: ConnectionTrait>(
    db: &T,
    op: &Operation,
) -> Result<std::result::Result<Outcome, Rejection>> {
    let (client, tx_id) = (i32::from(op.client()), i64::from(op.tx()));

    // Make sure the row exists, then lock it for the rest of the transaction.
    // Rolling back on rejection removes a row created here.
    account::Entity::insert(account::ActiveModel::from(account::Model::new(client)).reset_all())
        .on_conflict_do_nothing()
        .exec(db)
        .await?;

    let mut account = account::Entity::find_by_id(client)
        .lock_exclusive()
        .one(db)
        .await?
        .ok_or_else(|| DbErr::RecordNotFound(format!("account {client}")))?;

    match *op {
        Operation::Deposit { amount, .. } | Operation::Withdrawal { amount, .. } => {
            let (kind, outcome) = match *op {
                Operation::Deposit { .. } => (TxKind::Deposit, account.deposit(amount)),
                Operation::Withdrawal { .. } => (TxKind::Withdrawal, account.withdraw(amount)),
                _ => unreachable!("outer matches only deposit or withdrawal"),
            };
            let mut stored = transaction::Model::new(tx_id, client, kind, amount);
            if let Err(reason) = outcome {
                stored = stored.rejected(reason);
            }

            // Rejected records stored w/ reason: ids of failures are burnt.
            let insertion = transaction::Entity::insert(
                transaction::ActiveModel::from(stored.clone()).reset_all(),
            )
            .on_conflict_do_nothing()
            .exec(db)
            .await?;

            if matches!(insertion, TryInsertResult::Conflicted) {
                let existing = transaction::Entity::find_by_id(tx_id).one(db).await?;
                return Ok(if existing.is_some_and(|tx| tx.is_replay_of(&stored)) {
                    Ok(Outcome::Duplicate) // Swallowing dupes as "correctly applied before"
                } else {
                    Err(Rejection::DuplicateTx) // tx_id "reuses" are not dupes. Error it.
                });
            }
            if let Err(why) = outcome {
                return Ok(Ok(Outcome::Rejected(why)));
            }
        }
        Operation::Dispute { .. } | Operation::Resolve { .. } | Operation::Chargeback { .. } => {
            let Some(mut stored) = transaction::Entity::find_by_id(tx_id).one(db).await? else {
                // Found not the transaction this dispute-thing targets
                return Ok(Err(Rejection::UnknownTx));
            };
            let outcome = match op {
                Operation::Dispute { .. } => stored.dispute(client)
                                                    .and_then(|a| account.hold(a)),
                Operation::Resolve { .. } => stored.resolve(client)
                                                    .and_then(|a| account.release(a)),
                &Operation::Chargeback { .. } => stored.chargeback(client)
                                                       .and_then(|a| account.chargeback(a)),
                _ => unreachable!("outer matches only Dispute, Resolve or Chargeback"),
            };
            if let Err(reason) = outcome {
                return Ok(Err(reason));
            }
            transaction::ActiveModel::from(stored)
                .reset_all()
                .update(db)
                .await?;
        }
    }

    account::ActiveModel::from(account)
        .reset_all()
        .update(db)
        .await?;
    Ok(Ok(Outcome::Applied))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;
    use Operation::*;

    const A: crate::Amount = crate::Amount::from_raw(10_000);

    #[tokio::test]
    async fn apply_returns_outcomes() {
        let store = Store::in_memory().await.unwrap();
        for (op, want) in [
            (
                Deposit {
                    client: 1,
                    tx: 1,
                    amount: A,
                },
                Outcome::Applied,
            ),
            (
                Deposit {
                    client: 1,
                    tx: 1,
                    amount: A,
                },
                Outcome::Duplicate,
            ),
            (
                Deposit {
                    client: 1,
                    tx: 1,
                    amount: crate::Amount::ZERO,
                },
                Outcome::Rejected(Rejection::DuplicateTx),
            ),
            (
                Dispute { client: 1, tx: 9 },
                Outcome::Rejected(Rejection::UnknownTx),
            ),
            (
                Withdrawal {
                    client: 1,
                    tx: 2,
                    amount: crate::Amount::from_raw(20_000),
                },
                Outcome::Rejected(Rejection::InsufficientFunds),
            ),
            (Dispute { client: 1, tx: 1 }, Outcome::Applied),
            (Chargeback { client: 1, tx: 1 }, Outcome::Applied),
            (
                Deposit {
                    client: 1,
                    tx: 3,
                    amount: A,
                },
                Outcome::Rejected(Rejection::AccountLocked),
            ),
        ] {
            assert_eq!(apply(store.db(), &op).await.unwrap(), want, "{op:?}");
        }
    }

    /// A nested `begin` is a savepoint: a rolled-back rejection leaves no trace
    /// inside an outer transaction, while the rest of it commits.
    async fn outer_transaction_keeps_savepoints(store: &Store) {
        let outer = store.db().begin().await.unwrap();
        let rejected = apply(&outer, &Dispute { client: 5, tx: 1 }).await.unwrap();
        assert_eq!(rejected, Outcome::Rejected(Rejection::UnknownTx));
        let applied = apply(
            &outer,
            &Deposit {
                client: 1,
                tx: 1,
                amount: A,
            },
        )
        .await;
        assert_eq!(applied.unwrap(), Outcome::Applied);
        // SQLite has one connection: commit before reading.
        outer.commit().await.unwrap();
        let clients: Vec<_> = account::Entity::find()
            .all(store.db())
            .await
            .unwrap()
            .into_iter()
            .map(|a| a.client)
            .collect();
        assert_eq!(clients, [1]);
    }

    #[tokio::test]
    async fn apply_inside_an_outer_transaction() {
        outer_transaction_keeps_savepoints(&Store::in_memory().await.unwrap()).await;
    }

    /// Same, on Postgres, when `AMOUNTER_TEST_POSTGRES` is set (scratch database).
    #[tokio::test]
    async fn apply_inside_an_outer_transaction_postgres() {
        let Ok(url) = std::env::var("AMOUNTER_TEST_POSTGRES") else {
            return;
        };
        let db = sea_orm::Database::connect(&url).await.unwrap();
        let store = Store::from_connection(db.clone()).await.unwrap();
        db.execute_unprepared("TRUNCATE accounts, transactions")
            .await
            .unwrap();
        outer_transaction_keeps_savepoints(&store).await;
    }
}
