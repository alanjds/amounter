//! Business rules.
//!
//! Each record runs in its own database transaction that locks the client's
//! account row (`SELECT … FOR UPDATE` on PostgreSQL), so concurrent writers
//! for the same client are serialized and no balance update is lost.
//!
//! A dispute-family record rejected by a rule rolls back and leaves no trace. A
//! deposit/withdrawal rejected by a rule is kept as a tombstone row in
//! `transactions` (with `rejected_reason` set, balances untouched), so its tx id
//! stays taken.
//!
//! Rules:
//! - Deposits/withdrawals need an amount (zero is fine) and an unused tx id. A
//!   replay of an identical, already-seen deposit/withdrawal is an ignored
//!   [`Outcome::Duplicate`]; the same id with different content is rejected.
//! - Withdrawals need sufficient *available* funds.
//! - Locked accounts reject deposits and withdrawals.
//! - Only deposits can be disputed (available -> held), resolved
//!   (held -> available) or charged back (held removed, account locked). Rejected
//!   deposits/withdrawals are not real transactions and cannot be disputed.
//! - Dispute-family records must reference a tx belonging to the same client.

use sea_orm::{
    ActiveModelTrait, DatabaseTransaction, DbErr, EntityTrait, QuerySelect, Set, TransactionTrait,
    TryInsertResult, Unchanged,
};

use crate::{
    Amount, Error, Rejection, Result, Store,
    entity::{
        account,
        transaction::{self, TxKind, TxState},
    },
    model::{Kind, Record},
};

/// What happened to a record that broke no rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The record changed the ledger.
    Applied,
    /// An identical deposit/withdrawal was already seen; ignored (at-least-once
    /// delivery).
    Duplicate,
}

/// Result of applying one record: `Ok(Ok(_))` accepted, `Ok(Err(_))` rejected by
/// a business rule, `Err(_)` fatal.
pub type Applied = Result<std::result::Result<Outcome, Rejection>>;

/// How a transaction that reached its end (and so is committed) finished.
enum Step {
    Done(Outcome),
    /// A deposit/withdrawal rejected by a rule; its tombstone row is committed.
    Rejected(Rejection),
}

pub async fn apply(store: &Store, rec: &Record) -> Applied {
    let txn = store.db().begin().await?;
    match apply_in(&txn, rec).await {
        Ok(step) => {
            txn.commit().await?;
            Ok(match step {
                Step::Done(outcome) => Ok(outcome),
                Step::Rejected(reason) => Err(reason),
            })
        }
        Err(Failure::Rejected(r)) => {
            txn.rollback().await?;
            Ok(Err(r))
        }
        // Dropping the transaction rolls it back.
        Err(Failure::Fatal(e)) => Err(e),
    }
}

enum Failure {
    Rejected(Rejection),
    Fatal(Error),
}
impl From<Rejection> for Failure {
    fn from(r: Rejection) -> Self {
        Failure::Rejected(r)
    }
}
impl From<DbErr> for Failure {
    fn from(e: DbErr) -> Self {
        Failure::Fatal(e.into())
    }
}

fn add(a: Amount, b: Amount) -> std::result::Result<Amount, Rejection> {
    a.checked_add(b).ok_or(Rejection::Overflow)
}
fn sub(a: Amount, b: Amount) -> std::result::Result<Amount, Rejection> {
    a.checked_sub(b).ok_or(Rejection::Overflow)
}

async fn apply_in(txn: &DatabaseTransaction, rec: &Record) -> std::result::Result<Step, Failure> {
    let client = i32::from(rec.client);
    let tx_id = i64::from(rec.tx);

    // Make sure the row exists, then lock it for the rest of the transaction.
    // Rolling back on rejection removes a row created here.
    account::Entity::insert(account::ActiveModel {
        client: Set(client),
        available: Set(Amount::ZERO),
        held: Set(Amount::ZERO),
        locked: Set(false),
    })
    .on_conflict_do_nothing()
    .exec(txn)
    .await?;
    let mut acct = account::Entity::find_by_id(client)
        .lock_exclusive()
        .one(txn)
        .await?
        .ok_or_else(|| DbErr::RecordNotFound(format!("account {client}")))?;

    match rec.kind {
        Kind::Deposit | Kind::Withdrawal => {
            let kind = if rec.kind == Kind::Deposit {
                TxKind::Deposit
            } else {
                TxKind::Withdrawal
            };
            let rejection = match rec.amount {
                None => Some(Rejection::InvalidAmount),
                Some(_) if acct.locked => Some(Rejection::AccountLocked),
                Some(a) if kind == TxKind::Withdrawal && acct.available < a => {
                    Some(Rejection::InsufficientFunds)
                }
                Some(a) if kind == TxKind::Deposit && add(acct.available, a).is_err() => {
                    Some(Rejection::Overflow)
                }
                Some(_) => None,
            };
            let amount = rec.amount.unwrap_or(Amount::ZERO);
            // The primary key makes tx ids unique even across concurrent writers.
            // Rejected records are stored too (with their reason) so the id stays
            // taken.
            let inserted = transaction::Entity::insert(transaction::ActiveModel {
                tx: Set(tx_id),
                client: Set(client),
                kind: Set(kind),
                amount: Set(amount),
                state: Set(TxState::Normal),
                rejected_reason: Set(rejection.map(|r| format!("{r:?}"))),
            })
            .on_conflict_do_nothing()
            .exec(txn)
            .await?;
            if matches!(inserted, TryInsertResult::Conflicted) {
                let seen = transaction::Entity::find_by_id(tx_id).one(txn).await?;
                let replay = seen
                    .is_some_and(|t| t.client == client && t.kind == kind && t.amount == amount);
                return if replay {
                    Ok(Step::Done(Outcome::Duplicate))
                } else {
                    Err(Rejection::DuplicateTx.into())
                };
            }
            if let Some(reason) = rejection {
                return Ok(Step::Rejected(reason));
            }
            if kind == TxKind::Deposit {
                acct.available = add(acct.available, amount)?;
            } else {
                acct.available = sub(acct.available, amount)?;
            }
        }
        Kind::Dispute | Kind::Resolve | Kind::Chargeback => {
            let stored = transaction::Entity::find_by_id(tx_id)
                .one(txn)
                .await?
                .filter(|t| t.client == client && t.rejected_reason.is_none())
                .ok_or(Rejection::UnknownTx)?;
            let new_state = match rec.kind {
                Kind::Dispute => {
                    if stored.kind != TxKind::Deposit {
                        return Err(Rejection::NotDisputable.into());
                    }
                    if stored.state != TxState::Normal {
                        return Err(Rejection::AlreadyDisputed.into());
                    }
                    acct.available = sub(acct.available, stored.amount)?;
                    acct.held = add(acct.held, stored.amount)?;
                    TxState::Disputed
                }
                Kind::Resolve => {
                    if stored.state != TxState::Disputed {
                        return Err(Rejection::NotDisputed.into());
                    }
                    acct.held = sub(acct.held, stored.amount)?;
                    acct.available = add(acct.available, stored.amount)?;
                    TxState::Normal
                }
                _ => {
                    if stored.state != TxState::Disputed {
                        return Err(Rejection::NotDisputed.into());
                    }
                    acct.held = sub(acct.held, stored.amount)?;
                    acct.locked = true;
                    TxState::ChargedBack
                }
            };
            transaction::ActiveModel {
                tx: Unchanged(tx_id),
                state: Set(new_state),
                ..Default::default()
            }
            .update(txn)
            .await?;
        }
    }

    account::ActiveModel {
        client: Unchanged(client),
        available: Set(acct.available),
        held: Set(acct.held),
        locked: Set(acct.locked),
    }
    .update(txn)
    .await?;
    Ok(Step::Done(Outcome::Applied))
}
