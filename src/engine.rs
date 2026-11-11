//! Business rules.
//!
//! Each record runs in its own database transaction that locks the client's
//! account row (`SELECT … FOR UPDATE` on PostgreSQL), so concurrent writers
//! for the same client are serialized and no balance update is lost. A record
//! rejected by a rule rolls back and leaves no trace.
//!
//! Rules:
//! - Deposits/withdrawals need a positive amount and a unique tx id.
//! - Withdrawals need sufficient *available* funds.
//! - Locked accounts reject deposits and withdrawals.
//! - Only deposits can be disputed (available -> held), resolved
//!   (held -> available) or charged back (held removed, account locked).
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

/// Outcome of applying one record: `Ok(Ok(()))` applied, `Ok(Err(_))` rejected
/// by a business rule, `Err(_)` fatal.
pub type Applied = Result<std::result::Result<(), Rejection>>;

pub async fn apply(store: &Store, rec: &Record) -> Applied {
    let txn = store.db().begin().await?;
    match apply_in(&txn, rec).await {
        Ok(()) => {
            txn.commit().await?;
            Ok(Ok(()))
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

async fn apply_in(txn: &DatabaseTransaction, rec: &Record) -> std::result::Result<(), Failure> {
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
            let amount = rec
                .amount
                .filter(|a| a.is_positive())
                .ok_or(Rejection::InvalidAmount)?;
            if acct.locked {
                return Err(Rejection::AccountLocked.into());
            }
            let kind = if rec.kind == Kind::Deposit {
                TxKind::Deposit
            } else {
                TxKind::Withdrawal
            };
            // The primary key makes tx ids unique even across concurrent writers.
            let inserted = transaction::Entity::insert(transaction::ActiveModel {
                tx: Set(tx_id),
                client: Set(client),
                kind: Set(kind),
                amount: Set(amount),
                state: Set(TxState::Normal),
            })
            .on_conflict_do_nothing()
            .exec(txn)
            .await?;
            if matches!(inserted, TryInsertResult::Conflicted) {
                return Err(Rejection::DuplicateTx.into());
            }
            if kind == TxKind::Deposit {
                acct.available = add(acct.available, amount)?;
            } else {
                if acct.available < amount {
                    return Err(Rejection::InsufficientFunds.into());
                }
                acct.available = sub(acct.available, amount)?;
            }
        }
        Kind::Dispute | Kind::Resolve | Kind::Chargeback => {
            let stored = transaction::Entity::find_by_id(tx_id)
                .one(txn)
                .await?
                .filter(|t| t.client == client)
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
    Ok(())
}
