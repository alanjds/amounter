use sea_orm::entity::prelude::*;

use crate::{Amount, Rejection};

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "i16", db_type = "SmallInteger")]
pub enum TxKind {
    Deposit = 0,
    Withdrawal = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "i16", db_type = "SmallInteger")]
pub enum TxState {
    Normal = 0,
    Disputed = 1,
    ChargedBack = 2,
}

/// A Deposit or Withdrawal
/// `rejected_reason` empty: Success
/// `rejected_reason` populated: Transaction failed
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "transactions")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub tx: i64,
    pub client: i32,
    pub kind: TxKind,
    pub amount: Amount,
    pub state: TxState,
    /// `None` for an accepted transaction, else the stable `Rejection::code`.
    pub rejected_reason: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

impl Model {
    /// A new, accepted transaction.
    pub fn new(tx: i64, client: i32, kind: TxKind, amount: Amount) -> Self {
        Model {
            tx,
            client,
            kind,
            amount,
            state: TxState::Normal,
            rejected_reason: None,
        }
    }

    /// Marks the transaction as a rejected attempt (a tombstone).
    pub fn rejected(self, reason: Rejection) -> Self {
        Model {
            rejected_reason: Some(reason.code().to_owned()),
            ..self
        }
    }

    /// An at-least-once redelivery: same client, kind and amount.
    pub fn is_replay_of(&self, other: &Model) -> bool {
        self.client == other.client && self.kind == other.kind && self.amount == other.amount
    }

    /// Only an accepted transaction of this client can be referenced.
    fn ensure_owned_by(&self, client: i32) -> Result<(), Rejection> {
        if self.client == client && self.rejected_reason.is_none() {
            Ok(())
        } else {
            Err(Rejection::UnknownTx)
        }
    }

    /// Normal deposit -> disputed; returns the amount to hold. Unchanged on `Err`.
    pub fn dispute(&mut self, client: i32) -> Result<Amount, Rejection> {
        self.ensure_owned_by(client)?;
        if self.kind != TxKind::Deposit {
            return Err(Rejection::NotDisputable);
        }
        if self.state != TxState::Normal {
            return Err(Rejection::AlreadyDisputed);
        }
        self.state = TxState::Disputed;
        Ok(self.amount)
    }

    /// Disputed -> normal; returns this transaction amount
    pub fn resolve(&mut self, client: i32) -> Result<Amount, Rejection> {
        self.leave_dispute(client, TxState::Normal)
    }

    /// Disputed -> charged back; returns this transaction amount
    pub fn chargeback(&mut self, client: i32) -> Result<Amount, Rejection> {
        self.leave_dispute(client, TxState::ChargedBack)
    }

    fn leave_dispute(&mut self, client: i32, to: TxState) -> Result<Amount, Rejection> {
        self.ensure_owned_by(client)?;
        if self.state != TxState::Disputed {
            return Err(Rejection::NotDisputed);
        }
        self.state = to;
        Ok(self.amount)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx(kind: TxKind, state: TxState) -> Model {
        Model {
            state,
            ..Model::new(1, 7, kind, Amount::from_raw(20_000))
        }
    }
    const A: Amount = Amount::from_raw(20_000);

    #[test]
    fn dispute_rules() {
        let mut m = tx(TxKind::Deposit, TxState::Normal);
        assert_eq!(m.dispute(7), Ok(A));
        assert_eq!(m.state, TxState::Disputed);

        for (mut m, client, want) in [
            (
                tx(TxKind::Withdrawal, TxState::Normal),
                7,
                Rejection::NotDisputable,
            ),
            (
                tx(TxKind::Deposit, TxState::Disputed),
                7,
                Rejection::AlreadyDisputed,
            ),
            (
                tx(TxKind::Deposit, TxState::ChargedBack),
                7,
                Rejection::AlreadyDisputed,
            ),
            (
                tx(TxKind::Deposit, TxState::Normal),
                8,
                Rejection::UnknownTx,
            ),
            (
                tx(TxKind::Deposit, TxState::Normal).rejected(Rejection::AccountLocked),
                7,
                Rejection::UnknownTx,
            ),
        ] {
            let before = m.clone();
            assert_eq!(m.dispute(client), Err(want));
            assert_eq!(m, before);
        }
    }

    #[test]
    fn resolve_and_chargeback_rules() {
        let mut m = tx(TxKind::Deposit, TxState::Disputed);
        assert_eq!(m.resolve(7), Ok(A));
        assert_eq!(m.state, TxState::Normal);
        assert_eq!(m.dispute(7), Ok(A)); // OQ 4: can be disputed again
        assert_eq!(m.chargeback(7), Ok(A));
        assert_eq!(m.state, TxState::ChargedBack);

        for (mut m, client, want) in [
            (
                tx(TxKind::Deposit, TxState::Normal),
                7,
                Rejection::NotDisputed,
            ),
            (
                tx(TxKind::Deposit, TxState::ChargedBack),
                7,
                Rejection::NotDisputed,
            ),
            (
                tx(TxKind::Deposit, TxState::Disputed),
                8,
                Rejection::UnknownTx,
            ),
        ] {
            let before = m.clone();
            assert_eq!(m.resolve(client), Err(want));
            assert_eq!(m.chargeback(client), Err(want));
            assert_eq!(m, before);
        }
    }

    #[test]
    fn replay_and_tombstone() {
        let m = tx(TxKind::Deposit, TxState::Normal);
        assert!(m.is_replay_of(&m.clone().rejected(Rejection::Overflow)));
        assert!(!m.is_replay_of(&Model {
            client: 8,
            ..m.clone()
        }));
        assert!(!m.is_replay_of(&Model {
            kind: TxKind::Withdrawal,
            ..m.clone()
        }));
        assert!(!m.is_replay_of(&Model {
            amount: Amount::ZERO,
            ..m.clone()
        }));
        assert_eq!(
            m.rejected(Rejection::InsufficientFunds)
                .rejected_reason
                .as_deref(),
            Some("InsufficientFunds")
        );
    }
}
