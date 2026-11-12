use futures_util::{Stream, StreamExt};
use sea_orm::entity::prelude::*;
use serde::Serialize;

use crate::{Amount, Error, Rejection, Result, Store};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "accounts")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub client: i32,
    pub available: Amount,
    pub held: Amount,
    pub locked: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

impl Model {
    pub fn new(client: i32) -> Self {
        Model {
            client,
            available: Amount::ZERO,
            held: Amount::ZERO,
            locked: false,
        }
    }

    pub fn total(&self) -> Option<Amount> {
        self.available.checked_add(self.held)
    }

    /// The report row; `Error::Overflow` when total doesn't fit.
    pub fn summary(&self) -> Result<Summary> {
        Ok(Summary {
            client: self.client,
            available: self.available,
            held: self.held,
            total: self.total().ok_or(Error::Overflow)?,
            locked: self.locked,
        })
    }

    /// Streams the report row of every account, unordered
    pub async fn summaries(
        store: &Store,
    ) -> Result<impl Stream<Item = Result<Summary>> + Send + '_> {
        let accounts = Entity::find().stream(store.db()).await?;
        Ok(accounts.map(|a| a?.summary()))
    }

    // Every rule method below leaves the model unchanged on `Err`.

    pub fn deposit(&mut self, a: Amount) -> Result<(), Rejection> {
        if self.locked {
            return Err(Rejection::AccountLocked);
        }
        self.available = add(self.available, a)?;
        Ok(())
    }

    pub fn withdraw(&mut self, a: Amount) -> Result<(), Rejection> {
        if self.locked {
            return Err(Rejection::AccountLocked);
        }
        if self.available < a {
            return Err(Rejection::InsufficientFunds);
        }
        self.available = sub(self.available, a)?;
        Ok(())
    }

    /// Dispute: available -> held. May drive `available` negative
    pub fn hold(&mut self, a: Amount) -> Result<(), Rejection> {
        let (available, held) = (sub(self.available, a)?, add(self.held, a)?);
        (self.available, self.held) = (available, held);
        Ok(())
    }

    /// Resolve: held -> available.
    pub fn release(&mut self, a: Amount) -> Result<(), Rejection> {
        let (available, held) = (add(self.available, a)?, sub(self.held, a)?);
        (self.available, self.held) = (available, held);
        Ok(())
    }

    /// Chargeback: the held funds leave. LOCKS THE ACCOUNT
    pub fn chargeback(&mut self, a: Amount) -> Result<(), Rejection> {
        self.held = sub(self.held, a)?;
        self.locked = true;
        Ok(())
    }
}

/// One line of the account report; the field order is the column order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Summary {
    pub client: i32,
    pub available: Amount,
    pub held: Amount,
    pub total: Amount,
    pub locked: bool,
}

// Just to let stuff less verbose...

fn add(a: Amount, b: Amount) -> Result<Amount, Rejection> {
    a.checked_add(b).ok_or(Rejection::Overflow)
}

fn sub(a: Amount, b: Amount) -> Result<Amount, Rejection> {
    a.checked_sub(b).ok_or(Rejection::Overflow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    const MAX: Amount = Amount::from_raw(i64::MAX);
    const fn amt(raw: i64) -> Amount {
        Amount::from_raw(raw)
    }
    fn acct(available: i64, held: i64, locked: bool) -> Model {
        Model {
            available: amt(available),
            held: amt(held),
            locked,
            ..Model::new(1)
        }
    }

    #[test]
    fn summary_computes_total() {
        let s = acct(15_000, 5_000, true).summary().unwrap();
        assert_eq!(s.total, amt(20_000));
        assert_eq!(
            serde_json::to_string(&s).unwrap(),
            r#"{"client":1,"available":"1.5000","held":"0.5000","total":"2.0000","locked":true}"#
        );
    }

    #[test]
    fn summary_overflow() {
        let m = Model {
            available: MAX,
            held: amt(1),
            ..Model::new(1)
        };
        assert!(matches!(m.summary(), Err(Error::Overflow)));
    }

    #[rstest]
    #[case::ok(acct(5, 0, false), 3, Ok(acct(8, 0, false)))]
    #[case::zero(acct(5, 0, false), 0, Ok(acct(5, 0, false)))]
    #[case::locked(acct(5, 0, true), 3, Err(Rejection::AccountLocked))]
    #[case::overflow(Model { available: MAX, ..Model::new(1) }, 1, Err(Rejection::Overflow))]
    fn deposit(#[case] mut m: Model, #[case] a: i64, #[case] want: Result<Model, Rejection>) {
        let before = m.clone();
        let got = m.deposit(amt(a)).map(|()| m.clone());
        assert_eq!(got, want);
        if want.is_err() {
            assert_eq!(m, before);
        }
    }

    #[rstest]
    #[case::ok(acct(5, 0, false), 3, Ok(acct(2, 0, false)))]
    #[case::exact(acct(5, 0, false), 5, Ok(acct(0, 0, false)))]
    #[case::zero(acct(0, 0, false), 0, Ok(acct(0, 0, false)))]
    #[case::insufficient(acct(5, 0, false), 6, Err(Rejection::InsufficientFunds))]
    #[case::locked(acct(5, 0, true), 3, Err(Rejection::AccountLocked))]
    #[case::locked_beats_insufficient(acct(5, 0, true), 6, Err(Rejection::AccountLocked))]
    fn withdraw(#[case] mut m: Model, #[case] a: i64, #[case] want: Result<Model, Rejection>) {
        let before = m.clone();
        let got = m.withdraw(amt(a)).map(|()| m.clone());
        assert_eq!(got, want);
        if want.is_err() {
            assert_eq!(m, before);
        }
    }

    #[rstest]
    #[case::hold(acct(5, 1, false), Model::hold, Ok(acct(3, 3, false)))]
    // Decided in OQ 1: a dispute may drive available negative.
    #[case::hold_negative(acct(1, 0, false), Model::hold, Ok(acct(-1, 2, false)))]
    #[case::hold_overflow(Model { held: MAX, ..Model::new(1) }, Model::hold, Err(Rejection::Overflow))]
    #[case::release(acct(3, 3, false), Model::release, Ok(acct(5, 1, false)))]
    #[case::release_overflow(Model { available: MAX, held: amt(2), ..Model::new(1) }, Model::release, Err(Rejection::Overflow))]
    #[case::chargeback(acct(3, 3, false), Model::chargeback, Ok(acct(3, 1, true)))]
    // Current behavior, see OQ 3.
    #[case::chargeback_when_locked(acct(0, 2, true), Model::chargeback, Ok(acct(0, 0, true)))]
    #[case::chargeback_overflow(Model { held: amt(i64::MIN), ..Model::new(1) }, Model::chargeback, Err(Rejection::Overflow))]
    fn dispute_family(
        #[case] mut m: Model,
        #[case] f: fn(&mut Model, Amount) -> Result<(), Rejection>,
        #[case] want: Result<Model, Rejection>,
    ) {
        let before = m.clone();
        let got = f(&mut m, amt(2)).map(|()| m.clone());
        assert_eq!(got, want);
        if want.is_err() {
            assert_eq!(m, before);
        }
    }
}
