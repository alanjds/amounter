use sea_orm::entity::prelude::*;

use crate::Amount;

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

/// A persisted deposit/withdrawal, kept so later disputes can reference it.
/// A row with `rejected_reason` set records a rejected attempt: it only keeps
/// the tx id taken and is never disputable.
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "transactions")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub tx: i64,
    pub client: i32,
    pub kind: TxKind,
    pub amount: Amount,
    pub state: TxState,
    /// `None` for an accepted transaction, else the `Rejection` variant name.
    pub rejected_reason: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
