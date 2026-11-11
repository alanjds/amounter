use sea_orm::entity::prelude::*;

use crate::Amount;

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "i16", db_type = "SmallInteger")]
pub enum TxKind {
    #[sea_orm(num_value = 0)]
    Deposit,
    #[sea_orm(num_value = 1)]
    Withdrawal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "i16", db_type = "SmallInteger")]
pub enum TxState {
    #[sea_orm(num_value = 0)]
    Normal,
    #[sea_orm(num_value = 1)]
    Disputed,
    #[sea_orm(num_value = 2)]
    ChargedBack,
}

/// A persisted deposit/withdrawal, kept so later disputes can reference it.
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "transactions")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub tx: i64,
    pub client: i32,
    pub kind: TxKind,
    pub amount: Amount,
    pub state: TxState,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
