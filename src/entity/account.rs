use sea_orm::entity::prelude::*;

use crate::Amount;

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
}
