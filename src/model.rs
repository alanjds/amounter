use serde::Deserialize;

use crate::Amount;

pub type ClientId = u16;
pub type TxId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

/// One input CSV row. `amount` is empty for dispute/resolve/chargeback.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Record {
    #[serde(rename = "type")]
    pub kind: Kind,
    pub client: ClientId,
    pub tx: TxId,
    #[serde(default)]
    pub amount: Option<Amount>,
}
