use crate::Amount;

pub type ClientId = u16;
pub type TxId = u32;
/// 1-based position of an operation in its input stream (CSV data row).
pub type Seq = u64;

/// One ledger operation. A deposit or withdrawal always has an amount
/// Dispute-like operations carry no amount
///
/// `OpKind` is its generated fieldless tag, stored in the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumDiscriminants)]
#[strum_discriminants(name(OpKind))]
#[strum_discriminants(derive(sea_orm::EnumIter, sea_orm::DeriveActiveEnum))]
#[strum_discriminants(sea_orm(rs_type = "i16", db_type = "SmallInteger"))]
pub enum Operation {
    #[strum_discriminants(sea_orm(num_value = 0))]
    Deposit {
        client: ClientId,
        tx: TxId,
        amount: Amount,
    },
    #[strum_discriminants(sea_orm(num_value = 1))]
    Withdrawal {
        client: ClientId,
        tx: TxId,
        amount: Amount,
    },
    #[strum_discriminants(sea_orm(num_value = 2))]
    Dispute { client: ClientId, tx: TxId },
    #[strum_discriminants(sea_orm(num_value = 3))]
    Resolve { client: ClientId, tx: TxId },
    #[strum_discriminants(sea_orm(num_value = 4))]
    Chargeback { client: ClientId, tx: TxId },
}

impl Operation {
    /// The client the operation is for.
    pub fn client(&self) -> ClientId {
        match *self {
            Self::Deposit { client, .. }
            | Self::Withdrawal { client, .. }
            | Self::Dispute { client, .. }
            | Self::Resolve { client, .. }
            | Self::Chargeback { client, .. } => client,
        }
    }

    /// The transaction id the operation carries (its own for a deposit or
    /// withdrawal, the referenced one for dispute-like ones).
    pub fn tx(&self) -> TxId {
        match *self {
            Self::Deposit { tx, .. }
            | Self::Withdrawal { tx, .. }
            | Self::Dispute { tx, .. }
            | Self::Resolve { tx, .. }
            | Self::Chargeback { tx, .. } => tx,
        }
    }
}
