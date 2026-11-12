use crate::Amount;

pub type ClientId = u16;
pub type TxId = u32;
/// 1-based position of an operation in its input stream (CSV data row).
pub type Seq = u64;

/// One ledger operation. A deposit or withdrawal always has an amount (zero is
/// fine); the dispute-like ones carry none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Deposit {
        client: ClientId,
        tx: TxId,
        amount: Amount,
    },
    Withdrawal {
        client: ClientId,
        tx: TxId,
        amount: Amount,
    },
    Dispute {
        client: ClientId,
        tx: TxId,
    },
    Resolve {
        client: ClientId,
        tx: TxId,
    },
    Chargeback {
        client: ClientId,
        tx: TxId,
    },
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
