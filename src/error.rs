use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Fatal errors: processing cannot continue.
#[derive(Debug, Error)]
pub enum Error {
    #[error("csv error: {0}")]
    Csv(#[from] csv_async::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("database error: {0}")]
    Db(#[from] sea_orm::DbErr),
}

/// A record that is valid CSV but breaks a business rule. It is skipped and
/// reported, and processing carries on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum Rejection {
    #[error("deposit/withdrawal without a positive amount")]
    InvalidAmount,
    #[error("duplicate transaction id")]
    DuplicateTx,
    #[error("account is locked")]
    AccountLocked,
    #[error("insufficient available funds")]
    InsufficientFunds,
    #[error("referenced transaction not found for this client")]
    UnknownTx,
    #[error("only deposits can be disputed")]
    NotDisputable,
    #[error("transaction is already disputed or charged back")]
    AlreadyDisputed,
    #[error("transaction is not under dispute")]
    NotDisputed,
    #[error("amount overflow")]
    Overflow,
}
