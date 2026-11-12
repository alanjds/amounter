use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Fatal errors: processing cannot continue.
#[derive(Debug, Error)]
pub enum Error {
    #[error("database error: {0}")]
    Db(#[from] sea_orm::DbErr),
    #[error("amount overflow")]
    Overflow,
}

/// A record that is valid CSV but breaks a business rule. It is skipped and
/// reported, and processing carries on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error, strum::IntoStaticStr)]
pub enum Rejection {
    #[error("transaction id already used by a different transaction")]
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

impl Rejection {
    /// Rejection code persisted in the database
    pub fn code(&self) -> &'static str {
        (*self).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    /// The persisted codes are literals on purpose: renaming a variant must not
    /// change them.
    #[rstest]
    #[case(Rejection::DuplicateTx, "DuplicateTx")]
    #[case(Rejection::AccountLocked, "AccountLocked")]
    #[case(Rejection::InsufficientFunds, "InsufficientFunds")]
    #[case(Rejection::UnknownTx, "UnknownTx")]
    #[case(Rejection::NotDisputable, "NotDisputable")]
    #[case(Rejection::AlreadyDisputed, "AlreadyDisputed")]
    #[case(Rejection::NotDisputed, "NotDisputed")]
    #[case(Rejection::Overflow, "Overflow")]
    fn codes_are_stable(#[case] r: Rejection, #[case] code: &str) {
        assert_eq!(r.code(), code);
    }
}
