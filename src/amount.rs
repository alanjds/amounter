use std::{fmt, str::FromStr};

use sea_orm::DeriveValueType;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const SCALE: i64 = 10_000;
const DECIMALS: usize = 4;

/// Fixed-point money with 4 decimal places, stored as integer ten-thousandths.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, DeriveValueType)]
pub struct Amount(i64);

impl Amount {
    pub const ZERO: Amount = Amount(0);

    pub const fn from_raw(raw: i64) -> Self {
        Amount(raw)
    }
    pub const fn raw(self) -> i64 {
        self.0
    }
    pub fn is_positive(self) -> bool {
        self.0 > 0
    }
    pub fn checked_add(self, o: Amount) -> Option<Amount> {
        self.0.checked_add(o.0).map(Amount)
    }
    pub fn checked_sub(self, o: Amount) -> Option<Amount> {
        self.0.checked_sub(o.0).map(Amount)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid amount {0:?}: expected an unsigned decimal with up to 4 decimal places")]
pub struct ParseAmountError(String);

impl FromStr for Amount {
    type Err = ParseAmountError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || ParseAmountError(s.to_owned());
        let (whole, frac) = match s.split_once('.') {
            Some((w, f)) => (w, f),
            None => (s, ""),
        };
        let digits = |p: &str| p.bytes().all(|b| b.is_ascii_digit());
        if whole.is_empty()
            || !digits(whole)
            || !digits(frac)
            || frac.len() > DECIMALS
            || (s.contains('.') && frac.is_empty())
        {
            return Err(err());
        }
        let whole: i64 = whole.parse().map_err(|_| err())?;
        let frac_val: i64 = if frac.is_empty() {
            0
        } else {
            // Right-pad to 4 digits: "5" -> 5000.
            format!("{frac:0<DECIMALS$}").parse().map_err(|_| err())?
        };
        whole
            .checked_mul(SCALE)
            .and_then(|w| w.checked_add(frac_val))
            .map(Amount)
            .ok_or_else(err)
    }
}

impl fmt::Display for Amount {
    /// Always prints 4 decimals.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.0 < 0 { "-" } else { "" };
        let abs = self.0.unsigned_abs();
        let scale = SCALE as u64;
        write!(f, "{sign}{}.{:04}", abs / scale, abs % scale)
    }
}

impl Serialize for Amount {
    /// Same text as [`Display`](fmt::Display): fixed 4 decimals.
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Amount {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("0", 0)]
    #[case("1", 10_000)]
    #[case("1.5", 15_000)]
    #[case("1.0001", 10_001)]
    #[case("0.0001", 1)]
    #[case("123.4567", 1_234_567)]
    fn parses_valid(#[case] input: &str, #[case] raw: i64) {
        assert_eq!(input.parse::<Amount>().unwrap().raw(), raw);
    }

    #[rstest]
    #[case("")]
    #[case(".")]
    #[case("1.")]
    #[case(".5")]
    #[case("-1")]
    #[case("+1")]
    #[case("1.23456")]
    #[case("abc")]
    #[case("1e3")]
    #[case("99999999999999999999")]
    fn rejects_invalid(#[case] input: &str) {
        assert!(input.parse::<Amount>().is_err());
    }

    #[rstest]
    #[case(0, "0.0000")]
    #[case(15_000, "1.5000")]
    #[case(1, "0.0001")]
    #[case(-25_000, "-2.5000")]
    #[case(-1, "-0.0001")]
    fn displays_fixed_point(#[case] raw: i64, #[case] expected: &str) {
        assert_eq!(Amount::from_raw(raw).to_string(), expected);
    }
}
