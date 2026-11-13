//! SeaORM entities. Unsigned ids are widened (u16 -> i32, u32 -> i64) because
//! PostgreSQL has no unsigned integer types.

pub mod account;
pub mod journal;
pub mod transaction;
