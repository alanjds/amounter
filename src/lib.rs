//! Transaction-ledger processing library.
//!
//! Three independent, lazy stages, fully async and transport-agnostic:
//!
//! 1. [`format::csv::decode`]: any [`tokio::io::AsyncRead`] of CSV rows into a
//!    stream of [`Operation`]s;
//! 2. [`pipeline::ingest`]: operations into [`Outcome`]s, applied to a [`Store`]
//!    (SeaORM: SQLite or PostgreSQL) one database transaction each;
//! 3. [`entity::account::Model::summaries`] into [`format::csv::encode_report`]:
//!    the account report to any [`tokio::io::AsyncWrite`], independent of 2.
//!
//! The CLI wires the stages itself; a TCP server gives each connection a clone
//! of the same [`Store`].

pub mod amount;
pub mod engine;
pub mod entity;
pub mod error;
pub mod format;
pub mod model;
pub mod pipeline;
pub mod store;

pub use amount::Amount;
pub use engine::Outcome;
pub use error::{Error, Rejection, Result};
pub use model::{ClientId, Operation, Seq, TxId};
pub use pipeline::IngestionReport;
pub use store::Store;
