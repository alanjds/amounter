//! Transaction-ledger processing library.
//!
//! The pipeline is transport-agnostic and fully async: it consumes any
//! [`tokio::io::AsyncRead`] of CSV records, persists state through a
//! [`Store`] (SeaORM: SQLite or PostgreSQL), and writes the account report to
//! any [`tokio::io::AsyncWrite`]. The CLI wires these to a file/stdin and
//! stdout; a TCP server wires them to a socket and gives each connection a
//! clone of the same [`Store`].

pub mod amount;
pub mod engine;
pub mod entity;
pub mod error;
pub mod io;
pub mod model;
pub mod store;

pub use amount::Amount;
pub use engine::Outcome;
pub use error::{Error, Rejection, Result};
pub use io::{Summary, process, write_report};
pub use model::{ClientId, Kind, Record, TxId};
pub use store::Store;
