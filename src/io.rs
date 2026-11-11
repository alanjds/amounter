//! Transport-agnostic pipeline: `AsyncRead` -> store -> `AsyncWrite`.

use csv_async::{AsyncReaderBuilder, AsyncWriterBuilder, StringRecord};
use futures_util::TryStreamExt;
use sea_orm::EntityTrait;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::{Amount, Error, Rejection, Result, Store, engine, entity::account, model::Record};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Summary {
    pub applied: u64,
    pub rejected: u64,
}

/// Streams CSV records from `input` into `store`, applying business rules.
///
/// Rows are read one at a time, so memory use is independent of input size.
/// `on_reject(row, reason)` is called for each rule violation (`row` is the
/// 1-based data row); those rows are skipped. Malformed CSV or database
/// failures abort with an error. Records of one stream are applied in order.
pub async fn process<R: AsyncRead + Unpin + Send>(
    input: R,
    store: &Store,
    mut on_reject: impl FnMut(u64, &Rejection),
) -> Result<Summary> {
    let mut reader = AsyncReaderBuilder::new()
        .flexible(true) // dispute rows may omit the trailing amount field
        .create_reader(input);
    let mut headers = reader.headers().await?.clone();
    headers.trim();

    let mut summary = Summary::default();
    let mut row = 0u64;
    let mut line = StringRecord::new();
    while reader.read_record(&mut line).await? {
        line.trim();
        let rec: Record = line.deserialize(Some(&headers))?;
        row += 1;
        match engine::apply(store, &rec).await? {
            Ok(()) => summary.applied += 1,
            Err(reason) => {
                summary.rejected += 1;
                on_reject(row, &reason);
            }
        }
    }
    Ok(summary)
}

/// One line of the account report; field names are the CSV columns.
#[derive(Serialize)]
struct ReportRow {
    client: i32,
    available: Amount,
    held: Amount,
    total: Amount,
    locked: bool,
}

/// Matches the field names of [`ReportRow`]. Written up front because a serde
/// CSV writer only emits headers together with the first row, and the report
/// must have a header even when there are no accounts.
const REPORT_HEADER: &str = "client,available,held,total,locked\n";

impl TryFrom<account::Model> for ReportRow {
    type Error = Error;

    fn try_from(a: account::Model) -> Result<Self> {
        let total = a
            .total()
            .ok_or_else(|| Error::Io(std::io::Error::other("total overflow")))?;
        Ok(ReportRow {
            client: a.client,
            available: a.available,
            held: a.held,
            total,
            locked: a.locked,
        })
    }
}

/// Writes the account report as CSV (`client,available,held,total,locked`),
/// streaming rows from the database in no particular order. The header is
/// always the first line, even with no accounts. Flushes `output` but leaves closing it
/// (e.g. a socket shutdown) to the caller.
pub async fn write_report<W: AsyncWrite + Unpin + Send>(
    store: &Store,
    mut output: W,
) -> Result<()> {
    output.write_all(REPORT_HEADER.as_bytes()).await?;
    let mut csv = AsyncWriterBuilder::new()
        .has_headers(false) // already written above
        .create_serializer(output);
    let mut accounts = account::Entity::find().stream(store.db()).await?;
    while let Some(account) = accounts.try_next().await? {
        csv.serialize(ReportRow::try_from(account)?).await?;
    }
    csv.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `REPORT_HEADER` is written by hand (see its docs), so check it still
    /// matches the column names serde derives from `ReportRow`.
    #[tokio::test]
    async fn report_header_matches_report_row_fields() {
        let mut csv = AsyncWriterBuilder::new().create_serializer(Vec::new());
        csv.serialize(ReportRow {
            client: 1,
            available: Amount::ZERO,
            held: Amount::ZERO,
            total: Amount::ZERO,
            locked: false,
        })
        .await
        .unwrap();
        let out = String::from_utf8(csv.into_inner().await.unwrap()).unwrap();
        assert_eq!(out.lines().next().unwrap(), REPORT_HEADER.trim_end());
    }
}
