//! The CSV wire format: `type,client,tx,amount` in, account report out.

use csv_async::{AsyncReaderBuilder, AsyncWriterBuilder, Trim};
use futures_util::{Stream, StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Deserializer, de::Error as _};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::{Amount, ClientId, Seq, TxId, entity::account::Summary, model::Operation};

/// Errors of the CSV stages.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Malformed CSV, unknown `type`, bad amount, or a failed read.
    #[error("csv error: {0}")]
    Csv(#[from] csv_async::Error),
    #[error("row {row}: {kind} without an amount")]
    MissingAmount { row: Seq, kind: &'static str },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// The report input failed.
    #[error(transparent)]
    Store(#[from] crate::Error),
}

/// Decodes `type,client,tx,amount` rows into operations
///
/// Items carry their 1-based data row number.
/// Yields at most one `Err` then ends without reading another record
pub fn decode<'r, R: AsyncRead + Unpin + Send + 'r>(
    input: R,
) -> impl Stream<Item = Result<(Seq, Operation), Error>> + Send + 'r {
    let rows = AsyncReaderBuilder::new()
        .flexible(true)
        .trim(Trim::All)
        .create_deserializer(input)
        .into_deserialize::<Row>();

    stream::unfold(Some((Box::pin(rows), 0u64)), |state| async move {
        let (mut rows, n) = state?; // None: finished, do not pull
        let seq = n + 1;
        match rows.next().await? {
            Err(e) => Some((Err(Error::Csv(e)), None)),
            Ok(row) => {
                let kind = row.kind;
                Some(match row.into_operation() {
                    Some(op) => (Ok((seq, op)), Some((rows, seq))),
                    None => (
                        Err(Error::MissingAmount {
                            row: seq,
                            kind: kind.into(),
                        }),
                        None,
                    ),
                })
            }
        }
    })
    .fuse() // `Unfold` panics if polled after it ended
}

/// Because Serde refuses to write it when the output CSV is empty
const REPORT_HEADER: &str = "client,available,held,total,locked\n";

/// Writes the account report CSV
///
/// Flushes `out` but "closing" is for the caller.
pub async fn encode_report<S, W>(mut summaries: S, mut out: W) -> Result<(), Error>
where
    S: Stream<Item = crate::Result<Summary>> + Unpin,
    W: AsyncWrite + Unpin + Send,
{
    out.write_all(REPORT_HEADER.as_bytes()).await?;
    let mut csv = AsyncWriterBuilder::new()
        .has_headers(false) // already written above
        .create_serializer(out);
    while let Some(summary) = summaries.try_next().await? {
        csv.serialize(summary).await?;
    }
    csv.flush().await?;
    Ok(())
}

/// One input CSV row. `amount` is empty for dispute/resolve/chargeback.
#[derive(Debug, Clone, Copy, Deserialize)]
struct Row {
    #[serde(rename = "type", deserialize_with = "case_insensitive_kind")]
    kind: RowKind,
    client: ClientId,
    tx: TxId,
    #[serde(default)]
    amount: Option<Amount>,
}

/// The `type` column, but "kind" :) Matched ignoring ASCII case.
#[derive(Debug, Clone, Copy, strum::IntoStaticStr, strum::EnumString)]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
enum RowKind {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

fn case_insensitive_kind<'de, D: Deserializer<'de>>(d: D) -> Result<RowKind, D::Error> {
    String::deserialize(d)?.parse().map_err(D::Error::custom)
}

impl Row {
    /// `None` for a deposit/withdrawal without an amount.
    /// A dispute-like row ignores an amount if present.
    fn into_operation(self) -> Option<Operation> {
        let Self {
            kind,
            client,
            tx,
            amount,
        } = self;
        Some(match kind {
            RowKind::Deposit => Operation::Deposit {
                client,
                tx,
                amount: amount?,
            },
            RowKind::Withdrawal => Operation::Withdrawal {
                client,
                tx,
                amount: amount?,
            },
            RowKind::Dispute => Operation::Dispute { client, tx },
            RowKind::Resolve => Operation::Resolve { client, tx },
            RowKind::Chargeback => Operation::Chargeback { client, tx },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::account;
    use futures_util::FutureExt;
    use rstest::rstest;
    use tokio::io::duplex;

    fn row(kind: RowKind, amount: Option<Amount>) -> Row {
        Row {
            kind,
            client: 1,
            tx: 2,
            amount,
        }
    }
    const A: Amount = Amount::from_raw(15_000);

    #[rstest]
    #[case::deposit(RowKind::Deposit, Some(A), Some(Operation::Deposit { client: 1, tx: 2, amount: A }))]
    #[case::withdrawal(RowKind::Withdrawal, Some(A), Some(Operation::Withdrawal { client: 1, tx: 2, amount: A }))]
    #[case::deposit_zero(RowKind::Deposit, Some(Amount::ZERO), Some(Operation::Deposit { client: 1, tx: 2, amount: Amount::ZERO }))]
    #[case::deposit_no_amount(RowKind::Deposit, None, None)]
    #[case::withdrawal_no_amount(RowKind::Withdrawal, None, None)]
    #[case::dispute(RowKind::Dispute, None, Some(Operation::Dispute { client: 1, tx: 2 }))]
    #[case::resolve(RowKind::Resolve, None, Some(Operation::Resolve { client: 1, tx: 2 }))]
    #[case::chargeback(RowKind::Chargeback, None, Some(Operation::Chargeback { client: 1, tx: 2 }))]
    #[case::dispute_amount_dropped(RowKind::Dispute, Some(A), Some(Operation::Dispute { client: 1, tx: 2 }))]
    fn into_operation_cases(
        #[case] kind: RowKind,
        #[case] amount: Option<Amount>,
        #[case] expected: Option<Operation>,
    ) {
        assert_eq!(row(kind, amount).into_operation(), expected);
    }

    async fn decoded(csv: &str) -> Vec<Result<(Seq, Operation), Error>> {
        decode(csv.as_bytes()).collect().await
    }

    #[tokio::test]
    async fn decode_numbers_rows_and_trims() {
        let got =
            decoded("type, client, tx, amount\ndeposit, 1, 1, 1.5\ndispute, 1, 1\nresolve,1,1,\n")
                .await;
        let got: Vec<_> = got.into_iter().map(Result::unwrap).collect();
        assert_eq!(
            got,
            [
                (
                    1,
                    Operation::Deposit {
                        client: 1,
                        tx: 1,
                        amount: Amount::from_raw(15_000)
                    }
                ),
                (2, Operation::Dispute { client: 1, tx: 1 }),
                (3, Operation::Resolve { client: 1, tx: 1 }),
            ]
        );
    }

    #[rstest]
    #[case::capitalized("Deposit")]
    #[case::upper("DEPOSIT")]
    #[case::mixed("dEpOsIt")]
    #[tokio::test]
    async fn decode_type_ignores_case(#[case] kind: &str) {
        let got = decoded(&format!("type,client,tx,amount\n{kind},1,1,1.0\n")).await;
        assert!(
            matches!(got.as_slice(), [Ok((1, Operation::Deposit { .. }))]),
            "{got:?}"
        );
    }

    #[rstest]
    #[case::unknown_type("bogus,1,1,1.0")]
    #[case::negative_amount("deposit,1,1,-1.0")]
    #[case::non_numeric_client("deposit,x,1,1.0")]
    #[tokio::test]
    async fn decode_bad_row_is_a_csv_error(#[case] row: &str) {
        let got = decoded(&format!("type,client,tx,amount\n{row}\n")).await;
        assert!(matches!(got.as_slice(), [Err(Error::Csv(_))]), "{got:?}");
    }

    #[rstest]
    #[case::header_only("type,client,tx,amount\n")]
    #[case::empty("")]
    #[tokio::test]
    async fn decode_without_rows_is_empty(#[case] csv: &str) {
        assert!(decoded(csv).await.is_empty());
    }

    #[rstest]
    #[case::bad_type("bogus,1,2,1.0", true)]
    #[case::missing_amount("deposit,1,2,", false)]
    #[tokio::test]
    async fn decode_ends_after_the_first_error(#[case] bad: &str, #[case] csv_error: bool) {
        let got = decoded(&format!(
            "type,client,tx,amount\ndeposit,1,1,1.0\n{bad}\ndeposit,1,3,1.0\n"
        ))
        .await;
        assert_eq!(got.len(), 2, "{got:?}");
        assert!(got[0].is_ok());
        match &got[1] {
            Err(Error::Csv(_)) => assert!(csv_error),
            Err(Error::MissingAmount {
                row: 2,
                kind: "deposit",
            }) => assert!(!csv_error),
            other => panic!("{other:?}"),
        }
    }

    /// After an error, no further record is read: the peer below never closes,
    /// so a read would be pending instead of ready.
    #[tokio::test]
    async fn decode_does_not_read_after_an_error() {
        let (mut peer, reader) = duplex(1024);
        peer.write_all(b"type,client,tx,amount\ndeposit,1,1,1.0\nbogus,1,2,1.0\n")
            .await
            .unwrap();
        let mut ops = Box::pin(decode(reader));
        assert!(ops.next().await.unwrap().is_ok());
        assert!(ops.next().await.unwrap().is_err());
        assert!(ops.next().now_or_never().expect("must not wait").is_none());
        drop(peer);
    }

    async fn encoded(items: Vec<crate::Result<Summary>>) -> (Vec<u8>, Result<(), Error>) {
        let mut out = Vec::new();
        let res = encode_report(stream::iter(items), &mut out).await;
        (out, res)
    }

    #[tokio::test]
    async fn encode_report_writes_header_even_when_empty() {
        let (out, res) = encoded(vec![]).await;
        res.unwrap();
        assert_eq!(out, REPORT_HEADER.as_bytes());
    }

    #[tokio::test]
    async fn encode_report_writes_rows_with_four_decimals() {
        let mut m = account::Model::new(7);
        m.available = Amount::from_raw(15_000);
        let (out, res) = encoded(vec![m.summary()]).await;
        res.unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            format!("{REPORT_HEADER}7,1.5000,0.0000,1.5000,false\n")
        );
    }

    #[tokio::test]
    async fn encode_report_aborts_on_a_failed_item() {
        let (out, res) = encoded(vec![Err(crate::Error::Overflow)]).await;
        assert!(matches!(res, Err(Error::Store(crate::Error::Overflow))));
        assert_eq!(out, REPORT_HEADER.as_bytes());
    }

    /// `REPORT_HEADER` is written by hand, so check it still matches the column
    /// names serde derives from `Summary`.
    #[tokio::test]
    async fn report_header_matches_summary_fields() {
        let mut csv = AsyncWriterBuilder::new().create_serializer(Vec::new());
        csv.serialize(account::Model::new(1).summary().unwrap())
            .await
            .unwrap();
        let out = String::from_utf8(csv.into_inner().await.unwrap()).unwrap();
        assert_eq!(out.lines().next().unwrap(), REPORT_HEADER.trim_end());
    }
}
