//! Stage 2 of the pipeline: operations in, outcomes out.
//!
//! Stage 1 ([`crate::format::csv::decode`]) turns bytes into operations and
//! stage 3 ([`crate::entity::account::Model::summaries`] into an encoder)
//! reports the accounts; only this stage touches the ledger.

use futures_util::{Stream, StreamExt, stream};
use tracing::Instrument;

use crate::{Operation, Outcome, Seq, Store, engine};

/// Emit an INFO progress event every this many rows.
pub const PROGRESS_EVERY: u64 = 1_000_000;

/// Applies operations to the store, one database transaction each, strictly in
/// order (so per-client order is kept). Owns `Store` (a cheap clone), so the
/// result can be spawned.
///
/// The error type is the caller's choice: source errors pass through, database
/// errors convert with `From`. Yields at most one `Err` and then ends; the stop
/// check runs BEFORE the next source pull, so nothing is pulled, let alone
/// applied, after a failure.
pub fn ingest<'s, S, E>(
    store: Store,
    ops: S,
) -> impl Stream<Item = Result<(Seq, Outcome), E>> + Send + 's
where
    S: Stream<Item = Result<(Seq, Operation), E>> + Send + 's,
    E: From<crate::Error> + Send + 's,
{
    stream::unfold(Some((Box::pin(ops), store)), |state| async move {
        let (mut ops, store) = state?; // None: finished, do not pull
        let (seq, op) = match ops.next().await? {
            Ok(item) => item,
            Err(e) => return Some((Err(e), None)),
        };
        let applied = engine::apply(store.db(), &op)
            .instrument(tracing::trace_span!("record", row = seq))
            .await;
        Some(match applied {
            Ok(outcome) => (Ok((seq, outcome)), Some((ops, store))),
            Err(e) => (Err(e.into()), None),
        })
    })
    .fuse() // `Unfold` panics if polled after it ended
}

/// Counts of an ingestion outcomes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IngestionReport {
    pub applied: u64,
    pub duplicates: u64,
    pub rejected: u64,
}

impl IngestionReport {
    pub fn record(&mut self, outcome: &Outcome) {
        match outcome {
            Outcome::Applied => self.applied += 1,
            Outcome::Duplicate => self.duplicates += 1,
            Outcome::Rejected(_) => self.rejected += 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_util::TryStreamExt;
    use rstest::rstest;
    use sea_orm::ConnectionTrait;

    use super::*;
    use crate::{Amount, Error, Rejection, entity::account};

    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error("source failed")]
        Source,
        #[error(transparent)]
        Store(#[from] Error),
    }

    const A: Amount = Amount::from_raw(10_000);
    fn deposit(client: u16, tx: u32, amount: Amount) -> Operation {
        Operation::Deposit { client, tx, amount }
    }

    async fn totals(store: &Store) -> Vec<(i32, Amount)> {
        let mut rows: Vec<_> = account::Model::summaries(store)
            .await
            .unwrap()
            .map_ok(|s| (s.client, s.total))
            .try_collect()
            .await
            .unwrap();
        rows.sort();
        rows
    }

    #[tokio::test]
    async fn outcomes_come_out_in_order_with_their_seq() {
        let store = Store::in_memory().await.unwrap();
        let ops = stream::iter([
            Ok((10, deposit(1, 1, A))),
            Ok((11, deposit(1, 1, A))),
            Ok((12, deposit(1, 1, Amount::ZERO))),
            Ok((13, Operation::Dispute { client: 1, tx: 1 })),
        ]);
        let got: Vec<_> = ingest::<_, TestError>(store, ops)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            got,
            [
                (10, Outcome::Applied),
                (11, Outcome::Duplicate),
                (12, Outcome::Rejected(Rejection::DuplicateTx)),
                (13, Outcome::Applied),
            ]
        );
    }

    #[tokio::test]
    async fn a_source_error_ends_the_stream_after_committing_earlier_items() {
        let store = Store::in_memory().await.unwrap();
        let pulled = AtomicUsize::new(0);
        let ops = stream::iter([
            Ok((1, deposit(1, 1, A))),
            Ok((2, deposit(2, 2, A))),
            Err(TestError::Source),
            Ok((4, deposit(3, 3, A))),
        ])
        .inspect(|_| {
            pulled.fetch_add(1, Ordering::Relaxed);
        });
        let mut out = Box::pin(ingest(store.clone(), ops));
        assert!(matches!(out.next().await, Some(Ok((1, Outcome::Applied)))));
        assert!(matches!(out.next().await, Some(Ok((2, Outcome::Applied)))));
        assert!(matches!(out.next().await, Some(Err(TestError::Source))));
        assert!(out.next().await.is_none());
        assert!(out.next().await.is_none());
        assert_eq!(pulled.load(Ordering::Relaxed), 3, "the fourth was pulled");
        assert_eq!(totals(&store).await, [(1, A), (2, A)]);
    }

    #[tokio::test]
    async fn a_store_error_ends_the_stream_too() {
        let store = Store::in_memory().await.unwrap();
        let pulled = AtomicUsize::new(0);
        let breaker = store.clone();
        let ops = stream::iter(0..3u32).then(|i| {
            pulled.fetch_add(1, Ordering::Relaxed);
            let breaker = breaker.clone();
            async move {
                if i == 1 {
                    breaker
                        .db()
                        .execute_unprepared("DROP TABLE accounts")
                        .await
                        .unwrap();
                }
                Ok::<_, TestError>((u64::from(i) + 1, deposit(1, i + 1, A)))
            }
        });
        let mut out = Box::pin(ingest(store, ops));
        assert!(matches!(out.next().await, Some(Ok((1, Outcome::Applied)))));
        assert!(matches!(out.next().await, Some(Err(TestError::Store(_)))));
        assert!(out.next().await.is_none());
        assert_eq!(pulled.load(Ordering::Relaxed), 2, "the third was pulled");
    }

    #[rstest]
    #[case::applied(Outcome::Applied, IngestionReport { applied: 1, ..IngestionReport::default() })]
    #[case::duplicate(Outcome::Duplicate, IngestionReport { duplicates: 1, ..IngestionReport::default() })]
    #[case::rejected(Outcome::Rejected(Rejection::Overflow), IngestionReport { rejected: 1, ..IngestionReport::default() })]
    fn tally_records(#[case] outcome: Outcome, #[case] want: IngestionReport) {
        let mut t = IngestionReport::default();
        t.record(&outcome);
        assert_eq!(t, want);
    }
}
