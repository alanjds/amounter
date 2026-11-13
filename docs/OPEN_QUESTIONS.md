# Open questions

Behaviors the spec does not pin down, and the choice made for now. Revisit with
the Project Owner.

## Ledger semantics

1. **Dispute when funds were already withdrawn.** A dispute moves the deposit's
   amount from `available` to `held` without checking `available` is large enough,
   so `available` can go negative (deposit 2.0, withdraw 1.5, dispute the deposit:
   `available = -1.5`, `held = 2.0`, `total = 0.5`). Decided: allow it, since
   the funds really are in dispute; rejecting would let a client withdraw disputed
   money and dodge the dispute. Negative balances come from disputes only;
   withdrawal rules are unchanged. Pinned by tests in `tests/pipeline.rs`.
2. **Disputes on withdrawals.** Only deposits are disputable; disputing a
   withdrawal is rejected (`NotDisputable`).
3. **Locked accounts.** A chargeback locks the account, which then rejects
   deposits and withdrawals. Dispute / resolve / chargeback on a locked account
   are currently still accepted (the engine only checks the lock for deposits and
   withdrawals). Decide explicitly. Only a chargeback on a locked account is
   pinned (a unit test in `src/entity/account.rs`); dispute and resolve on one,
   and the whole path through the engine, are not.
4. **Lifecycle after resolve.** Current behavior: a resolved transaction returns
   to normal and can be disputed again; a charged-back one can no longer be
   disputed, resolved or charged back. Pinned by unit tests in
   `src/entity/transaction.rs` (`dispute_rules`, `resolve_and_chargeback_rules`).
5. **Zero amounts.** Deposits and withdrawals of `0` are accepted (decided); the
   client then appears in the report with zeros. A missing amount is malformed input (item 10).
6. **Tx id scope.** Tx ids are globally unique across clients; a deposit or
   withdrawal reusing any earlier id is rejected, even for a different client.
   A dispute-like row naming another client's tx is rejected.
7. **Redelivered rows (at-least-once delivery).** Implemented: a deposit/withdrawal
   replaying an already-seen tx id with the same client, kind and amount is
   accepted and ignored (idempotent no-op, counted separately from rejections);
   the same id with different content stays a rejection (conflict). Dispute-like
   rows carry no id of their own, so they cannot be deduplicated in general (a
   redelivered `dispute` after a `resolve` would dispute again).
8. **Rejected transactions keep their tx id.** Implemented: a rejected
   deposit/withdrawal is persisted in `transactions` with a nullable
   `rejected_reason` column (NULL = accepted), so the id stays taken and the
   reason is kept for audit. Consequences: rejection of these rows no longer
   "leaves no trace", the client's account row is created too (it shows up in the
   report with zeros), and rejected rows are not disputable. Existing databases
   need the new column (no migrations yet, see below).

## Input handling

9. **Unknown `type` is fatal.** An unknown type, negative amount or unparsable
   client aborts the run. Alternative: skip and report the row like a rule
   rejection. Fatal was chosen so bad data is not silently dropped.
10. **Missing amount on deposit/withdrawal.** Implemented: it is
    malformed input, a fatal decode error in the CSV adapter (`row N: deposit
    without an amount`), not a business rule. It used to be a rule rejection
    (`InvalidAmount`) that also claimed the tx id. Defaulting to 0 was rejected:
    a truncated row would become a zero deposit that takes the tx id, and the
    correct redelivery would then be rejected as a conflicting duplicate. An explicit `0` stays valid.
11. **A malformed row aborts the run but is not atomic.** Rows are applied as they
    stream, so rows before the bad one stay committed and the run exits non-zero.
    Decided: prefer throughput (batching) over early error detection, so with
    batching the rows of the batch in flight when the error hits need a defined
    outcome (rolled back and retried or bisected; see item 14). Documented in the
    README.

## Design debt

12. **Amount range.** `Amount` is an `i64` count of ten-thousandths, so one amount
   or balance tops out at 922337203685477.5807 (~922 trillion units). Anything
   larger (including a balance sum) is rejected as `Overflow` rather than
   wrapped. A decimal crate would not widen this on SQLite (see 13).

13. **`Amount` is hand-rolled** (`i64` ten-thousandths). Decision: keep it for now.
    A `rust_decimal` spike (`Amount(Decimal)`, stored as `i64`) was evaluated:
    - Behavior, output and speed were identical (1M rows, ~47 s either way).
    - It removed only ~15 lines (the tail of `from_str` and `Display`).
      Digit validation stays ours, because `Decimal::from_str` accepts `+1`, `1e3`,
      `.5` and `1_0`. Checked add/sub were already one-liners.
    - It added code: a `bounded()` range clamp and five manual SeaORM impls in
      place of `DeriveValueType` (`src/amount.rs` went from 135 to 208 lines,
      including tests), plus two dependencies.
    - SeaORM maps `Decimal` to REAL (f64) on SQLite, which silently loses
      precision (`922337203685477.5807` came back as `922337203685478`).
      Enabling `with-rust_decimal` or changing the column type would do this
      without any error.
    Revisit if Postgres `NUMERIC` becomes the primary backend: stored natively,
    a decimal type would drop the clamp and the manual impls and widen the range.

14. **Throughput and batching.** One commit per record gives ~600 rows/s on a
    local Postgres versus ~20k rows/s on SQLite. Options: a savepoint per record
    inside batched transactions, or sharding streams by client. Batching trades
    per-record isolation and crash granularity for speed: on error, a whole
    batch must be retried or bisected, and row locks are held longer. Stress runs
    (examples/gen_load.rs) show ~22k rows/s uncontended on in-memory SQLite, i.e.
    days for a full u32-range input; the in-memory backend also keeps every tx
    (~23 bytes/row) in RAM. Only `pipeline::ingest` knows the transaction size, so
    batching changes that one function (`apply` already nests as a savepoint inside
    an outer transaction). Batches hold several account locks until commit: see
    item 23.
15. **No migrations.** Tables are created with `IF NOT EXISTS`.
16. **Output order is unspecified**; fine for the spec, but sort before diffing
    two runs.

## Tech

17. **Logging.** Implemented: `tracing` replaces `eprintln!`. The lib only emits
    events/spans; the binary installs the subscriber (`src/telemetry.rs`). Text by
    default, JSON with `LOG_FORMAT=json`, filter from `RUST_LOG` (default `warn`),
    stdout stays CSV-only
    (see README, "Logging").
18. **OTEL export (follow-up, not implemented).** Export traces to an OTLP endpoint.
    Notes for whoever picks it up:
    - optional `otlp` cargo feature; no OpenTelemetry crate in the default build;
    - OTLP over http-proto with rustls preferred;
    - standard `OTEL_EXPORTER_OTLP_*` env vars; add the layer only when an
      endpoint is configured;
    - bounded flush/shutdown before exit, inside the tokio runtime;
    - separate per-layer filter, so stderr logging and export can differ;
    - pin the opentelemetry family together: `tracing-opentelemetry`,
      `opentelemetry`, `opentelemetry_sdk` and `opentelemetry-otlp` versions move
      in lockstep;
    - PII rule: client and tx ids are fine as attributes, never the database URL.
19. **Rejection floods.** One line per rejected row, no rate limit or sampling. A
    bad input can produce billions of stderr lines; consider a cap, sampling or
    counting only after N rejections.
20. **Per-record span cost.** The `record` span (TRACE) sits in the hot loop.
    Disabled at the default filter it showed no measurable cost on 1M rows, but its
    cost when enabled (and with an OTEL layer) at billions of rows is unmeasured.

## Architecture

21. **Rejected operations still roll back (deferred).** A rejected dispute-like
    row, and a conflicting duplicate id from a brand-new client, are undone by
    rolling back their database transaction (that is what removes the account row
    inserted up front). The alternative ("every operation commits": the dispute
    family looks the account up without inserting it, and a conflicting duplicate
    deletes an account row it just created) removes the need for savepoints once
    batching exists and gives an audit journal a single hook. It is deferred
    because the delete-if-inserted case is unverified on Postgres (a concurrent
    first insert for the same client). Revisit together with batching (item 14)
    and the audit trail (item 22), with a Postgres test first.
22. **Audit trail.** Implemented, minimal (README, "Audit journal"): an
    append-only `journal` table in the same database, one row per committed
    change (applied operations and tombstones) with the account right after it,
    written by the engine in the same transaction under the account lock.
    `verify` checks the rows' continuity by the account rules (memory
    O(clients)) and the live `accounts` against the last rows, printing one line
    per discrepancy. No migrations, triggers, runs, digests or rebuild. Left out
    on purpose, in the order I would add them:
    - **Transactions.** `verify` does not look at `transactions`. A replay of the
      journal through all the rules (memory O(transactions)) would compare them,
      and could back a `rebuild` of both tables.
    - **Hash chain.** Continuity does not detect a consistent rewrite of a
      client's rows from some point to the end, or a deleted row that changes no
      balance (tombstone, zero amount, a dispute with its resolve). Chaining each
      row to the previous row's hash (and publishing the head out of band) is
      the next step.
    - **Snapshots / tail replay.** A periodic checkpoint of the tables would
      bound a replay to the tail.
    - **Another store.** The journal shares the database (and its failure and
      access domain) with the tables; shipping it elsewhere (another database,
      append-only object storage) would make it independent evidence.
    - **Rolled-back rejections** are not journaled (item 21), so the journal is
      not a full input log; "every operation commits" would give them a row.
    - **Pre-journal databases** have history the journal does not hold: `verify`
      reports it; no marker tells them apart.
23. **Postgres batch deadlocks.** With batching, one transaction holds several
    client row locks. Options: lock clients in sorted order, retry the whole
    batch on deadlock (40P01/40001), shard streams by client, or a combination.
    Undecided; irrelevant while each operation commits on its own.
