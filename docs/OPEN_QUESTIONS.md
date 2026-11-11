# Open questions

Behaviors the spec does not pin down, and the choice made for now. Revisit with
the Project Owner.

## Ledger semantics

1. **Dispute when funds were already withdrawn.** A dispute moves the deposit's
   amount from `available` to `held` without checking `available` is large enough,
   so `available` can go negative (deposit 2.0, withdraw 1.5, dispute the deposit:
   `available = -1.5`, `held = 2.0`, `total = 0.5`). Current choice: allow it, since
   the funds really are in dispute; rejecting would let a client withdraw disputed
   money and dodge the dispute. No test pins this down yet.
2. **Disputes on withdrawals.** Only deposits are disputable; disputing a
   withdrawal is rejected (`NotDisputable`).
3. **Locked accounts.** A chargeback locks the account, which then rejects
   deposits and withdrawals. Dispute / resolve / chargeback on a locked account
   are currently still accepted (the engine only checks the lock for deposits and
   withdrawals). Decide explicitly; no test pins this down.
4. **Lifecycle after resolve.** Current behavior: a resolved transaction returns
   to normal and can be disputed again; a charged-back one can no longer be
   disputed, resolved or charged back. No tests pin this down yet.
5. **Zero amounts.** Deposits and withdrawals of `0` are accepted (decided); the
   client then appears in the report with zeros. Missing amounts are rejected.
6. **Tx id scope.** Tx ids are globally unique across clients; a deposit or
   withdrawal reusing any earlier id is rejected, even for a different client.
   A dispute-family row naming another client's tx is rejected.
7. **Redelivered rows (at-least-once delivery).** Implemented: a deposit/withdrawal
   replaying an already-seen tx id with the same client, kind and amount is
   accepted and ignored (idempotent no-op, counted separately from rejections);
   the same id with different content stays a rejection (conflict). Dispute-family
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
10. **Missing amount on deposit/withdrawal** is a rule rejection (`InvalidAmount`),
    not a fatal error. Confirm the intended behavior.

## Design debt

11. **Amount range.** `Amount` is an `i64` count of ten-thousandths, so one amount
   or balance tops out around 922 trillion units. Anything larger (including a
   balance sum) is rejected as `Overflow` rather than wrapped. Re-check the limit
   when moving to a money crate.

12. **`Amount` is hand-rolled** (`i64` ten-thousandths). Plan: replace it with an
   existing money/decimal crate, keeping 4-decimal output and the checked
   add/sub semantics. The `DeriveValueType` / serde impls in `src/amount.rs` are
   the surface to port.
13. **Throughput and batching.** One commit per record gives ~600 rows/s on a
    local Postgres versus ~20k rows/s on SQLite. Options: a savepoint per record
    inside batched transactions, or sharding streams by client. Batching trades
    per-record isolation and crash granularity for speed: on error, a whole
    batch must be retried or bisected, and row locks are held longer. Stress runs
    (examples/gen_load.rs) show ~22k rows/s uncontended on in-memory SQLite, i.e.
    days for a full u32-range input; the in-memory backend also keeps every tx
    (~23 bytes/row) in RAM.
14. **No migrations.** Tables are created with `IF NOT EXISTS`.
15. **Output order is unspecified**; fine for the spec, but sort before diffing
    two runs.

## Tech

16. **Logging is `eprintln!`.** Replace print-style output with `tracing`: the lib
    only emits events/spans, the binary installs the subscriber. Text by default,
    JSON when `LOG_FORMAT=json`, and the option to export traces to an OTEL (OTLP)
    endpoint (ideally behind a cargo feature). stdout must stay CSV-only. Open
    points: how rejections, `-q` and `RUST_LOG` interact, and per-row span cost at
    billions of rows. A plan is being drafted; nothing is implemented.
