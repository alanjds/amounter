# amounter

Reads a transactions CSV, streams it into a database, applies ledger rules, and
streams a per-client account CSV to stdout.

It is a reusable library (`amounter`) plus a thin CLI. The pipeline works on any
`AsyncRead`/`AsyncWrite`, so a TCP front end can reuse it unchanged, and storage
goes through SeaORM, so moving from SQLite to networked Postgres is a URL change.

## Usage

```sh
amounter transactions.csv                      # throwaway in-memory SQLite
cat transactions.csv | amounter -              # stdin (`-`, `--`, or no argument)
amounter --database-url 'sqlite://ledger.db?mode=rwc' transactions.csv
DATABASE_URL=postgres://user:pw@host/db amounter transactions.csv
RUST_LOG=error amounter transactions.csv         # only errors on stderr (no rejection lines)
LOG_FORMAT=json amounter transactions.csv      # structured logs on stderr
amounter verify --database-url 'sqlite://ledger.db?mode=rwc'  # audit, read-only
```

State persists in the database between runs, so a second run continues the ledger
of the first (tx ids stay unique across runs).

### Input

```csv
type, client, tx, amount
deposit, 1, 1, 1.0
withdrawal, 1, 2, 0.5
dispute, 1, 1
```

- Columns: `type,client,tx,amount`. Whitespace around fields is ignored.
- `type`: `deposit`, `withdrawal`, `dispute`, `resolve`, `chargeback`.
- `client` is a `u16`, `tx` a `u32`.
- `amount` is an unsigned decimal with up to 4 decimal places, required on
  `deposit` and `withdrawal` (zero is fine). Dispute-like rows may leave it empty
  or omit the field; an amount given there is ignored.

### Output

```csv
client,available,held,total,locked
1,1.5000,0.0000,1.5000,false
2,2.0000,0.0000,2.0000,false
```

The header is always the first line, even with no accounts. Amounts always have
4 decimals. **Rows are in no particular order** (sort them if you need to diff runs).

### Ledger rules

- Deposit / withdrawal carry an amount (zero is fine) and need a tx id not used before.
  An identical replay (same client, type and amount) is ignored and counted as a
  duplicate; the same id with different content is rejected.
- A withdrawal needs sufficient available funds. A rejected deposit/withdrawal is
  remembered (`rejected_reason` in `transactions`), so its tx id cannot be reused
  and it cannot be disputed.
- A locked account (after a chargeback) rejects deposits and withdrawals. Dispute,
  resolve and chargeback are still accepted on a locked account.
- Only deposits can be disputed (a dispute of a withdrawal is rejected): dispute
  moves available → held, resolve moves held → available, chargeback removes the
  held funds and locks the account.
- A dispute may drive `available` negative (deposit 2.0, withdraw 1.5, dispute the
  deposit: available -1.5, held 2.0, total 0.5). This is the only way a balance
  goes negative; withdrawals still need `available` to cover them.
- Dispute, resolve and chargeback must reference a transaction of the same client,
  in the right state.

A rule violation *rejects* the record: it is skipped and reported on stderr as
a `WARN` log line with `row` and `reason` (see [Logging](#logging)). Malformed CSV, a deposit/withdrawal without an amount, or a database
failure is fatal (non-zero exit, logged as `ERROR`).

### Redelivery caveat

Deposits and withdrawals carry a unique tx id, so they can be made idempotent
under at-least-once delivery, and are.
Dispute, resolve and chargeback rows carry no id of their own, so they **cannot**
be deduplicated: a redelivered `dispute` that arrives after a `resolve` disputes
the transaction again. Within a state machine they are only protected by the
state checks (e.g. a second `chargeback` is rejected because nothing is held).

## Audit journal

Every committed change appends one row to the `journal` table, in the same
database transaction as the change and under the same account lock: applied
deposits, withdrawals, disputes, resolves and chargebacks, and the
deposit/withdrawal tombstones (with their `rejected_reason`). Identical replays
and the rejections that roll back (dispute-like ones, a conflicting duplicate
id) change nothing and are not journaled.

| Column | |
| --- | --- |
| `seq` | autoincrement primary key: the global order (gaps are possible on Postgres) |
| `client`, `tx` | as in the operation (for a dispute-like row, the referenced tx) |
| `kind` | 0 deposit, 1 withdrawal, 2 dispute, 3 resolve, 4 chargeback |
| `amount` | the operation amount; for a dispute-like row, the referenced transaction's |
| `rejected_reason` | `NULL` when applied, else the tombstone's rejection code |
| `available_after`, `held_after`, `locked_after` | the account right after the operation |
| `at` | unix milliseconds; informational, no check reads it |

So each row is a snapshot of its account, and a client's previous state is its
previous row (or the all-zero account).

`amounter verify` (read-only) streams the journal in `seq` order and checks that
each row follows from the client's previous one by the account rules (the same
methods ingest uses), then that every `accounts` row equals its client's last
row (a client without rows must be all zero). Memory is O(clients); one pass over
`journal` and one over `accounts`. It prints one plain line per discrepancy on
stdout and exits non-zero if there is any; clean means no output:

```text
journal seq 3 (client 1, tx 3): available should be 1.5000, is 9.0000
accounts client 1: held should be 2.0000, is 0.0000
```

Limitations:

- Detection only: nothing rebuilds the tables from the journal, and `verify` does
  not look at `transactions` (a wrong dispute state, a missing or stray row).
- The journal lives in the same database: whoever can rewrite the tables can
  rewrite the journal too.
- No tamper-proofing beyond continuity. A consistent rewrite of a client's rows
  from some row to the end passes the journal check, and an appended consistent
  row shows only against `accounts`. A deleted row that changes no balance (a
  tombstone, a zero amount, a dispute with its resolve) is not seen at all. A
  hash chain would be the next step.
- Not a full input log: duplicates and rolled-back rejections are not journaled.
- A database written before the journal existed has no rows for that history:
  `verify` reports those accounts.
- Run `verify` on a quiet database. It reads in one database transaction, a
  snapshot on SQLite but not under Postgres' default isolation.

## Logging

Logs use [`tracing`](https://docs.rs/tracing). Set `LOG_FORMAT` and `RUST_LOG` to control it.

Default:

```text
2026-11-11T18:22:50.073063Z  WARN amounter: rejected row=2 reason=insufficient available funds
```

With `LOG_FORMAT=json`:

```json
{"timestamp":"2026-11-11T15:55:31.899968Z","level":"WARN","fields":{"message":"rejected","row":2,"reason":"insufficient available funds"},"target":"amounter"}
```

## Library

Three independent stages, each a lazy `Stream`:

```rust
use amounter::{Store, Outcome, entity::account, format::csv, pipeline};
use futures_util::TryStreamExt;

let store = Store::connect("sqlite://ledger.db?mode=rwc").await?;
let ops = csv::decode(reader);                                     // Stream<(Seq, Operation)>
let mut outcomes = Box::pin(pipeline::ingest(store.clone(), ops)); // Stream<(Seq, Outcome)>
let mut tally = pipeline::IngestionReport::default();             // optional
while let Some((row, outcome)) = outcomes.try_next().await? {
    tally.record(&outcome);
    if let Outcome::Rejected(why) = outcome { tracing::warn!(row, %why, "rejected") }
}
// Independent of the above (could be another process):
let summaries = account::Model::summaries(&store).await?;
csv::encode_report(Box::pin(summaries), &mut writer).await?;
```

- The library never installs a logger; install a `tracing` subscriber in your
  binary to see its events.
- `Store` is cheap to `clone()` (it shares the connection pool).
- `decode` reads one row at a time and `ingest` applies each operation in its own
  database transaction, locking the client's account row (`SELECT ... FOR UPDATE`),
  so memory is independent of input size and concurrent streams on the same
  client do not lose updates. `ingest` alone knows the transaction size.
- After the first error (malformed row, missing amount, database failure) both
  streams end without pulling or applying anything else; operations before it
  stay committed.
- The report streams accounts from the database; closing the writer is left to
  the caller.
- Money is a fixed-point `Amount` (integer ten-thousandths); no floats anywhere.

A TCP server is a loop around the same stages (this sketch compiles):

```rust
let store = Store::connect("sqlite::memory:").await?;
let listener = TcpListener::bind("127.0.0.1:4000").await?;
loop {
    let (socket, _) = listener.accept().await?;
    let store = store.clone();
    tokio::spawn(async move {
        let (reader, mut writer) = socket.into_split();
        let mut outcomes = Box::pin(pipeline::ingest(store.clone(), csv::decode(reader)));
        while let Some((row, outcome)) = outcomes.try_next().await? {
            if let Outcome::Rejected(why) = outcome { tracing::warn!(row, %why, "rejected") }
        }
        let summaries = account::Model::summaries(&store).await?;
        csv::encode_report(Box::pin(summaries), &mut writer).await?;
        writer.shutdown().await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });
}
```

## Development

The toolchain is pinned in `rust-toolchain.toml`.

```sh
cargo nextest run
cargo clippy --all-targets
cargo fmt --check
```

Tests use rstest (cases and fixtures) and indoc (multi-line CSV). The Postgres
concurrency test is skipped unless `AMOUNTER_TEST_POSTGRES` holds the URL of a
scratch database (its tables are truncated):

```sh
AMOUNTER_TEST_POSTGRES=postgres://user:pw@localhost/scratch cargo nextest run
```

### Layout

| Path | Role |
| --- | --- |
| `src/amount.rs` | fixed-point `Amount` |
| `src/model.rs` | `Operation`, id types |
| `src/format/csv.rs` | CSV wire format: `decode`, `encode_report` |
| `src/entity/` | SeaORM entities (`accounts`, `transactions`, `journal`) with the ledger rules as methods, the report row `account::Summary`, `Model::summaries` and the audit `journal::Model::verify` |
| `src/store.rs` | connection handling, table creation |
| `src/engine.rs` | applies one operation: DB transaction, account lock, rules, save, journal row |
| `src/pipeline.rs` | `ingest` (operations to outcomes) and the run `IngestionReport` |
| `src/main.rs` | clap CLI (ingest, `verify`) |
| `src/telemetry.rs` | CLI-only `tracing` subscriber setup (text/JSON, filters) |
| `tests/` | CLI, pipeline and concurrency tests; `common/` shared fixtures |

## Known limitations

- **Throughput:** one commit per record. About 20k rows/s on SQLite, but only
  ~600 rows/s against a local Postgres. Batching (a savepoint per record inside a
  larger transaction) or sharding by client would help.
- **Not atomic on a bad row:** rows are applied as they stream, so a malformed row
  aborts the run (non-zero exit) after the earlier rows are already committed.
  This is deliberate: throughput (batching) is preferred over early error
  detection, so do not rely on all-or-nothing input handling. Operations before the
  bad row stay committed, and no report is printed.
- **Schema:** tables are created with `IF NOT EXISTS`; there are no migrations.
- **SQLite** uses a single connection, so concurrency there is serialized; row
  locks only matter on Postgres. The report stream holds that only connection until
  it is dropped, so a slow reader stalls other users of the same `Store`.
- Output order is unspecified.
