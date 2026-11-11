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
- `amount` is an unsigned decimal with up to 4 decimal places. Dispute-family rows
  may leave it empty or omit the field.

### Output

```csv
client,available,held,total,locked
1,1.5000,0.0000,1.5000,false
2,2.0000,0.0000,2.0000,false
```

The header is always the first line, even with no accounts. Amounts always have
4 decimals. **Rows are in no particular order** (sort them if you need to diff runs).

### Ledger rules

- Deposit / withdrawal need an amount (zero is fine) and a tx id not used before.
  An identical replay (same client, type and amount) is ignored and counted as a
  duplicate; the same id with different content is rejected.
- A withdrawal needs sufficient available funds. A rejected deposit/withdrawal is
  remembered (`rejected_reason` in `transactions`), so its tx id cannot be reused
  and it cannot be disputed.
- A locked account (after a chargeback) rejects deposits and withdrawals. Dispute,
  resolve and chargeback are still accepted on a locked account.
- Only deposits can be disputed: dispute moves available → held, resolve moves
  held → available, chargeback removes the held funds and locks the account.
- Dispute, resolve and chargeback must reference a transaction of the same client,
  in the right state.

A rule violation *rejects* the record: it is skipped and reported on stderr as
a `WARN` log line with `row` and `reason` (see [Logging](#logging)). Malformed CSV or a database
failure is fatal (non-zero exit, logged as `ERROR`).

### Redelivery caveat

Deposits and withdrawals carry a unique tx id, so they can be made idempotent
under at-least-once delivery, and are.
Dispute, resolve and chargeback rows carry no id of their own, so they **cannot**
be deduplicated: a redelivered `dispute` that arrives after a `resolve` disputes
the transaction again. Within a state machine they are only protected by the
state checks (e.g. a second `chargeback` is rejected because nothing is held).

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

```rust
use amounter::{Store, process, write_report};

let store = Store::connect("sqlite://ledger.db?mode=rwc").await?;
let summary = process(reader, &store, |row, why| tracing::warn!(row, %why, "rejected")).await?;
write_report(&store, &mut writer).await?;
```

- The library never installs a logger; install a `tracing` subscriber in your
  binary to see its events.
- `Store` is cheap to `clone()` (it shares the connection pool).
- `process` reads one row at a time (memory is independent of input size) and
  applies each record in its own database transaction, locking the client's
  account row (`SELECT ... FOR UPDATE`) so concurrent streams on the same client
  do not lose updates.
- `write_report` streams accounts from the database; closing the writer is left to
  the caller.
- Money is a fixed-point `Amount` (integer ten-thousandths); no floats anywhere.

A TCP server is a loop around the same two calls (this sketch compiles):

```rust
let store = Store::connect("sqlite::memory:").await?;
let listener = TcpListener::bind("127.0.0.1:4000").await?;
loop {
    let (socket, _) = listener.accept().await?;
    let store = store.clone();
    tokio::spawn(async move {
        let (reader, mut writer) = socket.into_split();
        process(reader, &store, |row, why| tracing::warn!(row, %why, "rejected")).await?;
        write_report(&store, &mut writer).await?;
        writer.shutdown().await?;
        Ok::<_, amounter::Error>(())
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
| `src/model.rs` | CSV `Record`, `Kind`, id types |
| `src/entity/` | SeaORM entities (`accounts`, `transactions`) |
| `src/store.rs` | connection handling, table creation |
| `src/engine.rs` | ledger rules, one DB transaction per record |
| `src/io.rs` | `process` and `write_report` |
| `src/main.rs` | clap CLI |
| `src/telemetry.rs` | CLI-only `tracing` subscriber setup (text/JSON, filters) |
| `tests/` | CLI, pipeline and concurrency tests; `common/` shared fixtures |

## Known limitations

- **Throughput:** one commit per record. About 20k rows/s on SQLite, but only
  ~600 rows/s against a local Postgres. Batching (a savepoint per record inside a
  larger transaction) or sharding by client would help.
- **Schema:** tables are created with `IF NOT EXISTS`; there are no migrations.
- **SQLite** uses a single connection, so concurrency there is serialized; row
  locks only matter on Postgres.
- Output order is unspecified.
