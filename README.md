# amounter

A small transaction ledger. It reads a CSV of deposits, withdrawals and
disputes, applies the ledger rules in a database, and prints one CSV row per
client with the resulting balances.

It is a library (`amounter`) with a thin CLI on top. The stages work on any
`AsyncRead` / `AsyncWrite`, and storage goes through SeaORM: SQLite by default,
PostgreSQL by changing the URL.

Internal architecture is based on MVC, with SeaORM structs on Model layer,
CSV as the only View layer _for now_
and the CLI as the only Controller _for now_.

The idea is to have clear segregations to allow the lib to evolve for network-based consumption in the future.

### AI usage disclaimer

AI assistance was used since the beginning, generating code, tests
and adversarial reasoning. Commits are my own, with the prompts replayed on the `Prior inputs` sections by the bottom
of commit messages, specially on the initial commits.

Later commits have a summary of what the prompts asked and reached, over many
turns from last commit to this one.

Initial commits are more "loose" on code quality & readability,
with further refactors up to the current state.

## Quick start

```console
$ cargo run -- tests/fixtures/transactions.csv
client,available,held,total,locked
1,1.5000,0.0000,1.5000,false
2,2.0000,0.0000,2.0000,false
```

## Usage

```sh
amounter transactions.csv          # same as `amounter ingest <filename>`
cat transactions.csv | amounter -  # stdin: `-`, `--`, or no argument
DATABASE_URL=postgres://user:pw@host/db amounter ingest transactions.csv
DATABASE_URL=postgres://user:pw@host/db amounter verify
```

| Command | What it does |
| --- | --- |
| `ingest [FILE]` | The default. Applies the CSV and prints the account report on stdout. |
| `verify` | Read-only audit of the database against its journal. See [Audit journal](#audit-journal). |

Global options:
- `--database-url` (env `DATABASE_URL`): defaults to ephemeral in-memory SQLite database
- `--log-format`
(env `LOG_FORMAT`): `text` (default) or `json`.

State persists only if a database URL is provided

### Input

```csv
type, client, tx, amount
deposit, 1, 1, 1.0
withdrawal, 1, 2, 0.5
dispute, 1, 1
```

- Columns are `type,client,tx,amount`; whitespace around fields is ignored.
- `type` is `deposit`, `withdrawal`, `dispute`, `resolve` or `chargeback`.
- `client` is `u16` and `tx` a `u32`.
- `amount` is unsigned decimal with up to 4 decimal places. Required
  on `deposit` and `withdrawal` (zero is allowed).

### Output

CSV with `client,available,held,total,locked` header always present, amounts always w/ 4 decimals. **Row order is unspecified**: sort
before diffing two runs.

## Ledger rules

- **Deposit / withdrawal** with unique tx id. An identical replay is ignored as a duplicate. Same id w/ different content is rejected.
- Only deposits can be disputed
- Dispute-like rows must name a transaction of the same client, in the right state.
- Resolved deposits can be disputed again; a charged-back one _currently_ cannot.
- Disputes may drive `available` negative. **It is the only way a
  balance goes negative.**
- Chargebacks lock its accounts permanently _for now_.
- **Locked accounts** reject deposits and withdrawals. Still allows
  dispute, resolve and chargeback.
- Rejected deposit/withdrawal burn its tx id
- Rejected dispute-like row leaves no trace.

A rule violation *rejects* the row: it is skipped and logged as a `WARN` with
`row` and `reason`. Malformed CSV, an unknown `type`, a deposit or withdrawal
without an amount, or a database failure is fatal: non-zero exit, no report.

Dispute, resolve and chargeback rows cannot be deduplicated as they have no own id: a redelivered `dispute` arriving after a `resolve` disputes the
transaction again.

The choices the spec left open are in
[docs/OPEN_QUESTIONS.md](docs/OPEN_QUESTIONS.md).

## Audit journal

Every committed change appends one row to the `journal` table, in the same
database transaction and under the same account lock: applied operations and
deposit/withdrawal tombstones. Identical replays and the rejections that roll
back change nothing, so they are not journaled.

Each row is also a snapshot of the client's account right after the operation
(`available_after`, `held_after`, `locked_after`), so a client's previous state
is its previous row. `seq` is the
global order, assigned by the database.

`amounter verify` streams the journal in `seq` order and checks that:

1. each row follows from the client's previous row by the account rules (the
   same methods ingest uses);
2. every `accounts` row equals its client's last journal row.

It prints one plain line per discrepancy on stdout and
exits non-zero if there is any. Clean means no output.

```text
journal seq 3 (client 1, tx 3): available should be 1.5000, is 9.0000
accounts client 1: held should be 2.0000, is 0.0000
```

Limits:

- Detection only. Rebuilding and checking `transactions` is left for external tooling;
- The journal is in the same database. And with no access control here;
- Continuity only. Does not detect no-balance-change errors. A hash chain would be the next step.

## Library

Three independent, lazy stages: decode, ingest, summary

```rust
use amounter::{Outcome, Store, entity::account, format::csv, pipeline};
use futures_util::TryStreamExt;
use tokio::io::{AsyncRead, AsyncWrite};

async fn run(
    reader: impl AsyncRead + Unpin + Send,
    mut writer: impl AsyncWrite + Unpin + Send,
) -> Result<(), csv::Error> {
    let store = Store::connect("sqlite://ledger.db").await?;
    // Stream<Item = Result<(Seq, Operation), csv::Error>>
    let operations = csv::decode(reader);
    // Stream<Item = Result<(Seq, Outcome), csv::Error>>
    let mut outcomes = Box::pin(pipeline::ingest(store.clone(), operations));
    while let Some((row, outcome)) = outcomes.try_next().await? {
        if let Outcome::Rejected(reason) = outcome {
            tracing::warn!(row, %reason, "rejected")
        }
    }
    // Independent of the above, even concurrently if networked DB:
    let summaries = account::Model::summaries(&store).await?;
    csv::encode_report(Box::pin(summaries), &mut writer).await
}
```

- `decode` reads one row per pass. Could be implemented for other formats than CSV. (JSONL over TCP?)
- `ingest` applies each Operation within a DB transaction
  - There is no batching _for now_. Could be done per account
- Each ingestion does `SELECT ... FOR UPDATE` on the client row on Postgres. SQLite concurrency is not supported.
- After the first error (malformed row, missing amount, database failure) the
  streams end. Operations before it stays committed.
- Money is the fixed-point `Amount` (integer ten-thousandths).
  - No DB conversion surprises
  - No floats
  - No wraps: overflow => Err

## Development

The toolchain is pinned in `rust-toolchain.toml`.

```sh
cargo nextest run        # needs `cargo-nextest`; plain `cargo test` works too
cargo clippy --all-targets
cargo fmt --check
```

The Postgres tests are skipped unless `AMOUNTER_TEST_POSTGRES` holds the URL of
a scratch database (its tables are truncated):

```sh
AMOUNTER_TEST_POSTGRES=postgres://user:pw@localhost/scratch cargo nextest run
```

### Load testing

The generator `examples/gen_load.rs` writes deterministic CSVs of any size,
within some selectable profiles:

```sh
cargo run --release --example gen_load -- 1000000 --profile mixed > big.csv
```

## Current caveats

- **Throughput:** one commit per record. About 20k rows/s on SQLite, about 600
  rows/s on local Postgres. Would improve by batching or sharding by client
- **Rows are atomic, not input files:** rows apply as they stream, leaving room for future TCP decoders and batching.
- **No migrations:** tables are created with `IF NOT EXISTS`. Schema upgrades are out of scope for this toy project.
