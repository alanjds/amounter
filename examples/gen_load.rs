//! Deterministic load generator for stress/benchmark CSV input.
//!
//! ```text
//! cargo run --release --example gen_load -- ROWS [--seed N] [--profile P] [--output PATH|-]
//! ```
//!
//! Profiles: `sequential` (deposits, tx 1..), `sparse` (deposits, tx ids
//! scrambled over the whole u32 range), `mixed` (70% deposit / 20% withdrawal /
//! 10% dispute-like), `dup-heavy` (sparse deposits, ~10% repeated tx ids).
//!
//! `--whitespace` pads fields (`type, client, tx, amount`), varies amount
//! precision from 0 to 4 decimals and adds stray trailing spaces.
//! Tx ids and client ids are never in ascending order (except `sequential` tx).
//!
//! Memory is O(1): rows are streamed through a `BufWriter`, counters are u64.
//! Beyond 2^32 rows, tx ids necessarily wrap and repeat. For the deposit-only
//! profiles the expected sum of all totals (in 1/10000 units) is printed on
//! stderr as `expected_total=...`.

use std::{
    fs::File,
    io::{self, BufWriter, Write},
    process::exit,
};

struct Rng(u64);
impl Rng {
    /// splitmix64
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Bijective on u32: odd multiplier + offset, mod 2^32.
fn scramble(i: u64) -> u32 {
    (i as u32)
        .wrapping_mul(0x9E37_79B1)
        .wrapping_add(0x7F4A_7C15)
}

#[derive(Clone, Copy, PartialEq)]
enum Profile {
    Sequential,
    Sparse,
    Mixed,
    DupHeavy,
}

fn usage() -> ! {
    eprintln!(
        "usage: gen_load ROWS [--seed N] [--profile sequential|sparse|mixed|dup-heavy] [--whitespace] [--output PATH|-]"
    );
    exit(2)
}

/// Picks the decimal places (0..=4) to print and truncates `units`
/// (1/10000) accordingly; falls back to 4 places if that would give zero.
fn precise(rng: &mut Rng, units: u64, varied: bool) -> (u64, u32) {
    if !varied {
        return (units, 4);
    }
    let prec = rng.below(5) as u32;
    let t = units - units % 10u64.pow(4 - prec);
    if t == 0 { (units, 4) } else { (t, prec) }
}

fn amount(w: &mut impl Write, units: u64, prec: u32) -> io::Result<()> {
    let (int, frac) = (units / 10_000, units % 10_000 / 10u64.pow(4 - prec));
    if prec == 0 {
        write!(w, "{int}")
    } else {
        write!(w, "{int}.{frac:0width$}", width = prec as usize)
    }
}

/// Field separator and optional line padding for `--whitespace`.
fn sep(ws: bool) -> &'static str {
    if ws { ", " } else { "," }
}
fn eol(w: &mut impl Write, ws: bool, rng: &mut Rng) -> io::Result<()> {
    if ws && rng.below(8) == 0 {
        w.write_all(b"  ")?;
    }
    w.write_all(b"\n")
}

const WINDOW: usize = 4096;
const HOT: u64 = 8;

fn client(rng: &mut Rng) -> u16 {
    if rng.below(100) < 20 {
        1 + rng.below(HOT) as u16
    } else {
        1 + rng.below(65_535) as u16
    }
}

fn main() -> io::Result<()> {
    let mut args = std::env::args().skip(1);
    let (mut rows, mut seed, mut profile, mut out) = (None, 1u64, Profile::Mixed, "-".to_string());
    let mut ws = false;
    while let Some(a) = args.next() {
        match a.as_str() {
            "--seed" => {
                seed = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage())
            }
            "--whitespace" => ws = true,
            "--output" | "-o" => out = args.next().unwrap_or_else(|| usage()),
            "--profile" => {
                profile = match args.next().as_deref() {
                    Some("sequential") => Profile::Sequential,
                    Some("sparse") => Profile::Sparse,
                    Some("mixed") => Profile::Mixed,
                    Some("dup-heavy") => Profile::DupHeavy,
                    _ => usage(),
                }
            }
            _ if rows.is_none() => {
                rows = Some(
                    a.replace('_', "")
                        .parse::<u64>()
                        .unwrap_or_else(|_| usage()),
                )
            }
            _ => usage(),
        }
    }
    let rows = rows.unwrap_or_else(|| usage());
    let sink: Box<dyn Write> = if out == "-" {
        Box::new(io::stdout().lock())
    } else {
        Box::new(File::create(&out)?)
    };
    let mut w = BufWriter::with_capacity(1 << 20, sink);
    let mut rng = Rng(seed);
    let mut expected: u128 = 0;
    // Ring of recent deposits: (tx, client, state 0=normal 1=disputed).
    let mut win = vec![(0u32, 0u16, 0u8); WINDOW];
    let mut win_len = 0usize;
    let mut win_next = 0usize;
    // Ring of recently *inserted* tx ids, the targets of dup-heavy repeats.
    let mut recent = [0u32; 1024];
    let (mut recent_len, mut recent_next) = (0usize, 0usize);

    let sp = sep(ws);
    writeln!(w, "type{sp}client{sp}tx{sp}amount")?;
    for i in 0..rows {
        match profile {
            Profile::Sequential | Profile::Sparse | Profile::DupHeavy => {
                let id_of = |n: u64| {
                    if profile == Profile::Sequential {
                        (n as u32).wrapping_add(1)
                    } else {
                        scramble(n)
                    }
                };
                let c = client(&mut rng);
                let raw = 1 + rng.below(1_000_000);
                let (units, prec) = precise(&mut rng, raw, ws);
                let dup = profile == Profile::DupHeavy && i > 0 && rng.below(10) == 0;
                let wrapped = i > u64::from(u32::MAX);
                let tx = if dup && recent_len > 0 {
                    recent[rng.below(recent_len as u64) as usize]
                } else {
                    id_of(i)
                };
                let dup = dup && recent_len > 0;
                if !dup {
                    recent[recent_next] = tx;
                    recent_next = (recent_next + 1) % recent.len();
                    recent_len = (recent_len + 1).min(recent.len());
                }
                if !dup && !wrapped {
                    expected += u128::from(units);
                }
                write!(w, "deposit{sp}{c}{sp}{tx}{sp}")?;
                amount(&mut w, units, prec)?;
                eol(&mut w, ws, &mut rng)?;
            }
            Profile::Mixed => {
                let roll = rng.below(100);
                let c = client(&mut rng);
                if roll < 70 || (roll >= 90 && win_len == 0) {
                    let tx = scramble(i);
                    write!(w, "deposit{sp}{c}{sp}{tx}{sp}")?;
                    let raw = 1 + rng.below(1_000_000);
                    let (u, pr) = precise(&mut rng, raw, ws);
                    amount(&mut w, u, pr)?;
                    eol(&mut w, ws, &mut rng)?;
                    win[win_next] = (tx, c, 0);
                    win_next = (win_next + 1) % WINDOW;
                    win_len = (win_len + 1).min(WINDOW);
                } else if roll < 90 {
                    // Mostly small, occasionally large (insufficient funds path).
                    let units = if rng.below(10) == 0 {
                        1 + rng.below(5_000_000)
                    } else {
                        1 + rng.below(100_000)
                    };
                    let (u, pr) = precise(&mut rng, units, ws);
                    write!(w, "withdrawal{sp}{c}{sp}{}{sp}", scramble(i))?;
                    amount(&mut w, u, pr)?;
                    eol(&mut w, ws, &mut rng)?;
                } else {
                    let slot = rng.below(win_len as u64) as usize;
                    let (tx, c, st) = &mut win[slot];
                    let kind = if *st == 0 {
                        *st = 1;
                        "dispute"
                    } else if rng.below(100) < 90 {
                        *st = 0;
                        "resolve"
                    } else {
                        "chargeback"
                    };
                    // Alternate between "type,client,tx," and "type,client,tx".
                    if i % 2 == 0 {
                        write!(w, "{kind}{sp}{c}{sp}{tx}{sp}")?;
                    } else {
                        write!(w, "{kind}{sp}{c}{sp}{tx}")?;
                    }
                    eol(&mut w, ws, &mut rng)?;
                }
            }
        }
    }
    w.flush()?;
    if profile != Profile::Mixed {
        eprintln!("expected_total={expected}");
    }
    Ok(())
}
