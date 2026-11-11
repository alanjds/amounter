//! Installs the `tracing` subscriber for the CLI. The library only emits
//! events and spans; it never installs a subscriber.
//!
//! Logs go to stderr (stdout is the CSV report only). The format is `text` or
//! `json` (`LOG_FORMAT`); the filter is `RUST_LOG`, defaulting to `warn`.

use std::io::IsTerminal;

use clap::ValueEnum;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

/// Installs the global subscriber.
///
/// Use `RUST_LOG` as the level selecter, defaulting to `warn`.
pub fn init(format: LogFormat) -> Result<(), String> {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into())
        .from_env()
        .map_err(|e| format!("invalid RUST_LOG: {e}"))?;
    let layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal());
    let registry = tracing_subscriber::registry().with(filter);
    match format {
        LogFormat::Text => registry.with(layer).try_init(),
        LogFormat::Json => registry.with(layer.json()).try_init(),
    }
    .map_err(|e| format!("cannot install logger: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("text", LogFormat::Text)]
    #[case("TEXT", LogFormat::Text)]
    #[case("json", LogFormat::Json)]
    #[case("Json", LogFormat::Json)]
    fn parses_log_format_case_insensitively(#[case] s: &str, #[case] want: LogFormat) {
        assert_eq!(LogFormat::from_str(s, true).unwrap(), want);
    }

    #[test]
    fn rejects_unknown_log_format() {
        assert!(LogFormat::from_str("yaml", true).is_err());
    }
}
