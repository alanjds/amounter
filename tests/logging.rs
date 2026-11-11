//! Logging contract of the CLI: stdout is CSV only, logs go to stderr in the
//! format picked by `LOG_FORMAT`, filtered by `RUST_LOG`.

use indoc::indoc;
use rstest::rstest;

mod common;
use common::{CliResult, bin, run_cli};

const INPUT: &str = indoc! {"
    type,client,tx,amount
    deposit,1,1,1.0
    withdrawal,1,2,5.0
"};

const CLEAN_INPUT: &str = indoc! {"
    type,client,tx,amount
    deposit,1,1,1.0
"};

/// Runs the binary with `args` and extra `env`, feeding `stdin`.
fn run(args: &[&str], env: &[(&str, &str)], stdin: &str) -> CliResult {
    run_cli(bin().args(args).envs(env.iter().copied()), stdin)
}

fn json_lines(stderr: &str) -> Vec<serde_json::Value> {
    stderr
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l}")))
        .collect()
}

#[test]
fn default_text_shows_the_rejection() {
    let r = run(&["-"], &[], INPUT);
    assert!(r.success);
    assert!(
        r.stderr.contains("WARN") && r.stderr.contains("rejected row=2 reason=insufficient"),
        "{}",
        r.stderr
    );
    assert_eq!(r.stderr.lines().count(), 1, "{}", r.stderr); // the summary is INFO
}

#[rstest]
#[case("json")]
#[case("JSON")]
fn json_format_emits_only_json_lines(#[case] value: &str) {
    let r = run(&["-"], &[("LOG_FORMAT", value)], INPUT);
    assert!(r.success);
    let lines = json_lines(&r.stderr);
    let reject = lines
        .iter()
        .find(|l| l["fields"]["message"] == "rejected")
        .expect("reject event");
    assert_eq!(reject["fields"]["row"], 2);
    assert_eq!(reject["level"], "WARN");
    assert!(
        reject["fields"]["reason"]
            .as_str()
            .unwrap()
            .contains("insufficient")
    );
}

#[test]
fn json_format_via_flag() {
    let r = run(&["--log-format", "json", "-"], &[], INPUT);
    assert!(r.success);
    assert!(!json_lines(&r.stderr).is_empty());
}

#[test]
fn invalid_log_format_is_a_startup_error() {
    let r = run(&["-"], &[("LOG_FORMAT", "bogus")], INPUT);
    assert!(!r.success);
    assert!(r.stderr.contains("LOG_FORMAT"), "{}", r.stderr);
    assert!(r.stdout.is_empty());
}

#[test]
fn invalid_rust_log_is_a_startup_error() {
    let r = run(&["-"], &[("RUST_LOG", "amounter=loud")], INPUT);
    assert!(!r.success);
    assert!(r.stderr.contains("RUST_LOG"), "{}", r.stderr);
    assert!(r.stdout.is_empty());
}

#[test]
fn stdout_is_identical_whatever_the_logging() {
    let baseline = run(&["-"], &[], INPUT).stdout;
    assert!(baseline.starts_with("client,available,held,total,locked\n"));
    for env in [
        vec![("LOG_FORMAT", "json")],
        vec![("RUST_LOG", "trace")],
        vec![("RUST_LOG", "trace"), ("LOG_FORMAT", "json")],
    ] {
        let r = run(&["-"], &env, INPUT);
        assert!(r.success);
        assert_eq!(r.stdout, baseline, "{env:?}");
    }
}

#[test]
fn rust_log_error_hides_rejections() {
    let r = run(&["-"], &[("RUST_LOG", "error")], INPUT);
    assert!(r.success);
    assert_eq!(r.stderr, "");
}

#[rstest]
#[case::text("text")]
#[case::json("json")]
fn fatal_errors_are_logged(#[case] format: &str) {
    let r = run(&["/nonexistent.csv"], &[("LOG_FORMAT", format)], "");
    assert!(!r.success);
    if format == "json" {
        let lines = json_lines(&r.stderr);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["level"], "ERROR");
    } else {
        assert!(r.stderr.contains("ERROR"), "{}", r.stderr);
    }
}

#[test]
fn rust_log_off_gives_empty_stderr() {
    let r = run(&["-"], &[("RUST_LOG", "off")], CLEAN_INPUT);
    assert!(r.success);
    assert_eq!(r.stderr, "");
}

#[test]
fn rust_log_info_shows_the_summary() {
    let url = "sqlite::memory:";
    let r = run(
        &["-"],
        &[("RUST_LOG", "info"), ("DATABASE_URL", url)],
        CLEAN_INPUT,
    );
    assert!(r.success);
    assert!(r.stderr.contains("rejected=0"), "{}", r.stderr);
}
