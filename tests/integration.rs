use std::process::Command;

use assert_cmd::prelude::*;
use portfolio_rebalancer::{io_json::read_json, RebalanceOutput};
use rust_decimal::Decimal;
use tempfile::tempdir;

#[test]
fn cli_runs_against_examples() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let examples = std::path::Path::new(manifest_dir).join("examples");
    let tmp = tempdir().unwrap();
    let output = tmp.path().join("out.json");

    let mut cmd = Command::cargo_bin("portfolio-rebalancer").unwrap();
    cmd.arg("--positions")
        .arg(examples.join("positions.json"))
        .arg("--prices")
        .arg(examples.join("prices.json"))
        .arg("--targets")
        .arg(examples.join("targets.json"))
        .arg("--output")
        .arg(&output);
    cmd.assert().success();

    let parsed: RebalanceOutput = read_json(&output).unwrap();

    // Total value: 1500 + 200 + 750 + 10*250 + 50*75 + 40*250 + 25*60 = 20_200.
    assert_eq!(parsed.summary.total_value.to_string(), "20200.00");

    // Every account should end up with non-negative cash.
    for (id, acct) in &parsed.accounts {
        assert!(
            acct.ending_cash >= Decimal::ZERO,
            "account {id} negative ending cash: {}",
            acct.ending_cash
        );
    }

    // Drift bounded by whole-share rounding (one VTI share = 124 bps of a
    // $20k portfolio, so allow several shares of slack).
    assert!(
        parsed.summary.max_drift_bps < 500,
        "drift too large: {}",
        parsed.summary.max_drift_bps
    );
}

#[test]
fn cli_errors_on_invalid_targets() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let examples = std::path::Path::new(manifest_dir).join("examples");
    let tmp = tempdir().unwrap();
    let bad_targets = tmp.path().join("targets.json");
    std::fs::write(
        &bad_targets,
        r#"{
            "sleeves": {
                "us_equity": {
                    "target_weight": "0.50",
                    "holdings": { "VTI": "1.00" },
                    "preferred_accounts": ["taxable"]
                }
            }
        }"#,
    )
    .unwrap();
    let output = tmp.path().join("out.json");

    let mut cmd = Command::cargo_bin("portfolio-rebalancer").unwrap();
    cmd.arg("--positions")
        .arg(examples.join("positions.json"))
        .arg("--prices")
        .arg(examples.join("prices.json"))
        .arg("--targets")
        .arg(&bad_targets)
        .arg("--output")
        .arg(&output);
    cmd.assert().failure();
}
