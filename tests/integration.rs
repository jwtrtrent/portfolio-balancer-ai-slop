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
fn cli_emits_per_lot_sales_and_gain_totals() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let examples = std::path::Path::new(manifest_dir).join("examples");
    let tmp = tempdir().unwrap();
    let output = tmp.path().join("out.json");

    let mut cmd = Command::cargo_bin("portfolio-rebalancer").unwrap();
    cmd.arg("--positions")
        .arg(examples.join("positions-lots.json"))
        .arg("--prices")
        .arg(examples.join("prices.json"))
        .arg("--targets")
        .arg(examples.join("targets.json"))
        .arg("--output")
        .arg(&output)
        .arg("--lot-strategy")
        .arg("hifo")
        .arg("--as-of")
        .arg("2025-01-01");
    cmd.assert().success();

    let parsed: RebalanceOutput = read_json(&output).unwrap();

    // Taxable account has 40 VTI across two lots; the rebalance will sell
    // some VTI to fund bond buys. HIFO means the 2023 (higher-basis) lot is
    // consumed first, which is short-term given the 2025-01-01 as-of.
    let taxable = &parsed.accounts["taxable"];
    let vti = &taxable.positions["VTI"];
    assert!(vti.trade_shares < 0, "expected VTI sell");
    assert!(!vti.lots_sold.is_empty(), "expected per-lot detail");
    assert_eq!(vti.lots_sold[0].lot_id, "vti-2023");
    // Gain totals must be consistent with per-lot sum.
    let sum_gain: rust_decimal::Decimal = vti.lots_sold.iter().map(|l| l.realized_gain).sum();
    assert_eq!(
        sum_gain,
        vti.lots_sold
            .iter()
            .map(|l| l.proceeds - l.cost_basis)
            .sum()
    );
    assert_eq!(
        taxable.realized_gain,
        taxable.short_term_gain + taxable.long_term_gain
    );
    assert_eq!(
        parsed.summary.total_realized_gain,
        parsed.summary.total_short_term_gain + parsed.summary.total_long_term_gain
    );

    // Accounts without lots (roth_ira, traditional) still rebalance normally
    // but do not produce per-lot sale detail.
    let roth = &parsed.accounts["roth_ira"];
    assert_eq!(roth.realized_gain, rust_decimal::Decimal::ZERO);
    for pos in roth.positions.values() {
        assert!(pos.lots_sold.is_empty());
    }
}

#[test]
fn cli_honors_policy_file() {
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
        .arg(&output)
        .arg("--policies")
        .arg(examples.join("policies.json"));
    cmd.assert().success();

    let parsed: RebalanceOutput = read_json(&output).unwrap();

    // `intl_equity` sleeve only prefers `taxable`, which is fine — but
    // liquidating VXUS in roth_ira/traditional is blocked by the "no
    // international in retirement" policy. Those accounts hold no VXUS in
    // the canonical example, so the policy should not fire there. It
    // should still be loaded without error, and the run should complete.
    // The canonical positions hold 25 VXUS in taxable — which is allowed.
    let taxable = &parsed.accounts["taxable"];
    // VXUS ends up held in taxable (the preferred account). Trade may be
    // positive (buy more) or zero — either way the ticker must show up.
    assert!(taxable.positions.contains_key("VXUS"));

    // Now put VXUS into a retirement account and confirm the policy
    // blocks it.
    let bad_positions = tmp.path().join("positions-retire-intl.json");
    std::fs::write(
        &bad_positions,
        r#"{
            "accounts": {
                "roth_ira":    { "type": "roth",        "cash": "1500.00", "positions": { "VTI": 10, "VXUS": 10 } },
                "traditional": { "type": "traditional", "cash": "200.00",  "positions": { "BND": 50 } },
                "taxable":     { "type": "taxable",     "cash": "750.00",  "positions": { "VTI": 40 } }
            }
        }"#,
    )
    .unwrap();
    let output2 = tmp.path().join("out2.json");
    let mut cmd = Command::cargo_bin("portfolio-rebalancer").unwrap();
    cmd.arg("--positions")
        .arg(&bad_positions)
        .arg("--prices")
        .arg(examples.join("prices.json"))
        .arg("--targets")
        .arg(examples.join("targets.json"))
        .arg("--output")
        .arg(&output2)
        .arg("--policies")
        .arg(examples.join("policies.json"));
    cmd.assert().success();
    let parsed: RebalanceOutput = read_json(&output2).unwrap();
    // The policy denies selling VXUS from roth_ira (is_sell → deny). The
    // engine zeroes that trade → roth_ira keeps its 10 VXUS.
    let roth = &parsed.accounts["roth_ira"];
    let vxus = &roth.positions["VXUS"];
    assert_eq!(
        vxus.trade_shares, 0,
        "VXUS in roth should be held, not sold"
    );
    assert_eq!(vxus.target_shares, vxus.current_shares);
    // At least one deny violation for (roth_ira, VXUS).
    assert!(
        parsed
            .summary
            .policy_violations
            .iter()
            .any(|v| v.action == "deny"
                && v.account == "roth_ira"
                && v.ticker.as_deref() == Some("VXUS")),
        "expected deny violation for (roth_ira, VXUS), got: {:?}",
        parsed.summary.policy_violations
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
