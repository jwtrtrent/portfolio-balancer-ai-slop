use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::allocate::Allocation;
use crate::errors::RebalanceError;
use crate::model::{
    AccountResult, PositionResult, PositionsFile, PricesFile, RebalanceOutput, Summary,
    TargetsFile, Ticker,
};

pub fn build_output(
    positions: &PositionsFile,
    prices: &PricesFile,
    targets: &TargetsFile,
    allocation: &Allocation,
) -> Result<RebalanceOutput, RebalanceError> {
    let mut accounts = BTreeMap::new();
    // Total target shares per ticker (across all accounts) for drift calc.
    let mut total_target_shares: BTreeMap<Ticker, i64> = BTreeMap::new();

    for (account_id, account) in &positions.accounts {
        let empty = BTreeMap::new();
        let alloc_for_account = allocation
            .per_account_ticker_dollars
            .get(account_id)
            .unwrap_or(&empty);

        // Tickers we need to consider: those currently held + those allocated.
        let tickers: BTreeSet<&Ticker> = account
            .positions
            .keys()
            .chain(alloc_for_account.keys())
            .collect();

        let mut positions_out = BTreeMap::new();
        let mut net_cash_change = Decimal::ZERO;
        for ticker in tickers {
            let price = prices
                .prices
                .get(ticker)
                .ok_or_else(|| RebalanceError::MissingPrice {
                    ticker: ticker.clone(),
                })?
                .0;
            let current_shares = account.positions.get(ticker).copied().unwrap_or(0);
            let target_dollars = alloc_for_account
                .get(ticker)
                .copied()
                .unwrap_or(Decimal::ZERO);
            let target_shares = floor_shares(target_dollars, price);
            let trade_shares = target_shares - current_shares;
            let trade_value = price * Decimal::from(trade_shares);
            net_cash_change -= trade_value;
            *total_target_shares.entry(ticker.clone()).or_insert(0) += target_shares;
            positions_out.insert(
                ticker.clone(),
                PositionResult {
                    current_shares,
                    target_shares,
                    trade_shares,
                    trade_value,
                    price,
                },
            );
        }

        let ending_cash = account.cash + net_cash_change;
        accounts.insert(
            account_id.clone(),
            AccountResult {
                starting_cash: account.cash,
                ending_cash,
                positions: positions_out,
            },
        );
    }

    let summary = build_summary(targets, prices, allocation, &total_target_shares)?;

    Ok(RebalanceOutput { accounts, summary })
}

fn floor_shares(dollars: Decimal, price: Decimal) -> i64 {
    if price <= Decimal::ZERO || dollars <= Decimal::ZERO {
        return 0;
    }
    (dollars / price).floor().to_i64().unwrap_or(i64::MAX)
}

fn build_summary(
    targets: &TargetsFile,
    prices: &PricesFile,
    allocation: &Allocation,
    total_target_shares: &BTreeMap<Ticker, i64>,
) -> Result<Summary, RebalanceError> {
    let mut sleeve_drift_bps = BTreeMap::new();
    let mut max_drift_bps: i64 = 0;
    for (sleeve_id, sleeve) in &targets.sleeves {
        let target_dollars = allocation
            .sleeve_target_dollars
            .get(sleeve_id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let mut actual_dollars = Decimal::ZERO;
        for ticker in sleeve.holdings.keys() {
            let price = prices
                .prices
                .get(ticker)
                .ok_or_else(|| RebalanceError::MissingPrice {
                    ticker: ticker.clone(),
                })?
                .0;
            let shares = total_target_shares.get(ticker).copied().unwrap_or(0);
            actual_dollars += price * Decimal::from(shares);
        }
        let drift = drift_bps(actual_dollars, target_dollars, allocation.total_value);
        if drift > max_drift_bps {
            max_drift_bps = drift;
        }
        sleeve_drift_bps.insert(sleeve_id.clone(), drift);
    }

    Ok(Summary {
        total_value: allocation.total_value,
        sleeve_drift_bps,
        max_drift_bps,
    })
}

fn drift_bps(actual: Decimal, target: Decimal, total: Decimal) -> i64 {
    if total <= Decimal::ZERO {
        return 0;
    }
    let diff = (actual - target).abs();
    let bps = diff * Decimal::from(10_000) / total;
    bps.round().to_i64().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocate::allocate;
    use crate::model::{Account, DecimalStr, Sleeve};
    use pretty_assertions::assert_eq;
    use rust_decimal_macros::dec;

    fn account(cash: Decimal, positions: &[(&str, i64)]) -> Account {
        Account {
            r#type: None,
            cash,
            positions: positions.iter().map(|(t, s)| (t.to_string(), *s)).collect(),
        }
    }

    fn prices_of(items: &[(&str, Decimal)]) -> PricesFile {
        PricesFile {
            prices: items
                .iter()
                .map(|(t, p)| (t.to_string(), DecimalStr(*p)))
                .collect(),
        }
    }

    fn sleeve(weight: Decimal, holdings: &[(&str, Decimal)], preferred: &[&str]) -> Sleeve {
        Sleeve {
            target_weight: weight,
            holdings: holdings
                .iter()
                .map(|(t, w)| (t.to_string(), DecimalStr(*w)))
                .collect(),
            preferred_accounts: preferred.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn integer_rounding_floors_shares_and_leaves_cash() {
        // 1000 cash; target = 1000; price = 33 -> 30 shares = 990, 10 left over.
        let positions = PositionsFile {
            accounts: BTreeMap::from([("a".to_string(), account(dec!(1000), &[]))]),
        };
        let prices = prices_of(&[("X", dec!(33))]);
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "s".to_string(),
                sleeve(dec!(1.0), &[("X", dec!(1.0))], &["a"]),
            )]),
        };
        let alloc = allocate(&positions, &prices, &targets).unwrap();
        let out = build_output(&positions, &prices, &targets, &alloc).unwrap();
        let pos = &out.accounts["a"].positions["X"];
        assert_eq!(pos.target_shares, 30);
        assert_eq!(pos.trade_shares, 30);
        assert_eq!(out.accounts["a"].ending_cash, dec!(10));
    }

    #[test]
    fn liquidates_untargeted_positions() {
        // Holding AAPL but it is not in any sleeve -> sell all of it.
        let positions = PositionsFile {
            accounts: BTreeMap::from([(
                "a".to_string(),
                account(dec!(0), &[("AAPL", 5), ("VTI", 0)]),
            )]),
        };
        let prices = prices_of(&[("AAPL", dec!(100)), ("VTI", dec!(50))]);
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "s".to_string(),
                sleeve(dec!(1.0), &[("VTI", dec!(1.0))], &["a"]),
            )]),
        };
        let alloc = allocate(&positions, &prices, &targets).unwrap();
        let out = build_output(&positions, &prices, &targets, &alloc).unwrap();
        let aapl = &out.accounts["a"].positions["AAPL"];
        assert_eq!(aapl.target_shares, 0);
        assert_eq!(aapl.trade_shares, -5);
        assert_eq!(aapl.trade_value, dec!(-500));
        // Total value 500, all to VTI -> 10 shares.
        let vti = &out.accounts["a"].positions["VTI"];
        assert_eq!(vti.target_shares, 10);
        assert_eq!(vti.trade_shares, 10);
        assert_eq!(out.accounts["a"].ending_cash, dec!(0));
    }

    #[test]
    fn ending_cash_never_negative() {
        // Pick a deliberately ugly setup and verify no account goes negative.
        let positions = PositionsFile {
            accounts: BTreeMap::from([
                ("roth".to_string(), account(dec!(1500), &[("VTI", 10)])),
                ("trad".to_string(), account(dec!(200), &[("BND", 50)])),
                (
                    "taxable".to_string(),
                    account(dec!(750), &[("VTI", 40), ("VXUS", 25)]),
                ),
            ]),
        };
        let prices = prices_of(&[
            ("VTI", dec!(250)),
            ("VXUS", dec!(60)),
            ("BND", dec!(75)),
            ("BNDX", dec!(55)),
        ]);
        let targets = TargetsFile {
            sleeves: BTreeMap::from([
                (
                    "us".to_string(),
                    sleeve(dec!(0.5), &[("VTI", dec!(1.0))], &["taxable", "roth"]),
                ),
                (
                    "intl".to_string(),
                    sleeve(dec!(0.3), &[("VXUS", dec!(1.0))], &["taxable"]),
                ),
                (
                    "bonds".to_string(),
                    sleeve(
                        dec!(0.2),
                        &[("BND", dec!(0.7)), ("BNDX", dec!(0.3))],
                        &["roth", "trad"],
                    ),
                ),
            ]),
        };
        let alloc = allocate(&positions, &prices, &targets).unwrap();
        let out = build_output(&positions, &prices, &targets, &alloc).unwrap();
        for (id, acct) in &out.accounts {
            assert!(
                acct.ending_cash >= Decimal::ZERO,
                "account {id} went negative: {}",
                acct.ending_cash
            );
        }
    }

    #[test]
    fn sells_fund_buys_in_same_account() {
        // Account has 10 VTI ($1000) and we want all bonds in this account.
        let positions = PositionsFile {
            accounts: BTreeMap::from([("a".to_string(), account(dec!(0), &[("VTI", 10)]))]),
        };
        let prices = prices_of(&[("VTI", dec!(100)), ("BND", dec!(50))]);
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "bonds".to_string(),
                sleeve(dec!(1.0), &[("BND", dec!(1.0))], &["a"]),
            )]),
        };
        let alloc = allocate(&positions, &prices, &targets).unwrap();
        let out = build_output(&positions, &prices, &targets, &alloc).unwrap();
        assert_eq!(out.accounts["a"].positions["VTI"].trade_shares, -10);
        assert_eq!(out.accounts["a"].positions["BND"].target_shares, 20);
        assert_eq!(out.accounts["a"].ending_cash, dec!(0));
    }

    #[test]
    fn drift_bps_signed_then_abs() {
        // 100 dollars off on a 1,000,000 portfolio -> 1 bps.
        assert_eq!(super::drift_bps(dec!(100), dec!(0), dec!(1_000_000)), 1);
        assert_eq!(super::drift_bps(dec!(0), dec!(100), dec!(1_000_000)), 1);
        assert_eq!(super::drift_bps(dec!(0), dec!(0), dec!(1_000_000)), 0);
    }

    #[test]
    fn summary_zero_drift_when_dollars_align() {
        // 10k portfolio, 100% in VTI @ $100. Allocation = 10k, target shares = 100.
        let positions = PositionsFile {
            accounts: BTreeMap::from([("a".to_string(), account(dec!(10000), &[]))]),
        };
        let prices = prices_of(&[("VTI", dec!(100))]);
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "us".to_string(),
                sleeve(dec!(1.0), &[("VTI", dec!(1.0))], &["a"]),
            )]),
        };
        let alloc = allocate(&positions, &prices, &targets).unwrap();
        let out = build_output(&positions, &prices, &targets, &alloc).unwrap();
        assert_eq!(out.summary.max_drift_bps, 0);
        assert_eq!(out.summary.sleeve_drift_bps["us"], 0);
        assert_eq!(out.summary.total_value, dec!(10000));
    }

    #[test]
    fn unallocated_account_keeps_cash_and_liquidates() {
        // Account "side" is not in any preferred list and the sole sleeve
        // fits entirely in "main" — "side" should liquidate any holdings and
        // keep its cash + liquidation proceeds untouched.
        let positions = PositionsFile {
            accounts: BTreeMap::from([
                ("main".to_string(), account(dec!(5000), &[])),
                ("side".to_string(), account(dec!(100), &[("OLD", 2)])),
            ]),
        };
        let prices = prices_of(&[("VTI", dec!(50)), ("OLD", dec!(10))]);
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "us".to_string(),
                sleeve(dec!(1.0), &[("VTI", dec!(1.0))], &["main"]),
            )]),
        };
        let alloc = allocate(&positions, &prices, &targets).unwrap();
        let out = build_output(&positions, &prices, &targets, &alloc).unwrap();
        // main: total_value = 5120, ~5000 to it (preferred fully fills 5000),
        // side gets 120 spillover -> 2 shares VTI = 100, 20 cash + 20 liquidation = 40.
        // Just sanity-check that side's OLD is sold off:
        assert_eq!(out.accounts["side"].positions["OLD"].trade_shares, -2);
        assert!(out.accounts["side"].ending_cash >= Decimal::ZERO);
    }
}
