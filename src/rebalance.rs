use std::collections::{BTreeMap, BTreeSet, HashMap};

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use time::Date;

use crate::allocate::Allocation;
use crate::errors::RebalanceError;
use crate::id::SecurityId;
use crate::lot::{consume_lots, LotSelector, SaleAllocation};
use crate::model::{AccountResult, PositionResult, RebalanceOutput, SaleLotResult, Summary};
use crate::source::{AccountData, PortfolioSource};

pub fn build_output(
    source: &dyn PortfolioSource,
    allocation: &Allocation,
    lot_selector: LotSelector,
    as_of: Date,
) -> Result<RebalanceOutput, RebalanceError> {
    let mut accounts = BTreeMap::new();
    // Total target shares per security (across all accounts) for drift calc.
    let mut total_target_shares: HashMap<SecurityId, i64> = HashMap::new();
    let empty = HashMap::<SecurityId, Decimal>::new();

    let mut total_realized = Decimal::ZERO;
    let mut total_short = Decimal::ZERO;
    let mut total_long = Decimal::ZERO;

    for account in source.accounts() {
        let alloc_for_account = allocation
            .per_account_ticker_dollars
            .get(&account.id)
            .unwrap_or(&empty);

        let mut sids: BTreeSet<SecurityId> = account.positions.iter().map(|(s, _)| *s).collect();
        sids.extend(alloc_for_account.keys().copied());

        let mut positions_out: BTreeMap<String, PositionResult> = BTreeMap::new();
        let mut net_cash_change = Decimal::ZERO;
        let mut account_realized = Decimal::ZERO;
        let mut account_short = Decimal::ZERO;
        let mut account_long = Decimal::ZERO;

        for sid in sids {
            let price = source
                .price(sid)
                .ok_or_else(|| missing_price(source, sid))?;
            let current_shares = account
                .positions
                .iter()
                .find(|(s, _)| *s == sid)
                .map(|(_, shares)| *shares)
                .unwrap_or(0);
            let target_dollars = alloc_for_account
                .get(&sid)
                .copied()
                .unwrap_or(Decimal::ZERO);
            let target_shares = floor_shares(target_dollars, price);
            let trade_shares = target_shares - current_shares;
            let trade_value = price * Decimal::from(trade_shares);
            net_cash_change -= trade_value;
            *total_target_shares.entry(sid).or_insert(0) += target_shares;

            let lots_sold = if trade_shares < 0 {
                let sales = sell_lots(account, sid, -trade_shares, price, lot_selector, as_of);
                for s in &sales {
                    let g = s.realized_gain();
                    account_realized += g;
                    if s.is_long_term {
                        account_long += g;
                    } else {
                        account_short += g;
                    }
                }
                sales
                    .into_iter()
                    .map(to_sale_result)
                    .collect::<Vec<SaleLotResult>>()
            } else {
                Vec::new()
            };

            let ticker = source
                .registry()
                .security_name(sid)
                .map(|n| n.to_string())
                .unwrap_or_default();
            positions_out.insert(
                ticker,
                PositionResult {
                    current_shares,
                    target_shares,
                    trade_shares,
                    trade_value,
                    price,
                    lots_sold,
                },
            );
        }

        let ending_cash = account.cash + net_cash_change;
        total_realized += account_realized;
        total_short += account_short;
        total_long += account_long;
        accounts.insert(
            account.name.to_string(),
            AccountResult {
                starting_cash: account.cash,
                ending_cash,
                positions: positions_out,
                realized_gain: account_realized,
                short_term_gain: account_short,
                long_term_gain: account_long,
            },
        );
    }

    let summary = build_summary(
        source,
        allocation,
        &total_target_shares,
        total_realized,
        total_short,
        total_long,
    )?;

    Ok(RebalanceOutput { accounts, summary })
}

fn sell_lots(
    account: &AccountData,
    security: SecurityId,
    shares_to_sell: i64,
    price: Decimal,
    selector: LotSelector,
    as_of: Date,
) -> Vec<SaleAllocation> {
    // Gather this security's lots in a local vec so we can mutate remaining
    // quantities without touching the shared `Arc<[LotData]>`.
    let lots: Vec<_> = account
        .lots
        .iter()
        .filter(|l| l.security == security)
        .cloned()
        .collect();
    if lots.is_empty() {
        return Vec::new();
    }
    let mut remaining: Vec<i64> = lots.iter().map(|l| l.quantity).collect();
    let total_available: i64 = remaining.iter().sum();
    let to_sell = shares_to_sell.min(total_available);
    consume_lots(selector, &lots, &mut remaining, price, as_of, to_sell)
}

fn to_sale_result(s: SaleAllocation) -> SaleLotResult {
    let proceeds = s.proceeds();
    let basis = s.total_basis();
    SaleLotResult {
        lot_id: s.external_id.to_string(),
        shares_sold: s.shares_sold,
        acquired: s.acquired,
        holding_days: s.holding_days,
        is_long_term: s.is_long_term,
        cost_basis: basis,
        proceeds,
        realized_gain: proceeds - basis,
    }
}

fn floor_shares(dollars: Decimal, price: Decimal) -> i64 {
    if price <= Decimal::ZERO || dollars <= Decimal::ZERO {
        return 0;
    }
    (dollars / price).floor().to_i64().unwrap_or(i64::MAX)
}

fn build_summary(
    source: &dyn PortfolioSource,
    allocation: &Allocation,
    total_target_shares: &HashMap<SecurityId, i64>,
    total_realized_gain: Decimal,
    total_short_term_gain: Decimal,
    total_long_term_gain: Decimal,
) -> Result<Summary, RebalanceError> {
    let mut sleeve_drift_bps = BTreeMap::new();
    let mut max_drift_bps: i64 = 0;
    for sleeve in source.sleeves() {
        let target_dollars = allocation
            .sleeve_target_dollars
            .get(&sleeve.id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let mut actual_dollars = Decimal::ZERO;
        for &(sid, _) in &*sleeve.holdings {
            let price = source
                .price(sid)
                .ok_or_else(|| missing_price(source, sid))?;
            let shares = total_target_shares.get(&sid).copied().unwrap_or(0);
            actual_dollars += price * Decimal::from(shares);
        }
        let drift = drift_bps(actual_dollars, target_dollars, allocation.total_value);
        if drift > max_drift_bps {
            max_drift_bps = drift;
        }
        sleeve_drift_bps.insert(sleeve.name.to_string(), drift);
    }

    Ok(Summary {
        total_value: allocation.total_value,
        sleeve_drift_bps,
        max_drift_bps,
        total_realized_gain,
        total_short_term_gain,
        total_long_term_gain,
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

fn missing_price(source: &dyn PortfolioSource, sid: SecurityId) -> RebalanceError {
    let ticker = source
        .registry()
        .security_name(sid)
        .map(|n| n.to_string())
        .unwrap_or_default();
    RebalanceError::MissingPrice { ticker }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocate::allocate;
    use crate::core::InMemoryPortfolio;
    use crate::model::{
        Account, DecimalStr, LotEntry, LotList, PositionEntry, PositionsFile, PricesFile, Sleeve,
        TargetsFile,
    };
    use pretty_assertions::assert_eq;
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;
    use time::macros::date;

    const AS_OF: Date = date!(2025 - 01 - 01);

    fn account(cash: Decimal, positions: &[(&str, i64)]) -> Account {
        Account {
            r#type: None,
            cash,
            positions: positions
                .iter()
                .map(|(t, s)| (t.to_string(), PositionEntry::Shares(*s)))
                .collect(),
        }
    }

    fn account_with_lots(cash: Decimal, positions: &[(&str, Vec<LotEntry>)]) -> Account {
        Account {
            r#type: None,
            cash,
            positions: positions
                .iter()
                .map(|(t, lots)| {
                    (
                        t.to_string(),
                        PositionEntry::Lots(LotList { lots: lots.clone() }),
                    )
                })
                .collect(),
        }
    }

    fn lot(quantity: i64, basis: Decimal, acquired: Date, id: &str) -> LotEntry {
        LotEntry {
            quantity,
            cost_basis: basis,
            acquired,
            id: Some(id.into()),
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

    fn build(p: PositionsFile, pr: PricesFile, t: TargetsFile) -> InMemoryPortfolio {
        InMemoryPortfolio::from_dtos(&p, &pr, &t).unwrap()
    }

    fn build_trades(port: &InMemoryPortfolio, selector: LotSelector) -> RebalanceOutput {
        let alloc = allocate(port).unwrap();
        build_output(port, &alloc, selector, AS_OF).unwrap()
    }

    #[test]
    fn integer_rounding_floors_shares_and_leaves_cash() {
        let p = PositionsFile {
            accounts: BTreeMap::from([("a".to_string(), account(dec!(1000), &[]))]),
        };
        let pr = prices_of(&[("X", dec!(33))]);
        let t = TargetsFile {
            sleeves: BTreeMap::from([(
                "s".to_string(),
                sleeve(dec!(1.0), &[("X", dec!(1.0))], &["a"]),
            )]),
        };
        let port = build(p, pr, t);
        let out = build_trades(&port, LotSelector::Fifo);
        let pos = &out.accounts["a"].positions["X"];
        assert_eq!(pos.target_shares, 30);
        assert_eq!(pos.trade_shares, 30);
        assert_eq!(out.accounts["a"].ending_cash, dec!(10));
    }

    #[test]
    fn liquidates_untargeted_positions() {
        let p = PositionsFile {
            accounts: BTreeMap::from([(
                "a".to_string(),
                account(dec!(0), &[("AAPL", 5), ("VTI", 0)]),
            )]),
        };
        let pr = prices_of(&[("AAPL", dec!(100)), ("VTI", dec!(50))]);
        let t = TargetsFile {
            sleeves: BTreeMap::from([(
                "s".to_string(),
                sleeve(dec!(1.0), &[("VTI", dec!(1.0))], &["a"]),
            )]),
        };
        let port = build(p, pr, t);
        let out = build_trades(&port, LotSelector::Fifo);
        let aapl = &out.accounts["a"].positions["AAPL"];
        assert_eq!(aapl.target_shares, 0);
        assert_eq!(aapl.trade_shares, -5);
        assert_eq!(aapl.trade_value, dec!(-500));
        // No lot info → no per-lot report.
        assert!(aapl.lots_sold.is_empty());
        let vti = &out.accounts["a"].positions["VTI"];
        assert_eq!(vti.target_shares, 10);
        assert_eq!(vti.trade_shares, 10);
        assert_eq!(out.accounts["a"].ending_cash, dec!(0));
    }

    #[test]
    fn ending_cash_never_negative() {
        let p = PositionsFile {
            accounts: BTreeMap::from([
                ("roth".to_string(), account(dec!(1500), &[("VTI", 10)])),
                ("trad".to_string(), account(dec!(200), &[("BND", 50)])),
                (
                    "taxable".to_string(),
                    account(dec!(750), &[("VTI", 40), ("VXUS", 25)]),
                ),
            ]),
        };
        let pr = prices_of(&[
            ("VTI", dec!(250)),
            ("VXUS", dec!(60)),
            ("BND", dec!(75)),
            ("BNDX", dec!(55)),
        ]);
        let t = TargetsFile {
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
        let port = build(p, pr, t);
        let out = build_trades(&port, LotSelector::Fifo);
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
        let p = PositionsFile {
            accounts: BTreeMap::from([("a".to_string(), account(dec!(0), &[("VTI", 10)]))]),
        };
        let pr = prices_of(&[("VTI", dec!(100)), ("BND", dec!(50))]);
        let t = TargetsFile {
            sleeves: BTreeMap::from([(
                "bonds".to_string(),
                sleeve(dec!(1.0), &[("BND", dec!(1.0))], &["a"]),
            )]),
        };
        let port = build(p, pr, t);
        let out = build_trades(&port, LotSelector::Fifo);
        assert_eq!(out.accounts["a"].positions["VTI"].trade_shares, -10);
        assert_eq!(out.accounts["a"].positions["BND"].target_shares, 20);
        assert_eq!(out.accounts["a"].ending_cash, dec!(0));
    }

    #[test]
    fn drift_bps_signed_then_abs() {
        assert_eq!(super::drift_bps(dec!(100), dec!(0), dec!(1_000_000)), 1);
        assert_eq!(super::drift_bps(dec!(0), dec!(100), dec!(1_000_000)), 1);
        assert_eq!(super::drift_bps(dec!(0), dec!(0), dec!(1_000_000)), 0);
    }

    #[test]
    fn summary_zero_drift_when_dollars_align() {
        let p = PositionsFile {
            accounts: BTreeMap::from([("a".to_string(), account(dec!(10000), &[]))]),
        };
        let pr = prices_of(&[("VTI", dec!(100))]);
        let t = TargetsFile {
            sleeves: BTreeMap::from([(
                "us".to_string(),
                sleeve(dec!(1.0), &[("VTI", dec!(1.0))], &["a"]),
            )]),
        };
        let port = build(p, pr, t);
        let out = build_trades(&port, LotSelector::Fifo);
        assert_eq!(out.summary.max_drift_bps, 0);
        assert_eq!(out.summary.sleeve_drift_bps["us"], 0);
        assert_eq!(out.summary.total_value, dec!(10000));
    }

    #[test]
    fn unallocated_account_keeps_cash_and_liquidates() {
        let p = PositionsFile {
            accounts: BTreeMap::from([
                ("main".to_string(), account(dec!(5000), &[])),
                ("side".to_string(), account(dec!(100), &[("OLD", 2)])),
            ]),
        };
        let pr = prices_of(&[("VTI", dec!(50)), ("OLD", dec!(10))]);
        let t = TargetsFile {
            sleeves: BTreeMap::from([(
                "us".to_string(),
                sleeve(dec!(1.0), &[("VTI", dec!(1.0))], &["main"]),
            )]),
        };
        let port = build(p, pr, t);
        let out = build_trades(&port, LotSelector::Fifo);
        assert_eq!(out.accounts["side"].positions["OLD"].trade_shares, -2);
        assert!(out.accounts["side"].ending_cash >= Decimal::ZERO);
    }

    fn lot_liquidation_scenario() -> (PositionsFile, PricesFile, TargetsFile) {
        let p = PositionsFile {
            accounts: BTreeMap::from([(
                "taxable".to_string(),
                account_with_lots(
                    dec!(0),
                    &[(
                        "VTI",
                        vec![
                            lot(5, dec!(100), date!(2022 - 01 - 01), "old"),
                            lot(5, dec!(300), date!(2024 - 06 - 01), "new"),
                        ],
                    )],
                ),
            )]),
        };
        let pr = prices_of(&[("VTI", dec!(200)), ("BND", dec!(100))]);
        let t = TargetsFile {
            sleeves: BTreeMap::from([(
                "bonds".to_string(),
                sleeve(dec!(1.0), &[("BND", dec!(1.0))], &["taxable"]),
            )]),
        };
        (p, pr, t)
    }

    #[test]
    fn fifo_sells_oldest_lot_first_and_reports_long_term_gain() {
        let (p, pr, t) = lot_liquidation_scenario();
        let port = build(p, pr, t);
        let out = build_trades(&port, LotSelector::Fifo);
        let vti = &out.accounts["taxable"].positions["VTI"];
        assert_eq!(vti.trade_shares, -10);
        assert_eq!(vti.lots_sold.len(), 2);
        assert_eq!(vti.lots_sold[0].lot_id, "old");
        assert_eq!(vti.lots_sold[0].shares_sold, 5);
        assert_eq!(vti.lots_sold[0].proceeds, dec!(1000));
        assert_eq!(vti.lots_sold[0].cost_basis, dec!(500));
        assert_eq!(vti.lots_sold[0].realized_gain, dec!(500));
        assert!(vti.lots_sold[0].is_long_term);
        assert_eq!(vti.lots_sold[1].lot_id, "new");
        assert_eq!(vti.lots_sold[1].realized_gain, dec!(-500));
        assert!(!vti.lots_sold[1].is_long_term);
        assert_eq!(out.accounts["taxable"].realized_gain, dec!(0));
        assert_eq!(out.accounts["taxable"].long_term_gain, dec!(500));
        assert_eq!(out.accounts["taxable"].short_term_gain, dec!(-500));
        assert_eq!(out.summary.total_realized_gain, dec!(0));
    }

    #[test]
    fn hifo_sells_highest_basis_first() {
        let (p, pr, t) = lot_liquidation_scenario();
        let port = build(p, pr, t);
        let out = build_trades(&port, LotSelector::Hifo);
        let vti = &out.accounts["taxable"].positions["VTI"];
        assert_eq!(vti.lots_sold[0].lot_id, "new");
        assert_eq!(vti.lots_sold[0].realized_gain, dec!(-500));
        assert_eq!(vti.lots_sold[1].lot_id, "old");
    }

    #[test]
    fn lifo_sells_newest_first() {
        let (p, pr, t) = lot_liquidation_scenario();
        let port = build(p, pr, t);
        let out = build_trades(&port, LotSelector::Lifo);
        let vti = &out.accounts["taxable"].positions["VTI"];
        assert_eq!(vti.lots_sold[0].lot_id, "new");
        assert_eq!(vti.lots_sold[1].lot_id, "old");
    }

    #[test]
    fn partial_sell_consumes_exactly_the_requested_shares() {
        // Targets want 5 shares of VTI (worth $1000) and all remaining cash
        // in BND — forcing a 5-share sell of VTI from a 10-share position.
        let p = PositionsFile {
            accounts: BTreeMap::from([(
                "taxable".to_string(),
                account_with_lots(
                    dec!(0),
                    &[(
                        "VTI",
                        vec![
                            lot(5, dec!(50), date!(2022 - 01 - 01), "old"),
                            lot(5, dec!(150), date!(2024 - 06 - 01), "new"),
                        ],
                    )],
                ),
            )]),
        };
        let pr = prices_of(&[("VTI", dec!(200)), ("BND", dec!(100))]);
        let t = TargetsFile {
            sleeves: BTreeMap::from([
                (
                    "us".to_string(),
                    sleeve(dec!(0.5), &[("VTI", dec!(1.0))], &["taxable"]),
                ),
                (
                    "bonds".to_string(),
                    sleeve(dec!(0.5), &[("BND", dec!(1.0))], &["taxable"]),
                ),
            ]),
        };
        let port = build(p, pr, t);
        let out = build_trades(&port, LotSelector::Fifo);
        let vti = &out.accounts["taxable"].positions["VTI"];
        assert_eq!(vti.trade_shares, -5);
        let total_sold: i64 = vti.lots_sold.iter().map(|l| l.shares_sold).sum();
        assert_eq!(total_sold, 5);
        assert_eq!(vti.lots_sold[0].lot_id, "old");
    }
}
