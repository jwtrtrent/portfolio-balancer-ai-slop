use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::errors::RebalanceError;
use crate::model::{AccountId, PositionsFile, PricesFile, SleeveId, TargetsFile, Ticker};

/// Result of mapping sleeve dollar targets onto account/ticker dollar targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allocation {
    /// Target dollars per `(account, ticker)`.
    pub per_account_ticker_dollars: BTreeMap<AccountId, BTreeMap<Ticker, Decimal>>,
    /// Total target dollars per sleeve (= portfolio_total × sleeve.target_weight).
    pub sleeve_target_dollars: BTreeMap<SleeveId, Decimal>,
    /// Starting total value of each account (cash + Σ positions × price).
    pub account_total_value: BTreeMap<AccountId, Decimal>,
    /// Portfolio total starting value.
    pub total_value: Decimal,
}

/// Smallest unit of money we'll bother to redistribute. Anything below this
/// is rounded away as residual cash.
const ALLOCATION_DUST: Decimal = dec!(0.01);

pub fn allocate(
    positions: &PositionsFile,
    prices: &PricesFile,
    targets: &TargetsFile,
) -> Result<Allocation, RebalanceError> {
    let account_total_value = compute_account_values(positions, prices)?;
    let total_value: Decimal = account_total_value.values().copied().sum();
    if total_value <= Decimal::ZERO {
        return Err(RebalanceError::ZeroPortfolioValue);
    }

    let mut remaining_capacity = account_total_value.clone();
    let mut per_account_ticker_dollars: BTreeMap<AccountId, BTreeMap<Ticker, Decimal>> =
        BTreeMap::new();
    let mut sleeve_target_dollars: BTreeMap<SleeveId, Decimal> = BTreeMap::new();

    for (sleeve_id, sleeve) in &targets.sleeves {
        let sleeve_dollars = total_value * sleeve.target_weight;
        sleeve_target_dollars.insert(sleeve_id.clone(), sleeve_dollars);
        let mut remaining = sleeve_dollars;

        // Phase 1: fill preferred accounts in order.
        for account in &sleeve.preferred_accounts {
            if remaining <= ALLOCATION_DUST {
                break;
            }
            let cap = remaining_capacity.get(account).copied().unwrap_or_default();
            if cap <= Decimal::ZERO {
                continue;
            }
            let take = remaining.min(cap);
            credit_sleeve(&mut per_account_ticker_dollars, account, sleeve, take);
            remaining -= take;
            *remaining_capacity.get_mut(account).unwrap() -= take;
        }

        // Phase 2: spill equally across remaining accounts that still have
        // capacity. Iterates because a uniform split may exceed an account's
        // remaining capacity, in which case we cap that account and re-split.
        if remaining > ALLOCATION_DUST {
            let preferred: BTreeSet<&str> = sleeve
                .preferred_accounts
                .iter()
                .map(|s| s.as_str())
                .collect();
            spill_equally(
                &mut remaining,
                &mut remaining_capacity,
                &mut per_account_ticker_dollars,
                sleeve,
                &preferred,
            );
        }

        // Phase 3: if there's still remainder (preferred filled all non-preferred
        // capacity but preferred accounts still have room), make one last sweep
        // over any account with capacity.
        if remaining > ALLOCATION_DUST {
            spill_equally(
                &mut remaining,
                &mut remaining_capacity,
                &mut per_account_ticker_dollars,
                sleeve,
                &BTreeSet::new(),
            );
        }
    }

    Ok(Allocation {
        per_account_ticker_dollars,
        sleeve_target_dollars,
        account_total_value,
        total_value,
    })
}

fn compute_account_values(
    positions: &PositionsFile,
    prices: &PricesFile,
) -> Result<BTreeMap<AccountId, Decimal>, RebalanceError> {
    let mut out = BTreeMap::new();
    for (account_id, account) in &positions.accounts {
        let mut total = account.cash;
        for (ticker, shares) in &account.positions {
            let price = prices
                .prices
                .get(ticker)
                .ok_or_else(|| RebalanceError::MissingPrice {
                    ticker: ticker.clone(),
                })?;
            total += price.0 * Decimal::from(*shares);
        }
        out.insert(account_id.clone(), total);
    }
    Ok(out)
}

fn credit_sleeve(
    per_account_ticker_dollars: &mut BTreeMap<AccountId, BTreeMap<Ticker, Decimal>>,
    account: &str,
    sleeve: &crate::model::Sleeve,
    dollars: Decimal,
) {
    let entry = per_account_ticker_dollars
        .entry(account.to_string())
        .or_default();
    for (ticker, sub_weight) in &sleeve.holdings {
        let amount = dollars * sub_weight.0;
        *entry.entry(ticker.clone()).or_insert(Decimal::ZERO) += amount;
    }
}

fn spill_equally(
    remaining: &mut Decimal,
    remaining_capacity: &mut BTreeMap<AccountId, Decimal>,
    per_account_ticker_dollars: &mut BTreeMap<AccountId, BTreeMap<Ticker, Decimal>>,
    sleeve: &crate::model::Sleeve,
    excluded: &BTreeSet<&str>,
) {
    loop {
        let eligible: Vec<AccountId> = remaining_capacity
            .iter()
            .filter(|(id, cap)| **cap > Decimal::ZERO && !excluded.contains(id.as_str()))
            .map(|(id, _)| id.clone())
            .collect();
        if eligible.is_empty() || *remaining <= ALLOCATION_DUST {
            break;
        }
        let share = *remaining / Decimal::from(eligible.len() as i64);
        let mut progressed = Decimal::ZERO;
        for account in &eligible {
            let cap = remaining_capacity[account];
            let take = share.min(cap);
            if take <= Decimal::ZERO {
                continue;
            }
            credit_sleeve(per_account_ticker_dollars, account, sleeve, take);
            *remaining_capacity.get_mut(account).unwrap() -= take;
            *remaining -= take;
            progressed += take;
        }
        if progressed <= ALLOCATION_DUST {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Account, DecimalStr, Sleeve};
    use pretty_assertions::assert_eq;

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
    fn fits_entirely_in_first_preferred() {
        let positions = PositionsFile {
            accounts: BTreeMap::from([
                ("roth".to_string(), account(dec!(10000), &[])),
                ("taxable".to_string(), account(dec!(10000), &[])),
            ]),
        };
        let prices = prices_of(&[("VTI", dec!(100))]);
        let targets = TargetsFile {
            sleeves: BTreeMap::from([
                (
                    "us".to_string(),
                    sleeve(dec!(0.5), &[("VTI", dec!(1.0))], &["taxable"]),
                ),
                (
                    "bonds".to_string(),
                    sleeve(dec!(0.5), &[("VTI", dec!(1.0))], &["roth"]),
                ),
            ]),
        };
        let alloc = allocate(&positions, &prices, &targets).unwrap();
        // 50% of 20k = 10k each, taxable gets us, roth gets bonds.
        assert_eq!(
            alloc.per_account_ticker_dollars["taxable"]["VTI"],
            dec!(10000)
        );
        assert_eq!(alloc.per_account_ticker_dollars["roth"]["VTI"], dec!(10000));
        assert_eq!(alloc.total_value, dec!(20000));
    }

    #[test]
    fn spills_to_next_preferred_when_first_full() {
        // total 10k. sleeve = 100% of total = 10k, prefer roth (cap 1k) then taxable (cap 9k).
        let positions = PositionsFile {
            accounts: BTreeMap::from([
                ("roth".to_string(), account(dec!(1000), &[])),
                ("taxable".to_string(), account(dec!(9000), &[])),
            ]),
        };
        let prices = prices_of(&[("BND", dec!(50))]);
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "bonds".to_string(),
                sleeve(dec!(1.0), &[("BND", dec!(1.0))], &["roth", "taxable"]),
            )]),
        };
        let alloc = allocate(&positions, &prices, &targets).unwrap();
        assert_eq!(alloc.per_account_ticker_dollars["roth"]["BND"], dec!(1000));
        assert_eq!(
            alloc.per_account_ticker_dollars["taxable"]["BND"],
            dec!(9000)
        );
    }

    #[test]
    fn spills_equally_to_non_preferred_when_preferred_full() {
        // total = 30k. sleeve 100%. prefer "small" (cap 6k). Spill 24k equally
        // across the two remaining 12k accounts.
        let positions = PositionsFile {
            accounts: BTreeMap::from([
                ("small".to_string(), account(dec!(6000), &[])),
                ("a".to_string(), account(dec!(12000), &[])),
                ("b".to_string(), account(dec!(12000), &[])),
            ]),
        };
        let prices = prices_of(&[("X", dec!(1))]);
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "s".to_string(),
                sleeve(dec!(1.0), &[("X", dec!(1.0))], &["small"]),
            )]),
        };
        let alloc = allocate(&positions, &prices, &targets).unwrap();
        assert_eq!(alloc.per_account_ticker_dollars["small"]["X"], dec!(6000));
        assert_eq!(alloc.per_account_ticker_dollars["a"]["X"], dec!(12000));
        assert_eq!(alloc.per_account_ticker_dollars["b"]["X"], dec!(12000));
    }

    #[test]
    fn equal_spill_redistributes_when_one_account_caps() {
        // total = 30k. sleeve 100%. No preferred. Three accounts: 1k, 14.5k, 14.5k.
        // First pass: each gets 10k -> "tiny" caps at 1k, others fully take 10k each.
        // Remaining 9k split between two accounts of cap 4.5k each -> 4.5k each.
        let positions = PositionsFile {
            accounts: BTreeMap::from([
                ("tiny".to_string(), account(dec!(1000), &[])),
                ("a".to_string(), account(dec!(14500), &[])),
                ("b".to_string(), account(dec!(14500), &[])),
            ]),
        };
        let prices = prices_of(&[("X", dec!(1))]);
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "s".to_string(),
                sleeve(dec!(1.0), &[("X", dec!(1.0))], &[]),
            )]),
        };
        let alloc = allocate(&positions, &prices, &targets).unwrap();
        assert_eq!(alloc.per_account_ticker_dollars["tiny"]["X"], dec!(1000));
        assert_eq!(alloc.per_account_ticker_dollars["a"]["X"], dec!(14500));
        assert_eq!(alloc.per_account_ticker_dollars["b"]["X"], dec!(14500));
    }

    #[test]
    fn sub_weights_split_within_sleeve() {
        let positions = PositionsFile {
            accounts: BTreeMap::from([("a".to_string(), account(dec!(10000), &[]))]),
        };
        let prices = prices_of(&[("BND", dec!(75)), ("BNDX", dec!(55))]);
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "bonds".to_string(),
                sleeve(
                    dec!(1.0),
                    &[("BND", dec!(0.7)), ("BNDX", dec!(0.3))],
                    &["a"],
                ),
            )]),
        };
        let alloc = allocate(&positions, &prices, &targets).unwrap();
        assert_eq!(alloc.per_account_ticker_dollars["a"]["BND"], dec!(7000.0));
        assert_eq!(alloc.per_account_ticker_dollars["a"]["BNDX"], dec!(3000.0));
    }

    #[test]
    fn zero_portfolio_value_errors() {
        let positions = PositionsFile {
            accounts: BTreeMap::from([("a".to_string(), account(dec!(0), &[]))]),
        };
        let prices = prices_of(&[]);
        let targets = TargetsFile {
            sleeves: BTreeMap::new(),
        };
        let err = allocate(&positions, &prices, &targets).unwrap_err();
        assert!(matches!(err, RebalanceError::ZeroPortfolioValue));
    }

    #[test]
    fn account_value_includes_existing_positions() {
        let positions = PositionsFile {
            accounts: BTreeMap::from([("a".to_string(), account(dec!(100), &[("VTI", 10)]))]),
        };
        let prices = prices_of(&[("VTI", dec!(50))]);
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "s".to_string(),
                sleeve(dec!(1.0), &[("VTI", dec!(1.0))], &["a"]),
            )]),
        };
        let alloc = allocate(&positions, &prices, &targets).unwrap();
        // 100 cash + 10*50 positions = 600
        assert_eq!(alloc.total_value, dec!(600));
        assert_eq!(alloc.per_account_ticker_dollars["a"]["VTI"], dec!(600));
    }
}
