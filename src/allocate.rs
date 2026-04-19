use std::collections::{HashMap, HashSet};

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::errors::RebalanceError;
use crate::id::{AccountId, SecurityId, SleeveId};
use crate::source::{PortfolioSource, SleeveData};

/// Result of mapping sleeve dollar targets onto account/security dollar targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allocation {
    /// Target dollars per `(account, security)`.
    pub per_account_ticker_dollars: HashMap<AccountId, HashMap<SecurityId, Decimal>>,
    /// Total target dollars per sleeve (= portfolio_total × sleeve.target_weight).
    pub sleeve_target_dollars: HashMap<SleeveId, Decimal>,
    /// Starting total value of each account (cash + Σ positions × price).
    pub account_total_value: HashMap<AccountId, Decimal>,
    /// Portfolio total starting value.
    pub total_value: Decimal,
}

/// Smallest unit of money we'll bother to redistribute. Anything below this
/// is rounded away as residual cash.
const ALLOCATION_DUST: Decimal = dec!(0.01);

pub fn allocate(source: &dyn PortfolioSource) -> Result<Allocation, RebalanceError> {
    let account_total_value = compute_account_values(source)?;
    let total_value: Decimal = account_total_value.values().copied().sum();
    if total_value <= Decimal::ZERO {
        return Err(RebalanceError::ZeroPortfolioValue);
    }

    let mut remaining_capacity = account_total_value.clone();
    let mut per_account_ticker_dollars: HashMap<AccountId, HashMap<SecurityId, Decimal>> =
        HashMap::new();
    let mut sleeve_target_dollars: HashMap<SleeveId, Decimal> = HashMap::new();

    for sleeve in source.sleeves() {
        let sleeve_dollars = total_value * sleeve.target_weight;
        sleeve_target_dollars.insert(sleeve.id, sleeve_dollars);
        let mut remaining = sleeve_dollars;

        // Phase 1: fill preferred accounts in order.
        for &aid in &*sleeve.preferred_accounts {
            if remaining <= ALLOCATION_DUST {
                break;
            }
            let cap = remaining_capacity.get(&aid).copied().unwrap_or_default();
            if cap <= Decimal::ZERO {
                continue;
            }
            let take = remaining.min(cap);
            credit_sleeve(&mut per_account_ticker_dollars, aid, sleeve, take);
            remaining -= take;
            *remaining_capacity.get_mut(&aid).unwrap() -= take;
        }

        // Phase 2: spill equally across non-preferred accounts that still have
        // capacity. Iterates because a uniform split may exceed an account's
        // remaining capacity, in which case we cap that account and re-split.
        if remaining > ALLOCATION_DUST {
            let preferred: HashSet<AccountId> = sleeve.preferred_accounts.iter().copied().collect();
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
                &HashSet::new(),
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
    source: &dyn PortfolioSource,
) -> Result<HashMap<AccountId, Decimal>, RebalanceError> {
    let mut out = HashMap::with_capacity(source.accounts().len());
    for account in source.accounts() {
        let mut total = account.cash;
        for &(sid, shares) in &*account.positions {
            let price = source.price(sid).ok_or_else(|| {
                let ticker = source
                    .registry()
                    .security_name(sid)
                    .map(|n| n.to_string())
                    .unwrap_or_default();
                RebalanceError::MissingPrice { ticker }
            })?;
            total += price * Decimal::from(shares);
        }
        out.insert(account.id, total);
    }
    Ok(out)
}

fn credit_sleeve(
    per_account_ticker_dollars: &mut HashMap<AccountId, HashMap<SecurityId, Decimal>>,
    account: AccountId,
    sleeve: &SleeveData,
    dollars: Decimal,
) {
    let entry = per_account_ticker_dollars.entry(account).or_default();
    for &(sid, weight) in &*sleeve.holdings {
        *entry.entry(sid).or_insert(Decimal::ZERO) += dollars * weight;
    }
}

fn spill_equally(
    remaining: &mut Decimal,
    remaining_capacity: &mut HashMap<AccountId, Decimal>,
    per_account_ticker_dollars: &mut HashMap<AccountId, HashMap<SecurityId, Decimal>>,
    sleeve: &SleeveData,
    excluded: &HashSet<AccountId>,
) {
    loop {
        let mut eligible: Vec<AccountId> = remaining_capacity
            .iter()
            .filter(|(id, cap)| **cap > Decimal::ZERO && !excluded.contains(*id))
            .map(|(id, _)| *id)
            .collect();
        if eligible.is_empty() || *remaining <= ALLOCATION_DUST {
            break;
        }
        // Sort so splitting is deterministic across `HashMap` iteration orders.
        eligible.sort();
        let share = *remaining / Decimal::from(eligible.len() as i64);
        let mut progressed = Decimal::ZERO;
        for aid in &eligible {
            let cap = remaining_capacity[aid];
            let take = share.min(cap);
            if take <= Decimal::ZERO {
                continue;
            }
            credit_sleeve(per_account_ticker_dollars, *aid, sleeve, take);
            *remaining_capacity.get_mut(aid).unwrap() -= take;
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
    use crate::core::InMemoryPortfolio;
    use crate::model::{
        Account, DecimalStr, PositionEntry, PositionsFile, PricesFile, Sleeve, TargetsFile,
    };
    use pretty_assertions::assert_eq;
    use std::collections::BTreeMap;

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

    fn aid(p: &InMemoryPortfolio, name: &str) -> AccountId {
        p.registry().account_id(name).unwrap()
    }

    fn sid(p: &InMemoryPortfolio, name: &str) -> SecurityId {
        p.registry().security_id(name).unwrap()
    }

    #[test]
    fn fits_entirely_in_first_preferred() {
        let p = PositionsFile {
            accounts: BTreeMap::from([
                ("roth".to_string(), account(dec!(10000), &[])),
                ("taxable".to_string(), account(dec!(10000), &[])),
            ]),
        };
        let pr = prices_of(&[("VTI", dec!(100))]);
        let t = TargetsFile {
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
        let port = build(p, pr, t);
        let alloc = allocate(&port).unwrap();
        let vti = sid(&port, "VTI");
        assert_eq!(
            alloc.per_account_ticker_dollars[&aid(&port, "taxable")][&vti],
            dec!(10000)
        );
        assert_eq!(
            alloc.per_account_ticker_dollars[&aid(&port, "roth")][&vti],
            dec!(10000)
        );
        assert_eq!(alloc.total_value, dec!(20000));
    }

    #[test]
    fn spills_to_next_preferred_when_first_full() {
        let p = PositionsFile {
            accounts: BTreeMap::from([
                ("roth".to_string(), account(dec!(1000), &[])),
                ("taxable".to_string(), account(dec!(9000), &[])),
            ]),
        };
        let pr = prices_of(&[("BND", dec!(50))]);
        let t = TargetsFile {
            sleeves: BTreeMap::from([(
                "bonds".to_string(),
                sleeve(dec!(1.0), &[("BND", dec!(1.0))], &["roth", "taxable"]),
            )]),
        };
        let port = build(p, pr, t);
        let alloc = allocate(&port).unwrap();
        let bnd = sid(&port, "BND");
        assert_eq!(
            alloc.per_account_ticker_dollars[&aid(&port, "roth")][&bnd],
            dec!(1000)
        );
        assert_eq!(
            alloc.per_account_ticker_dollars[&aid(&port, "taxable")][&bnd],
            dec!(9000)
        );
    }

    #[test]
    fn spills_equally_to_non_preferred_when_preferred_full() {
        let p = PositionsFile {
            accounts: BTreeMap::from([
                ("small".to_string(), account(dec!(6000), &[])),
                ("a".to_string(), account(dec!(12000), &[])),
                ("b".to_string(), account(dec!(12000), &[])),
            ]),
        };
        let pr = prices_of(&[("X", dec!(1))]);
        let t = TargetsFile {
            sleeves: BTreeMap::from([(
                "s".to_string(),
                sleeve(dec!(1.0), &[("X", dec!(1.0))], &["small"]),
            )]),
        };
        let port = build(p, pr, t);
        let alloc = allocate(&port).unwrap();
        let x = sid(&port, "X");
        assert_eq!(
            alloc.per_account_ticker_dollars[&aid(&port, "small")][&x],
            dec!(6000)
        );
        assert_eq!(
            alloc.per_account_ticker_dollars[&aid(&port, "a")][&x],
            dec!(12000)
        );
        assert_eq!(
            alloc.per_account_ticker_dollars[&aid(&port, "b")][&x],
            dec!(12000)
        );
    }

    #[test]
    fn equal_spill_redistributes_when_one_account_caps() {
        let p = PositionsFile {
            accounts: BTreeMap::from([
                ("tiny".to_string(), account(dec!(1000), &[])),
                ("a".to_string(), account(dec!(14500), &[])),
                ("b".to_string(), account(dec!(14500), &[])),
            ]),
        };
        let pr = prices_of(&[("X", dec!(1))]);
        let t = TargetsFile {
            sleeves: BTreeMap::from([(
                "s".to_string(),
                sleeve(dec!(1.0), &[("X", dec!(1.0))], &[]),
            )]),
        };
        let port = build(p, pr, t);
        let alloc = allocate(&port).unwrap();
        let x = sid(&port, "X");
        assert_eq!(
            alloc.per_account_ticker_dollars[&aid(&port, "tiny")][&x],
            dec!(1000)
        );
        assert_eq!(
            alloc.per_account_ticker_dollars[&aid(&port, "a")][&x],
            dec!(14500)
        );
        assert_eq!(
            alloc.per_account_ticker_dollars[&aid(&port, "b")][&x],
            dec!(14500)
        );
    }

    #[test]
    fn sub_weights_split_within_sleeve() {
        let p = PositionsFile {
            accounts: BTreeMap::from([("a".to_string(), account(dec!(10000), &[]))]),
        };
        let pr = prices_of(&[("BND", dec!(75)), ("BNDX", dec!(55))]);
        let t = TargetsFile {
            sleeves: BTreeMap::from([(
                "bonds".to_string(),
                sleeve(
                    dec!(1.0),
                    &[("BND", dec!(0.7)), ("BNDX", dec!(0.3))],
                    &["a"],
                ),
            )]),
        };
        let port = build(p, pr, t);
        let alloc = allocate(&port).unwrap();
        let a = aid(&port, "a");
        assert_eq!(
            alloc.per_account_ticker_dollars[&a][&sid(&port, "BND")],
            dec!(7000.0)
        );
        assert_eq!(
            alloc.per_account_ticker_dollars[&a][&sid(&port, "BNDX")],
            dec!(3000.0)
        );
    }

    #[test]
    fn zero_portfolio_value_errors() {
        let p = PositionsFile {
            accounts: BTreeMap::from([("a".to_string(), account(dec!(0), &[]))]),
        };
        let pr = prices_of(&[]);
        let t = TargetsFile {
            sleeves: BTreeMap::new(),
        };
        let port = build(p, pr, t);
        let err = allocate(&port).unwrap_err();
        assert!(matches!(err, RebalanceError::ZeroPortfolioValue));
    }

    #[test]
    fn account_value_includes_existing_positions() {
        let p = PositionsFile {
            accounts: BTreeMap::from([("a".to_string(), account(dec!(100), &[("VTI", 10)]))]),
        };
        let pr = prices_of(&[("VTI", dec!(50))]);
        let t = TargetsFile {
            sleeves: BTreeMap::from([(
                "s".to_string(),
                sleeve(dec!(1.0), &[("VTI", dec!(1.0))], &["a"]),
            )]),
        };
        let port = build(p, pr, t);
        let alloc = allocate(&port).unwrap();
        assert_eq!(alloc.total_value, dec!(600));
        assert_eq!(
            alloc.per_account_ticker_dollars[&aid(&port, "a")][&sid(&port, "VTI")],
            dec!(600)
        );
    }
}
