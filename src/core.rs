use std::collections::HashMap;
use std::sync::Arc;

use rust_decimal::Decimal;

use crate::errors::RebalanceError;
use crate::id::{AccountId, LotId, SecurityId, SleeveId};
use crate::lot::LotData;
use crate::model::{LotEntry, PositionEntry, PositionsFile, PricesFile, TargetsFile};
use crate::registry::{Registry, SharedRegistry};
use crate::source::{AccountData, PortfolioSource, SleeveData};

/// Default, fully in-memory implementation of `PortfolioSource`. Built once
/// from the serde DTOs and then shared immutably across threads via `Arc`.
pub struct InMemoryPortfolio {
    registry: Arc<SharedRegistry>,
    accounts: Vec<AccountData>,
    accounts_by_id: HashMap<AccountId, usize>,
    securities: Vec<SecurityId>,
    prices: HashMap<SecurityId, Decimal>,
    sleeves: Vec<SleeveData>,
    sleeves_by_id: HashMap<SleeveId, usize>,
}

impl InMemoryPortfolio {
    pub fn from_dtos(
        positions: &PositionsFile,
        prices: &PricesFile,
        targets: &TargetsFile,
    ) -> Result<Self, RebalanceError> {
        let registry = Arc::new(SharedRegistry::new());

        let mut prices_map: HashMap<SecurityId, Decimal> =
            HashMap::with_capacity(prices.prices.len());
        for (name, price) in &prices.prices {
            let id = registry.intern_security(name);
            prices_map.insert(id, price.0);
        }

        let mut next_lot: u32 = 0;
        let mut accounts: Vec<AccountData> = Vec::with_capacity(positions.accounts.len());
        let mut accounts_by_id: HashMap<AccountId, usize> =
            HashMap::with_capacity(positions.accounts.len());
        for (name, account) in &positions.accounts {
            let aid = registry.intern_account(name);
            let mut pos: Vec<(SecurityId, i64)> = Vec::with_capacity(account.positions.len());
            let mut lots: Vec<LotData> = Vec::new();
            for (ticker, entry) in &account.positions {
                let sid = registry.intern_security(ticker);
                pos.push((sid, entry.total_shares()));
                if let PositionEntry::Lots(list) = entry {
                    for lot in &list.lots {
                        lots.push(build_lot(&mut next_lot, aid, sid, lot));
                    }
                }
            }
            pos.sort_by_key(|(s, _)| *s);
            // Keep lots grouped by security, then by acquired date, then by id
            // so callers see a stable ordering.
            lots.sort_by(|a, b| {
                a.security
                    .cmp(&b.security)
                    .then_with(|| a.acquired.cmp(&b.acquired))
                    .then_with(|| a.id.cmp(&b.id))
            });
            let name_arc = registry.account_name(aid).expect("just interned");
            let kind_arc = account.r#type.as_deref().map(Arc::<str>::from);
            let idx = accounts.len();
            accounts.push(AccountData {
                id: aid,
                name: name_arc,
                kind: kind_arc,
                cash: account.cash,
                positions: Arc::<[(SecurityId, i64)]>::from(pos),
                lots: Arc::<[LotData]>::from(lots),
            });
            accounts_by_id.insert(aid, idx);
        }

        let mut sleeves: Vec<SleeveData> = Vec::with_capacity(targets.sleeves.len());
        let mut sleeves_by_id: HashMap<SleeveId, usize> =
            HashMap::with_capacity(targets.sleeves.len());
        for (name, sleeve) in &targets.sleeves {
            let id = registry.intern_sleeve(name);
            let mut holdings: Vec<(SecurityId, Decimal)> =
                Vec::with_capacity(sleeve.holdings.len());
            for (ticker, weight) in &sleeve.holdings {
                holdings.push((registry.intern_security(ticker), weight.0));
            }
            holdings.sort_by_key(|(s, _)| *s);
            let preferred: Vec<AccountId> = sleeve
                .preferred_accounts
                .iter()
                .map(|n| registry.intern_account(n))
                .collect();
            let name_arc = registry.sleeve_name(id).expect("just interned");
            let idx = sleeves.len();
            sleeves.push(SleeveData {
                id,
                name: name_arc,
                target_weight: sleeve.target_weight,
                holdings: Arc::<[(SecurityId, Decimal)]>::from(holdings),
                preferred_accounts: Arc::<[AccountId]>::from(preferred),
            });
            sleeves_by_id.insert(id, idx);
        }

        let mut securities: Vec<SecurityId> = prices_map.keys().copied().collect();
        securities.sort();

        Ok(Self {
            registry,
            accounts,
            accounts_by_id,
            securities,
            prices: prices_map,
            sleeves,
            sleeves_by_id,
        })
    }

    pub fn shared_registry(&self) -> Arc<SharedRegistry> {
        Arc::clone(&self.registry)
    }
}

fn build_lot(
    next: &mut u32,
    account: AccountId,
    security: SecurityId,
    entry: &LotEntry,
) -> LotData {
    let id = LotId(*next);
    *next += 1;
    let external = entry.id.as_deref().unwrap_or("");
    LotData {
        id,
        external_id: Arc::<str>::from(external),
        account,
        security,
        quantity: entry.quantity,
        cost_basis_per_share: entry.cost_basis,
        acquired: entry.acquired,
    }
}

impl PortfolioSource for InMemoryPortfolio {
    fn registry(&self) -> &dyn Registry {
        &*self.registry
    }

    fn accounts(&self) -> &[AccountData] {
        &self.accounts
    }

    fn account(&self, id: AccountId) -> Option<&AccountData> {
        self.accounts_by_id.get(&id).map(|&i| &self.accounts[i])
    }

    fn securities(&self) -> &[SecurityId] {
        &self.securities
    }

    fn price(&self, id: SecurityId) -> Option<Decimal> {
        self.prices.get(&id).copied()
    }

    fn sleeves(&self) -> &[SleeveData] {
        &self.sleeves
    }

    fn sleeve(&self, id: SleeveId) -> Option<&SleeveData> {
        self.sleeves_by_id.get(&id).map(|&i| &self.sleeves[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Account, DecimalStr, LotList, Sleeve};
    use pretty_assertions::assert_eq;
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;
    use time::macros::date;

    fn sample_inputs() -> (PositionsFile, PricesFile, TargetsFile) {
        let positions = PositionsFile {
            accounts: BTreeMap::from([(
                "roth".to_string(),
                Account {
                    r#type: Some("roth".into()),
                    cash: dec!(1000),
                    positions: BTreeMap::from([("VTI".to_string(), PositionEntry::Shares(4))]),
                },
            )]),
        };
        let prices = PricesFile {
            prices: BTreeMap::from([
                ("VTI".to_string(), DecimalStr(dec!(100))),
                ("BND".to_string(), DecimalStr(dec!(50))),
            ]),
        };
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "us".to_string(),
                Sleeve {
                    target_weight: dec!(1.0),
                    holdings: BTreeMap::from([("VTI".to_string(), DecimalStr(dec!(1.0)))]),
                    preferred_accounts: vec!["roth".to_string()],
                },
            )]),
        };
        (positions, prices, targets)
    }

    #[test]
    fn lookups_round_trip_through_registry() {
        let (p, pr, t) = sample_inputs();
        let portfolio = InMemoryPortfolio::from_dtos(&p, &pr, &t).unwrap();
        let roth = portfolio.registry().account_id("roth").unwrap();
        let a = portfolio.account(roth).unwrap();
        assert_eq!(&*a.name, "roth");
        assert_eq!(a.cash, dec!(1000));
    }

    #[test]
    fn prices_are_indexed_by_id() {
        let (p, pr, t) = sample_inputs();
        let portfolio = InMemoryPortfolio::from_dtos(&p, &pr, &t).unwrap();
        let vti = portfolio.registry().security_id("VTI").unwrap();
        assert_eq!(portfolio.price(vti), Some(dec!(100)));
    }

    #[test]
    fn account_without_lots_has_empty_lots_arc() {
        let (p, pr, t) = sample_inputs();
        let portfolio = InMemoryPortfolio::from_dtos(&p, &pr, &t).unwrap();
        let roth = portfolio.registry().account_id("roth").unwrap();
        let a = portfolio.account(roth).unwrap();
        assert!(a.lots.is_empty());
    }

    #[test]
    fn lots_are_parsed_and_grouped_by_security() {
        let positions = PositionsFile {
            accounts: BTreeMap::from([(
                "taxable".to_string(),
                Account {
                    r#type: Some("taxable".into()),
                    cash: dec!(0),
                    positions: BTreeMap::from([
                        (
                            "VTI".to_string(),
                            PositionEntry::Lots(LotList {
                                lots: vec![
                                    crate::model::LotEntry {
                                        quantity: 5,
                                        cost_basis: dec!(200),
                                        acquired: date!(2023 - 06 - 01),
                                        id: Some("vti-2023".into()),
                                    },
                                    crate::model::LotEntry {
                                        quantity: 5,
                                        cost_basis: dec!(180),
                                        acquired: date!(2022 - 01 - 15),
                                        id: Some("vti-2022".into()),
                                    },
                                ],
                            }),
                        ),
                        ("BND".to_string(), PositionEntry::Shares(0)),
                    ]),
                },
            )]),
        };
        let prices = PricesFile {
            prices: BTreeMap::from([
                ("VTI".to_string(), DecimalStr(dec!(250))),
                ("BND".to_string(), DecimalStr(dec!(75))),
            ]),
        };
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "us".to_string(),
                Sleeve {
                    target_weight: dec!(1.0),
                    holdings: BTreeMap::from([("VTI".to_string(), DecimalStr(dec!(1.0)))]),
                    preferred_accounts: vec!["taxable".into()],
                },
            )]),
        };
        let portfolio = InMemoryPortfolio::from_dtos(&positions, &prices, &targets).unwrap();
        let tax = portfolio.registry().account_id("taxable").unwrap();
        let vti = portfolio.registry().security_id("VTI").unwrap();
        let a = portfolio.account(tax).unwrap();
        assert_eq!(a.positions.iter().find(|(s, _)| *s == vti).unwrap().1, 10);
        let lots: Vec<_> = portfolio.lots_for(tax, vti);
        assert_eq!(lots.len(), 2);
        // Sorted by acquired date: 2022 first.
        assert_eq!(&*lots[0].external_id, "vti-2022");
        assert_eq!(&*lots[1].external_id, "vti-2023");
    }

    #[test]
    fn sleeve_preferred_account_ids_follow_input_order() {
        let positions = PositionsFile {
            accounts: BTreeMap::from([
                (
                    "a".to_string(),
                    Account {
                        r#type: None,
                        cash: dec!(1),
                        positions: BTreeMap::new(),
                    },
                ),
                (
                    "b".to_string(),
                    Account {
                        r#type: None,
                        cash: dec!(1),
                        positions: BTreeMap::new(),
                    },
                ),
            ]),
        };
        let prices = PricesFile {
            prices: BTreeMap::from([("X".to_string(), DecimalStr(dec!(1)))]),
        };
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "s".to_string(),
                Sleeve {
                    target_weight: dec!(1.0),
                    holdings: BTreeMap::from([("X".to_string(), DecimalStr(dec!(1.0)))]),
                    preferred_accounts: vec!["b".to_string(), "a".to_string()],
                },
            )]),
        };
        let portfolio = InMemoryPortfolio::from_dtos(&positions, &prices, &targets).unwrap();
        let sid = portfolio.registry().sleeve_id("s").unwrap();
        let s = portfolio.sleeve(sid).unwrap();
        let names: Vec<Arc<str>> = s
            .preferred_accounts
            .iter()
            .map(|id| portfolio.registry().account_name(*id).unwrap())
            .collect();
        assert_eq!(&*names[0], "b");
        assert_eq!(&*names[1], "a");
    }
}
