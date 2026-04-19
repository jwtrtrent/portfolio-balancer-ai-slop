//! Portfolio rebalancer library.
//!
//! The library is organised around three abstractions:
//!
//! - [`PortfolioSource`]: read-only, thread-safe view of a portfolio.
//! - [`OutputSink`]: write-only destination for a [`RebalanceOutput`].
//! - [`StoreLoader`]: pairs a source with a sink for a given backend
//!   (JSON today, SQLite/Postgres later).
//!
//! A [`RebalanceEngine`] consumes a `&dyn PortfolioSource` and produces a
//! `RebalanceOutput`. The default pipeline is validate → allocate → build
//! trades, exposed as [`DefaultEngine`]. Tax lots are first-class: sell
//! trades are expanded into per-lot sale allocations using the configured
//! [`LotSelector`] so callers can report realised gain/loss split by
//! holding period.

pub mod allocate;
pub mod core;
pub mod engine;
pub mod errors;
pub mod id;
pub mod io_json;
pub mod lot;
pub mod model;
pub mod policy;
pub mod rebalance;
pub mod registry;
pub mod sink;
pub mod source;
pub mod store;
pub mod validate;

pub use core::InMemoryPortfolio;
pub use engine::{DefaultEngine, PolicyAwareEngine, RebalanceEngine};
pub use errors::RebalanceError;
pub use id::{AccountId, LotId, SecurityId, SleeveId};
pub use lot::{LotData, LotSelector, SaleAllocation};
pub use model::{
    Account, AccountResult, DecimalStr, LotEntry, LotList, PolicyViolation, PositionEntry,
    PositionResult, PositionsFile, PricesFile, RebalanceOutput, SaleLotResult, Sleeve, Summary,
    TargetsFile,
};
pub use policy::{PolicyAction, PolicyFile, PolicySet, PolicySpec};
pub use registry::{Registry, SharedRegistry};
pub use sink::OutputSink;
pub use source::{AccountData, PortfolioSource, SleeveData};
pub use store::{JsonOutputSink, JsonStoreLoader, LoadedStore, StoreLoader};

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;
    use time::macros::date;

    fn shares(n: i64) -> PositionEntry {
        PositionEntry::Shares(n)
    }

    #[test]
    fn end_to_end_smoke() {
        let positions = PositionsFile {
            accounts: BTreeMap::from([
                (
                    "roth".to_string(),
                    Account {
                        r#type: Some("roth".into()),
                        cash: dec!(1500),
                        positions: BTreeMap::from([("VTI".to_string(), shares(10))]),
                    },
                ),
                (
                    "trad".to_string(),
                    Account {
                        r#type: Some("traditional".into()),
                        cash: dec!(200),
                        positions: BTreeMap::from([("BND".to_string(), shares(50))]),
                    },
                ),
                (
                    "taxable".to_string(),
                    Account {
                        r#type: Some("taxable".into()),
                        cash: dec!(750),
                        positions: BTreeMap::from([
                            ("VTI".to_string(), shares(40)),
                            ("VXUS".to_string(), shares(25)),
                        ]),
                    },
                ),
            ]),
        };
        let prices = PricesFile {
            prices: BTreeMap::from([
                ("VTI".to_string(), DecimalStr(dec!(250))),
                ("VXUS".to_string(), DecimalStr(dec!(60))),
                ("BND".to_string(), DecimalStr(dec!(75))),
                ("BNDX".to_string(), DecimalStr(dec!(55))),
            ]),
        };
        let targets = TargetsFile {
            sleeves: BTreeMap::from([
                (
                    "us_equity".to_string(),
                    Sleeve {
                        target_weight: dec!(0.5),
                        holdings: BTreeMap::from([("VTI".to_string(), DecimalStr(dec!(1.0)))]),
                        preferred_accounts: vec!["taxable".into(), "roth".into()],
                    },
                ),
                (
                    "intl_equity".to_string(),
                    Sleeve {
                        target_weight: dec!(0.3),
                        holdings: BTreeMap::from([("VXUS".to_string(), DecimalStr(dec!(1.0)))]),
                        preferred_accounts: vec!["taxable".into()],
                    },
                ),
                (
                    "bonds".to_string(),
                    Sleeve {
                        target_weight: dec!(0.2),
                        holdings: BTreeMap::from([
                            ("BND".to_string(), DecimalStr(dec!(0.7))),
                            ("BNDX".to_string(), DecimalStr(dec!(0.3))),
                        ]),
                        preferred_accounts: vec!["roth".into(), "trad".into()],
                    },
                ),
            ]),
        };

        let portfolio = InMemoryPortfolio::from_dtos(&positions, &prices, &targets).unwrap();
        let engine = DefaultEngine::new(LotSelector::Fifo, Some(date!(2025 - 01 - 01)));
        let out = engine.rebalance(&portfolio).unwrap();
        for (id, acct) in &out.accounts {
            assert!(
                acct.ending_cash >= Decimal::ZERO,
                "account {id} negative ending cash: {}",
                acct.ending_cash
            );
        }
        // Total value: 1500 + 200 + 750 + 10*250 + 50*75 + 40*250 + 25*60 = 20_200.
        assert_eq!(out.summary.total_value, dec!(20200));
        assert!(
            out.summary.max_drift_bps < 500,
            "drift too large: {}",
            out.summary.max_drift_bps
        );
        // No lots declared → no realised gains tracked.
        assert_eq!(out.summary.total_realized_gain, Decimal::ZERO);
    }
}
