//! Portfolio rebalancer library.
//!
//! Public entry point: [`rebalance`], which takes parsed input files and
//! returns a [`RebalanceOutput`] describing the trades to execute.

pub mod allocate;
pub mod errors;
pub mod io_json;
pub mod model;
pub mod rebalance;
pub mod validate;

pub use errors::RebalanceError;
pub use model::{
    Account, AccountResult, DecimalStr, PositionResult, PositionsFile, PricesFile, RebalanceOutput,
    Sleeve, Summary, TargetsFile,
};

/// Validate inputs, allocate sleeves to accounts, and compute trades.
pub fn rebalance(
    positions: &PositionsFile,
    prices: &PricesFile,
    targets: &TargetsFile,
) -> Result<RebalanceOutput, RebalanceError> {
    validate::validate(positions, prices, targets)?;
    let allocation = allocate::allocate(positions, prices, targets)?;
    rebalance::build_output(positions, prices, targets, &allocation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;

    #[test]
    fn end_to_end_smoke() {
        let positions = PositionsFile {
            accounts: BTreeMap::from([
                (
                    "roth".to_string(),
                    Account {
                        r#type: Some("roth".into()),
                        cash: dec!(1500),
                        positions: BTreeMap::from([("VTI".to_string(), 10)]),
                    },
                ),
                (
                    "trad".to_string(),
                    Account {
                        r#type: Some("traditional".into()),
                        cash: dec!(200),
                        positions: BTreeMap::from([("BND".to_string(), 50)]),
                    },
                ),
                (
                    "taxable".to_string(),
                    Account {
                        r#type: Some("taxable".into()),
                        cash: dec!(750),
                        positions: BTreeMap::from([
                            ("VTI".to_string(), 40),
                            ("VXUS".to_string(), 25),
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

        let out = rebalance(&positions, &prices, &targets).unwrap();
        // No account ends up with negative cash.
        for (id, acct) in &out.accounts {
            assert!(
                acct.ending_cash >= Decimal::ZERO,
                "account {id} negative ending cash: {}",
                acct.ending_cash
            );
        }
        // Total value: 1500 + 200 + 750 + 10*250 + 50*75 + 40*250 + 25*60 = 20_200.
        assert_eq!(out.summary.total_value, dec!(20200));
        // Drift bounded by whole-share rounding: at $250/share in a $20k
        // portfolio, one share is 124 bps; allow a couple of shares of slack.
        assert!(
            out.summary.max_drift_bps < 500,
            "drift too large: {}",
            out.summary.max_drift_bps
        );
    }
}
