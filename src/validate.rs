use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::errors::RebalanceError;
use crate::source::PortfolioSource;

/// Tolerance for "weights sum to 1.0" checks.
const WEIGHT_EPSILON: Decimal = dec!(0.0001);

pub fn validate(source: &dyn PortfolioSource) -> Result<(), RebalanceError> {
    validate_prices(source)?;
    validate_positions(source)?;
    validate_targets(source)?;
    Ok(())
}

fn validate_prices(source: &dyn PortfolioSource) -> Result<(), RebalanceError> {
    for &sid in source.securities() {
        let price = source
            .price(sid)
            .expect("security id listed but missing price");
        if price <= Decimal::ZERO {
            let ticker = source
                .registry()
                .security_name(sid)
                .map(|n| n.to_string())
                .unwrap_or_default();
            return Err(RebalanceError::NonPositivePrice {
                ticker,
                price: price.to_string(),
            });
        }
    }
    Ok(())
}

fn validate_positions(source: &dyn PortfolioSource) -> Result<(), RebalanceError> {
    for account in source.accounts() {
        if account.cash < Decimal::ZERO {
            return Err(RebalanceError::NegativeCash {
                account: account.name.to_string(),
                cash: account.cash.to_string(),
            });
        }
        for &(sid, shares) in &*account.positions {
            if shares < 0 {
                let ticker = source
                    .registry()
                    .security_name(sid)
                    .map(|n| n.to_string())
                    .unwrap_or_default();
                return Err(RebalanceError::NegativeShares {
                    account: account.name.to_string(),
                    ticker,
                    shares,
                });
            }
            if source.price(sid).is_none() {
                let ticker = source
                    .registry()
                    .security_name(sid)
                    .map(|n| n.to_string())
                    .unwrap_or_default();
                return Err(RebalanceError::MissingPrice { ticker });
            }
        }
    }
    Ok(())
}

fn validate_targets(source: &dyn PortfolioSource) -> Result<(), RebalanceError> {
    let mut total_weight = Decimal::ZERO;
    for sleeve in source.sleeves() {
        if sleeve.target_weight <= Decimal::ZERO {
            return Err(RebalanceError::NonPositiveTargetWeight {
                sleeve: sleeve.name.to_string(),
                weight: sleeve.target_weight.to_string(),
            });
        }
        total_weight += sleeve.target_weight;

        let mut sub_weight_sum = Decimal::ZERO;
        for &(sid, weight) in &*sleeve.holdings {
            if weight <= Decimal::ZERO {
                let ticker = source
                    .registry()
                    .security_name(sid)
                    .map(|n| n.to_string())
                    .unwrap_or_default();
                return Err(RebalanceError::NonPositiveSubWeight {
                    sleeve: sleeve.name.to_string(),
                    ticker,
                    weight: weight.to_string(),
                });
            }
            sub_weight_sum += weight;
            if source.price(sid).is_none() {
                let ticker = source
                    .registry()
                    .security_name(sid)
                    .map(|n| n.to_string())
                    .unwrap_or_default();
                return Err(RebalanceError::MissingPrice { ticker });
            }
        }
        if (sub_weight_sum - Decimal::ONE).abs() > WEIGHT_EPSILON {
            return Err(RebalanceError::SleeveSubWeightsSum {
                sleeve: sleeve.name.to_string(),
                actual: sub_weight_sum.to_string(),
            });
        }

        for &aid in &*sleeve.preferred_accounts {
            if source.account(aid).is_none() {
                let account = source
                    .registry()
                    .account_name(aid)
                    .map(|n| n.to_string())
                    .unwrap_or_default();
                return Err(RebalanceError::UnknownPreferredAccount {
                    sleeve: sleeve.name.to_string(),
                    account,
                });
            }
        }
    }
    if (total_weight - Decimal::ONE).abs() > WEIGHT_EPSILON {
        return Err(RebalanceError::SleeveTargetWeightsSum {
            actual: total_weight.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::InMemoryPortfolio;
    use crate::model::{Account, DecimalStr, PositionsFile, PricesFile, Sleeve, TargetsFile};
    use std::collections::BTreeMap;

    fn make_inputs() -> (PositionsFile, PricesFile, TargetsFile) {
        let positions = PositionsFile {
            accounts: BTreeMap::from([
                (
                    "roth".to_string(),
                    Account {
                        r#type: Some("roth".into()),
                        cash: dec!(1000),
                        positions: BTreeMap::from([("VTI".to_string(), 10)]),
                    },
                ),
                (
                    "taxable".to_string(),
                    Account {
                        r#type: Some("taxable".into()),
                        cash: dec!(500),
                        positions: BTreeMap::new(),
                    },
                ),
            ]),
        };
        let prices = PricesFile {
            prices: BTreeMap::from([
                ("VTI".to_string(), DecimalStr(dec!(250))),
                ("BND".to_string(), DecimalStr(dec!(75))),
            ]),
        };
        let targets = TargetsFile {
            sleeves: BTreeMap::from([
                (
                    "us_equity".to_string(),
                    Sleeve {
                        target_weight: dec!(0.6),
                        holdings: BTreeMap::from([("VTI".to_string(), DecimalStr(dec!(1.0)))]),
                        preferred_accounts: vec!["taxable".to_string()],
                    },
                ),
                (
                    "bonds".to_string(),
                    Sleeve {
                        target_weight: dec!(0.4),
                        holdings: BTreeMap::from([("BND".to_string(), DecimalStr(dec!(1.0)))]),
                        preferred_accounts: vec!["roth".to_string()],
                    },
                ),
            ]),
        };
        (positions, prices, targets)
    }

    fn source(p: &PositionsFile, pr: &PricesFile, t: &TargetsFile) -> InMemoryPortfolio {
        InMemoryPortfolio::from_dtos(p, pr, t).unwrap()
    }

    #[test]
    fn valid_inputs_pass() {
        let (p, pr, t) = make_inputs();
        validate(&source(&p, &pr, &t)).unwrap();
    }

    #[test]
    fn target_weights_must_sum_to_one() {
        let (p, pr, mut t) = make_inputs();
        t.sleeves.get_mut("bonds").unwrap().target_weight = dec!(0.3);
        let err = validate(&source(&p, &pr, &t)).unwrap_err();
        assert!(matches!(err, RebalanceError::SleeveTargetWeightsSum { .. }));
    }

    #[test]
    fn sub_weights_must_sum_to_one() {
        let (p, pr, mut t) = make_inputs();
        t.sleeves
            .get_mut("us_equity")
            .unwrap()
            .holdings
            .insert("BND".to_string(), DecimalStr(dec!(0.5)));
        let err = validate(&source(&p, &pr, &t)).unwrap_err();
        assert!(matches!(err, RebalanceError::SleeveSubWeightsSum { .. }));
    }

    #[test]
    fn missing_price_for_sleeve_ticker() {
        let (p, mut pr, t) = make_inputs();
        pr.prices.remove("BND");
        let err = validate(&source(&p, &pr, &t)).unwrap_err();
        assert!(matches!(err, RebalanceError::MissingPrice { .. }));
    }

    #[test]
    fn missing_price_for_held_ticker() {
        let (mut p, pr, mut t) = make_inputs();
        p.accounts
            .get_mut("taxable")
            .unwrap()
            .positions
            .insert("AAPL".to_string(), 5);
        t.sleeves.clear();
        t.sleeves.insert(
            "us_equity".to_string(),
            Sleeve {
                target_weight: dec!(1.0),
                holdings: BTreeMap::from([("VTI".to_string(), DecimalStr(dec!(1.0)))]),
                preferred_accounts: vec!["taxable".to_string()],
            },
        );
        let err = validate(&source(&p, &pr, &t)).unwrap_err();
        match err {
            RebalanceError::MissingPrice { ticker } => assert_eq!(ticker, "AAPL"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn unknown_preferred_account() {
        let (p, pr, mut t) = make_inputs();
        t.sleeves
            .get_mut("bonds")
            .unwrap()
            .preferred_accounts
            .push("hsa".to_string());
        let err = validate(&source(&p, &pr, &t)).unwrap_err();
        assert!(matches!(
            err,
            RebalanceError::UnknownPreferredAccount { .. }
        ));
    }

    #[test]
    fn negative_cash_rejected() {
        let (mut p, pr, t) = make_inputs();
        p.accounts.get_mut("roth").unwrap().cash = dec!(-1);
        let err = validate(&source(&p, &pr, &t)).unwrap_err();
        assert!(matches!(err, RebalanceError::NegativeCash { .. }));
    }

    #[test]
    fn non_positive_price_rejected() {
        let (p, mut pr, t) = make_inputs();
        pr.prices.insert("VTI".into(), DecimalStr(dec!(0)));
        let err = validate(&source(&p, &pr, &t)).unwrap_err();
        assert!(matches!(err, RebalanceError::NonPositivePrice { .. }));
    }

    #[test]
    fn negative_shares_rejected() {
        let (mut p, pr, t) = make_inputs();
        p.accounts
            .get_mut("roth")
            .unwrap()
            .positions
            .insert("VTI".into(), -1);
        let err = validate(&source(&p, &pr, &t)).unwrap_err();
        assert!(matches!(err, RebalanceError::NegativeShares { .. }));
    }

    #[test]
    fn weight_within_epsilon_accepted() {
        let (p, pr, mut t) = make_inputs();
        t.sleeves.get_mut("us_equity").unwrap().target_weight = dec!(0.59995);
        t.sleeves.get_mut("bonds").unwrap().target_weight = dec!(0.40005);
        validate(&source(&p, &pr, &t)).unwrap();
    }
}
