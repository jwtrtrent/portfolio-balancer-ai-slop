use std::collections::BTreeSet;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::errors::RebalanceError;
use crate::model::{PositionsFile, PricesFile, TargetsFile};

/// Tolerance for "weights sum to 1.0" checks. Allows users to write
/// `0.333`, `0.333`, `0.334` etc. without rejection.
const WEIGHT_EPSILON: Decimal = dec!(0.0001);

pub fn validate(
    positions: &PositionsFile,
    prices: &PricesFile,
    targets: &TargetsFile,
) -> Result<(), RebalanceError> {
    validate_prices(prices)?;
    validate_positions(positions, prices)?;
    validate_targets(targets, positions, prices)?;
    Ok(())
}

fn validate_prices(prices: &PricesFile) -> Result<(), RebalanceError> {
    for (ticker, price) in &prices.prices {
        if price.0 <= Decimal::ZERO {
            return Err(RebalanceError::NonPositivePrice {
                ticker: ticker.clone(),
                price: price.0.to_string(),
            });
        }
    }
    Ok(())
}

fn validate_positions(
    positions: &PositionsFile,
    prices: &PricesFile,
) -> Result<(), RebalanceError> {
    for (account_id, account) in &positions.accounts {
        if account.cash < Decimal::ZERO {
            return Err(RebalanceError::NegativeCash {
                account: account_id.clone(),
                cash: account.cash.to_string(),
            });
        }
        for (ticker, shares) in &account.positions {
            if *shares < 0 {
                return Err(RebalanceError::NegativeShares {
                    account: account_id.clone(),
                    ticker: ticker.clone(),
                    shares: *shares,
                });
            }
            if !prices.prices.contains_key(ticker) {
                return Err(RebalanceError::MissingPrice {
                    ticker: ticker.clone(),
                });
            }
        }
    }
    Ok(())
}

fn validate_targets(
    targets: &TargetsFile,
    positions: &PositionsFile,
    prices: &PricesFile,
) -> Result<(), RebalanceError> {
    let known_accounts: BTreeSet<&str> = positions.accounts.keys().map(|s| s.as_str()).collect();

    let mut total_weight = Decimal::ZERO;
    for (sleeve_id, sleeve) in &targets.sleeves {
        if sleeve.target_weight <= Decimal::ZERO {
            return Err(RebalanceError::NonPositiveTargetWeight {
                sleeve: sleeve_id.clone(),
                weight: sleeve.target_weight.to_string(),
            });
        }
        total_weight += sleeve.target_weight;

        let mut sub_weight_sum = Decimal::ZERO;
        for (ticker, sub_weight) in &sleeve.holdings {
            if sub_weight.0 <= Decimal::ZERO {
                return Err(RebalanceError::NonPositiveSubWeight {
                    sleeve: sleeve_id.clone(),
                    ticker: ticker.clone(),
                    weight: sub_weight.0.to_string(),
                });
            }
            sub_weight_sum += sub_weight.0;
            if !prices.prices.contains_key(ticker) {
                return Err(RebalanceError::MissingPrice {
                    ticker: ticker.clone(),
                });
            }
        }
        if (sub_weight_sum - Decimal::ONE).abs() > WEIGHT_EPSILON {
            return Err(RebalanceError::SleeveSubWeightsSum {
                sleeve: sleeve_id.clone(),
                actual: sub_weight_sum.to_string(),
            });
        }

        for account in &sleeve.preferred_accounts {
            if !known_accounts.contains(account.as_str()) {
                return Err(RebalanceError::UnknownPreferredAccount {
                    sleeve: sleeve_id.clone(),
                    account: account.clone(),
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
    use crate::model::{Account, DecimalStr, Sleeve};
    use std::collections::BTreeMap;

    fn make_inputs() -> (PositionsFile, PricesFile, TargetsFile) {
        let mut accounts = BTreeMap::new();
        accounts.insert(
            "roth".to_string(),
            Account {
                r#type: Some("roth".into()),
                cash: dec!(1000),
                positions: BTreeMap::from([("VTI".to_string(), 10)]),
            },
        );
        accounts.insert(
            "taxable".to_string(),
            Account {
                r#type: Some("taxable".into()),
                cash: dec!(500),
                positions: BTreeMap::new(),
            },
        );
        let positions = PositionsFile { accounts };

        let prices = PricesFile {
            prices: BTreeMap::from([
                ("VTI".to_string(), DecimalStr(dec!(250))),
                ("BND".to_string(), DecimalStr(dec!(75))),
            ]),
        };

        let mut sleeves = BTreeMap::new();
        sleeves.insert(
            "us_equity".to_string(),
            Sleeve {
                target_weight: dec!(0.6),
                holdings: BTreeMap::from([("VTI".to_string(), DecimalStr(dec!(1.0)))]),
                preferred_accounts: vec!["taxable".to_string()],
            },
        );
        sleeves.insert(
            "bonds".to_string(),
            Sleeve {
                target_weight: dec!(0.4),
                holdings: BTreeMap::from([("BND".to_string(), DecimalStr(dec!(1.0)))]),
                preferred_accounts: vec!["roth".to_string()],
            },
        );
        let targets = TargetsFile { sleeves };

        (positions, prices, targets)
    }

    #[test]
    fn valid_inputs_pass() {
        let (p, pr, t) = make_inputs();
        validate(&p, &pr, &t).unwrap();
    }

    #[test]
    fn target_weights_must_sum_to_one() {
        let (p, pr, mut t) = make_inputs();
        t.sleeves.get_mut("bonds").unwrap().target_weight = dec!(0.3);
        let err = validate(&p, &pr, &t).unwrap_err();
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
        let err = validate(&p, &pr, &t).unwrap_err();
        assert!(matches!(err, RebalanceError::SleeveSubWeightsSum { .. }));
    }

    #[test]
    fn missing_price_for_sleeve_ticker() {
        let (p, mut pr, t) = make_inputs();
        pr.prices.remove("BND");
        let err = validate(&p, &pr, &t).unwrap_err();
        assert!(matches!(err, RebalanceError::MissingPrice { .. }));
    }

    #[test]
    fn missing_price_for_held_ticker() {
        let (mut p, pr, mut t) = make_inputs();
        // Put a held position with no price; sleeves untouched but valid.
        p.accounts
            .get_mut("taxable")
            .unwrap()
            .positions
            .insert("AAPL".to_string(), 5);
        // Simplify: drop sleeves to skip sleeve checks, keep weights at 1.
        t.sleeves.clear();
        t.sleeves.insert(
            "us_equity".to_string(),
            Sleeve {
                target_weight: dec!(1.0),
                holdings: BTreeMap::from([("VTI".to_string(), DecimalStr(dec!(1.0)))]),
                preferred_accounts: vec!["taxable".to_string()],
            },
        );
        let err = validate(&p, &pr, &t).unwrap_err();
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
        let err = validate(&p, &pr, &t).unwrap_err();
        assert!(matches!(
            err,
            RebalanceError::UnknownPreferredAccount { .. }
        ));
    }

    #[test]
    fn negative_cash_rejected() {
        let (mut p, pr, t) = make_inputs();
        p.accounts.get_mut("roth").unwrap().cash = dec!(-1);
        let err = validate(&p, &pr, &t).unwrap_err();
        assert!(matches!(err, RebalanceError::NegativeCash { .. }));
    }

    #[test]
    fn non_positive_price_rejected() {
        let (p, mut pr, t) = make_inputs();
        pr.prices.insert("VTI".into(), DecimalStr(dec!(0)));
        let err = validate(&p, &pr, &t).unwrap_err();
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
        let err = validate(&p, &pr, &t).unwrap_err();
        assert!(matches!(err, RebalanceError::NegativeShares { .. }));
    }

    #[test]
    fn weight_within_epsilon_accepted() {
        let (p, pr, mut t) = make_inputs();
        t.sleeves.get_mut("us_equity").unwrap().target_weight = dec!(0.59995);
        t.sleeves.get_mut("bonds").unwrap().target_weight = dec!(0.40005);
        validate(&p, &pr, &t).unwrap();
    }
}
