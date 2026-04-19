use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub type Ticker = String;
pub type AccountId = String;
pub type SleeveId = String;

/// `positions.json` — current holdings per account.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PositionsFile {
    pub accounts: BTreeMap<AccountId, Account>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Account {
    #[serde(default)]
    pub r#type: Option<String>,
    #[serde(with = "rust_decimal::serde::str")]
    pub cash: Decimal,
    #[serde(default)]
    pub positions: BTreeMap<Ticker, i64>,
}

/// `prices.json` — ticker → price.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct PricesFile {
    pub prices: BTreeMap<Ticker, DecimalStr>,
}

/// Decimal newtype that always serialises as a JSON string. Avoids floating
/// point sneaking in via numeric JSON literals.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct DecimalStr(#[serde(with = "rust_decimal::serde::str")] pub Decimal);

impl From<Decimal> for DecimalStr {
    fn from(d: Decimal) -> Self {
        DecimalStr(d)
    }
}

impl DecimalStr {
    pub fn into_inner(self) -> Decimal {
        self.0
    }
}

/// `targets.json` — sleeve definitions.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TargetsFile {
    pub sleeves: BTreeMap<SleeveId, Sleeve>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Sleeve {
    #[serde(with = "rust_decimal::serde::str")]
    pub target_weight: Decimal,
    pub holdings: BTreeMap<Ticker, DecimalStr>,
    #[serde(default)]
    pub preferred_accounts: Vec<AccountId>,
}

/// Output of a rebalance run.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RebalanceOutput {
    pub accounts: BTreeMap<AccountId, AccountResult>,
    pub summary: Summary,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AccountResult {
    #[serde(with = "rust_decimal::serde::str")]
    pub starting_cash: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub ending_cash: Decimal,
    pub positions: BTreeMap<Ticker, PositionResult>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PositionResult {
    pub current_shares: i64,
    pub target_shares: i64,
    pub trade_shares: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub trade_value: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Summary {
    #[serde(with = "rust_decimal::serde::str")]
    pub total_value: Decimal,
    pub sleeve_drift_bps: BTreeMap<SleeveId, i64>,
    pub max_drift_bps: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use rust_decimal_macros::dec;

    #[test]
    fn round_trip_positions_file() {
        let json = r#"{
            "accounts": {
                "roth_ira": { "type": "roth", "cash": "1500.00", "positions": { "VTI": 10 } }
            }
        }"#;
        let parsed: PositionsFile = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.accounts["roth_ira"].cash, dec!(1500.00));
        assert_eq!(parsed.accounts["roth_ira"].positions["VTI"], 10);
        let back = serde_json::to_string(&parsed).unwrap();
        let reparsed: PositionsFile = serde_json::from_str(&back).unwrap();
        assert_eq!(parsed, reparsed);
    }

    #[test]
    fn round_trip_prices_file() {
        let json = r#"{ "VTI": "250.00", "BND": "75.00" }"#;
        let parsed: PricesFile = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.prices["VTI"].0, dec!(250.00));
        let back = serde_json::to_string(&parsed).unwrap();
        let reparsed: PricesFile = serde_json::from_str(&back).unwrap();
        assert_eq!(parsed, reparsed);
    }

    #[test]
    fn round_trip_targets_file() {
        let json = r#"{
            "sleeves": {
                "bonds": {
                    "target_weight": "0.20",
                    "holdings": { "BND": "0.7", "BNDX": "0.3" },
                    "preferred_accounts": ["roth_ira", "traditional"]
                }
            }
        }"#;
        let parsed: TargetsFile = serde_json::from_str(json).unwrap();
        let bonds = &parsed.sleeves["bonds"];
        assert_eq!(bonds.target_weight, dec!(0.20));
        assert_eq!(bonds.holdings["BND"].0, dec!(0.7));
        assert_eq!(bonds.preferred_accounts, vec!["roth_ira", "traditional"]);
    }

    #[test]
    fn round_trip_output() {
        let mut accounts = BTreeMap::new();
        let mut positions = BTreeMap::new();
        positions.insert(
            "VTI".to_string(),
            PositionResult {
                current_shares: 10,
                target_shares: 0,
                trade_shares: -10,
                trade_value: dec!(-2500.00),
                price: dec!(250.00),
            },
        );
        accounts.insert(
            "roth_ira".to_string(),
            AccountResult {
                starting_cash: dec!(1500.00),
                ending_cash: dec!(12.34),
                positions,
            },
        );
        let mut sleeve_drift_bps = BTreeMap::new();
        sleeve_drift_bps.insert("us_equity".to_string(), 3);
        let out = RebalanceOutput {
            accounts,
            summary: Summary {
                total_value: dec!(100000.00),
                sleeve_drift_bps,
                max_drift_bps: 3,
            },
        };
        let s = serde_json::to_string(&out).unwrap();
        let back: RebalanceOutput = serde_json::from_str(&s).unwrap();
        assert_eq!(out, back);
    }
}
