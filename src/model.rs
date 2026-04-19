use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use time::Date;

pub type Ticker = String;

/// `positions.json` — current holdings per account.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PositionsFile {
    pub accounts: BTreeMap<String, Account>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Account {
    #[serde(default)]
    pub r#type: Option<String>,
    #[serde(with = "rust_decimal::serde::str")]
    pub cash: Decimal,
    #[serde(default)]
    pub positions: BTreeMap<Ticker, PositionEntry>,
}

/// One entry in an account's `positions` map. Accepts either a bare share
/// count (no lot information) or an object with an explicit tax-lot list.
///
/// The enum is `#[serde(untagged)]`, so the JSON stays backward compatible:
///
/// ```json
/// { "VTI": 10 }
/// { "VTI": { "lots": [{"quantity": 5, "cost_basis": "200.00", "acquired": "2022-01-15"}] } }
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum PositionEntry {
    Shares(i64),
    Lots(LotList),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct LotList {
    pub lots: Vec<LotEntry>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct LotEntry {
    pub quantity: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub cost_basis: Decimal,
    #[serde(with = "iso_date")]
    pub acquired: Date,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

/// ISO 8601 `YYYY-MM-DD` serde helper for [`time::Date`]. The default serde
/// impl on `Date` uses an opaque tuple format — not something callers want to
/// read or write in JSON.
mod iso_date {
    use serde::de::{self, Deserializer};
    use serde::ser::Serializer;
    use time::macros::format_description;
    use time::Date;

    const FMT: &[time::format_description::BorrowedFormatItem<'_>] =
        format_description!("[year]-[month]-[day]");

    pub fn serialize<S: Serializer>(d: &Date, s: S) -> Result<S::Ok, S::Error> {
        let formatted = d.format(FMT).map_err(serde::ser::Error::custom)?;
        s.serialize_str(&formatted)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Date, D::Error> {
        let s: String = serde::Deserialize::deserialize(d)?;
        Date::parse(&s, FMT).map_err(de::Error::custom)
    }
}

impl PositionEntry {
    /// Total share count represented by this entry.
    pub fn total_shares(&self) -> i64 {
        match self {
            PositionEntry::Shares(n) => *n,
            PositionEntry::Lots(list) => list.lots.iter().map(|l| l.quantity).sum(),
        }
    }
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
    pub sleeves: BTreeMap<String, Sleeve>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Sleeve {
    #[serde(with = "rust_decimal::serde::str")]
    pub target_weight: Decimal,
    pub holdings: BTreeMap<Ticker, DecimalStr>,
    #[serde(default)]
    pub preferred_accounts: Vec<String>,
}

/// Output of a rebalance run.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RebalanceOutput {
    pub accounts: BTreeMap<String, AccountResult>,
    pub summary: Summary,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AccountResult {
    #[serde(with = "rust_decimal::serde::str")]
    pub starting_cash: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub ending_cash: Decimal,
    pub positions: BTreeMap<Ticker, PositionResult>,
    #[serde(with = "rust_decimal::serde::str", default)]
    pub realized_gain: Decimal,
    #[serde(with = "rust_decimal::serde::str", default)]
    pub short_term_gain: Decimal,
    #[serde(with = "rust_decimal::serde::str", default)]
    pub long_term_gain: Decimal,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lots_sold: Vec<SaleLotResult>,
}

/// Per-lot slice of a sell produced by the engine. Mirrors
/// [`crate::lot::SaleAllocation`] but serialises cleanly.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SaleLotResult {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub lot_id: String,
    pub shares_sold: i64,
    #[serde(with = "iso_date")]
    pub acquired: Date,
    pub holding_days: i64,
    pub is_long_term: bool,
    #[serde(with = "rust_decimal::serde::str")]
    pub cost_basis: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub proceeds: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub realized_gain: Decimal,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Summary {
    #[serde(with = "rust_decimal::serde::str")]
    pub total_value: Decimal,
    pub sleeve_drift_bps: BTreeMap<String, i64>,
    pub max_drift_bps: i64,
    #[serde(with = "rust_decimal::serde::str", default)]
    pub total_realized_gain: Decimal,
    #[serde(with = "rust_decimal::serde::str", default)]
    pub total_short_term_gain: Decimal,
    #[serde(with = "rust_decimal::serde::str", default)]
    pub total_long_term_gain: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use rust_decimal_macros::dec;
    use time::macros::date;

    #[test]
    fn round_trip_positions_file_with_bare_shares() {
        let json = r#"{
            "accounts": {
                "roth_ira": { "type": "roth", "cash": "1500.00", "positions": { "VTI": 10 } }
            }
        }"#;
        let parsed: PositionsFile = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.accounts["roth_ira"].cash, dec!(1500.00));
        assert_eq!(
            parsed.accounts["roth_ira"].positions["VTI"],
            PositionEntry::Shares(10)
        );
        let back = serde_json::to_string(&parsed).unwrap();
        let reparsed: PositionsFile = serde_json::from_str(&back).unwrap();
        assert_eq!(parsed, reparsed);
    }

    #[test]
    fn round_trip_positions_file_with_lots() {
        let json = r#"{
            "accounts": {
                "taxable": {
                    "type": "taxable",
                    "cash": "0.00",
                    "positions": {
                        "VTI": { "lots": [
                            { "quantity": 5, "cost_basis": "200.00", "acquired": "2022-01-15", "id": "lot-a" },
                            { "quantity": 5, "cost_basis": "240.00", "acquired": "2023-06-01" }
                        ] }
                    }
                }
            }
        }"#;
        let parsed: PositionsFile = serde_json::from_str(json).unwrap();
        let entry = &parsed.accounts["taxable"].positions["VTI"];
        assert_eq!(entry.total_shares(), 10);
        match entry {
            PositionEntry::Lots(list) => {
                assert_eq!(list.lots.len(), 2);
                assert_eq!(list.lots[0].quantity, 5);
                assert_eq!(list.lots[0].cost_basis, dec!(200.00));
                assert_eq!(list.lots[0].acquired, date!(2022 - 01 - 15));
                assert_eq!(list.lots[0].id.as_deref(), Some("lot-a"));
                assert_eq!(list.lots[1].id, None);
            }
            _ => panic!("expected lots"),
        }
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
    fn round_trip_output_with_sale_details() {
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
                lots_sold: vec![SaleLotResult {
                    lot_id: "lot-a".into(),
                    shares_sold: 10,
                    acquired: date!(2022 - 01 - 15),
                    holding_days: 800,
                    is_long_term: true,
                    cost_basis: dec!(2000.00),
                    proceeds: dec!(2500.00),
                    realized_gain: dec!(500.00),
                }],
            },
        );
        accounts.insert(
            "roth_ira".to_string(),
            AccountResult {
                starting_cash: dec!(1500.00),
                ending_cash: dec!(4000.00),
                positions,
                realized_gain: dec!(500.00),
                short_term_gain: dec!(0.00),
                long_term_gain: dec!(500.00),
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
                total_realized_gain: dec!(500.00),
                total_short_term_gain: dec!(0.00),
                total_long_term_gain: dec!(500.00),
            },
        };
        let s = serde_json::to_string(&out).unwrap();
        let back: RebalanceOutput = serde_json::from_str(&s).unwrap();
        assert_eq!(out, back);
    }

    #[test]
    fn position_entry_total_shares_sums_lots() {
        let entry = PositionEntry::Lots(LotList {
            lots: vec![
                LotEntry {
                    quantity: 3,
                    cost_basis: dec!(1),
                    acquired: date!(2024 - 01 - 01),
                    id: None,
                },
                LotEntry {
                    quantity: 7,
                    cost_basis: dec!(1),
                    acquired: date!(2024 - 01 - 01),
                    id: None,
                },
            ],
        });
        assert_eq!(entry.total_shares(), 10);
    }
}
