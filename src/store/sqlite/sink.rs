use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use time::macros::format_description;
use time::OffsetDateTime;

use crate::errors::RebalanceError;
use crate::model::RebalanceOutput;
use crate::sink::OutputSink;

const ISO_DATETIME: &[time::format_description::BorrowedFormatItem<'_>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]Z");

/// Persists a [`RebalanceOutput`] into the SQLite schema. Each `write` opens a
/// new run row plus the per-account/trade/sale/sleeve children in a single
/// transaction.
#[derive(Debug, Clone)]
pub struct SqliteOutputSink {
    pub pool: Pool<SqliteConnectionManager>,
    /// Optional human label persisted on the run row.
    pub label: Option<String>,
}

impl OutputSink for SqliteOutputSink {
    fn write(&self, output: &RebalanceOutput) -> Result<(), RebalanceError> {
        let mut conn = self.pool.get()?;
        let tx = conn.transaction()?;

        let now = OffsetDateTime::now_utc()
            .format(ISO_DATETIME)
            .map_err(|e| RebalanceError::SqliteStore(format!("format timestamp: {e}")))?;

        tx.execute(
            "INSERT INTO rebalance_runs \
             (label, created_at, total_value, max_drift_bps, \
              total_realized_gain, total_short_term_gain, total_long_term_gain) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            (
                self.label.as_deref(),
                &now,
                output.summary.total_value.to_string(),
                output.summary.max_drift_bps,
                output.summary.total_realized_gain.to_string(),
                output.summary.total_short_term_gain.to_string(),
                output.summary.total_long_term_gain.to_string(),
            ),
        )?;
        let run_id = tx.last_insert_rowid();

        for (account_name, acct) in &output.accounts {
            tx.execute(
                "INSERT INTO rebalance_account_results \
                 (run_id, account, starting_cash, ending_cash, \
                  realized_gain, short_term_gain, long_term_gain) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                (
                    run_id,
                    account_name,
                    acct.starting_cash.to_string(),
                    acct.ending_cash.to_string(),
                    acct.realized_gain.to_string(),
                    acct.short_term_gain.to_string(),
                    acct.long_term_gain.to_string(),
                ),
            )?;
            for (ticker, pos) in &acct.positions {
                tx.execute(
                    "INSERT INTO rebalance_trades \
                     (run_id, account, ticker, current_shares, target_shares, \
                      trade_shares, trade_value, price) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    (
                        run_id,
                        account_name,
                        ticker,
                        pos.current_shares,
                        pos.target_shares,
                        pos.trade_shares,
                        pos.trade_value.to_string(),
                        pos.price.to_string(),
                    ),
                )?;
                for (seq, lot) in pos.lots_sold.iter().enumerate() {
                    let acquired = lot
                        .acquired
                        .format(format_description!("[year]-[month]-[day]"))
                        .map_err(|e| {
                            RebalanceError::SqliteStore(format!("format acquired: {e}"))
                        })?;
                    tx.execute(
                        "INSERT INTO rebalance_sales \
                         (run_id, account, ticker, seq, lot_id, shares_sold, acquired, \
                          holding_days, is_long_term, cost_basis, proceeds, realized_gain) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                        (
                            run_id,
                            account_name,
                            ticker,
                            seq as i64,
                            &lot.lot_id,
                            lot.shares_sold,
                            &acquired,
                            lot.holding_days,
                            lot.is_long_term as i64,
                            lot.cost_basis.to_string(),
                            lot.proceeds.to_string(),
                            lot.realized_gain.to_string(),
                        ),
                    )?;
                }
            }
        }

        for (sleeve, drift) in &output.summary.sleeve_drift_bps {
            tx.execute(
                "INSERT INTO rebalance_sleeve_drift (run_id, sleeve, drift_bps) \
                 VALUES (?1, ?2, ?3)",
                (run_id, sleeve, *drift),
            )?;
        }

        for (seq, v) in output.summary.policy_violations.iter().enumerate() {
            tx.execute(
                "INSERT INTO rebalance_violations \
                 (run_id, seq, policy, action, account, ticker, message) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                (
                    run_id,
                    seq as i64,
                    &v.policy,
                    &v.action,
                    &v.account,
                    v.ticker.as_deref(),
                    &v.message,
                ),
            )?;
        }

        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{DefaultEngine, RebalanceEngine};
    use crate::lot::LotSelector;
    use crate::model::{
        Account, DecimalStr, LotEntry, LotList, PositionEntry, PositionsFile, PricesFile, Sleeve,
        TargetsFile,
    };
    use crate::store::sqlite::open_memory_pool;
    use crate::store::sqlite::source::{ingest_inputs, SqlitePortfolioSource};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;
    use time::macros::date;

    fn sample() -> (PositionsFile, PricesFile, TargetsFile) {
        let positions = PositionsFile {
            accounts: BTreeMap::from([
                (
                    "taxable".into(),
                    Account {
                        r#type: Some("taxable".into()),
                        cash: dec!(750),
                        positions: BTreeMap::from([(
                            "VTI".into(),
                            PositionEntry::Lots(LotList {
                                lots: vec![LotEntry {
                                    quantity: 10,
                                    cost_basis: dec!(180),
                                    acquired: date!(2022 - 01 - 15),
                                    id: Some("vti-2022".into()),
                                }],
                            }),
                        )]),
                    },
                ),
                (
                    "roth".into(),
                    Account {
                        r#type: Some("roth".into()),
                        cash: dec!(2000),
                        positions: BTreeMap::new(),
                    },
                ),
            ]),
        };
        let prices = PricesFile {
            prices: BTreeMap::from([
                ("VTI".into(), DecimalStr(dec!(250))),
                ("BND".into(), DecimalStr(dec!(75))),
            ]),
        };
        let targets = TargetsFile {
            sleeves: BTreeMap::from([
                (
                    "us".into(),
                    Sleeve {
                        target_weight: dec!(0.7),
                        holdings: BTreeMap::from([("VTI".into(), DecimalStr(dec!(1.0)))]),
                        preferred_accounts: vec!["taxable".into(), "roth".into()],
                    },
                ),
                (
                    "bonds".into(),
                    Sleeve {
                        target_weight: dec!(0.3),
                        holdings: BTreeMap::from([("BND".into(), DecimalStr(dec!(1.0)))]),
                        preferred_accounts: vec!["roth".into()],
                    },
                ),
            ]),
        };
        (positions, prices, targets)
    }

    #[test]
    fn write_persists_run_with_trades_sales_and_drift() {
        let pool = open_memory_pool().unwrap();
        let (p, pr, t) = sample();
        ingest_inputs(&pool, &p, &pr, &t).unwrap();
        let source = SqlitePortfolioSource::new(pool.clone());
        let engine = DefaultEngine::new(LotSelector::Fifo, Some(date!(2025 - 01 - 01)));
        let out = engine.rebalance(&source).unwrap();
        let sink = SqliteOutputSink {
            pool: pool.clone(),
            label: Some("smoke".into()),
        };
        sink.write(&out).unwrap();

        let conn = pool.get().unwrap();
        let run_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM rebalance_runs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(run_count, 1);
        let label: Option<String> = conn
            .query_row("SELECT label FROM rebalance_runs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(label.as_deref(), Some("smoke"));

        // One trade row per (account, ticker) emitted by the engine.
        let trade_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM rebalance_trades", [], |r| r.get(0))
            .unwrap();
        let expected_trades: i64 = out
            .accounts
            .values()
            .map(|a| a.positions.len() as i64)
            .sum();
        assert_eq!(trade_count, expected_trades);

        // Sales rows match per-lot detail.
        let sale_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM rebalance_sales", [], |r| r.get(0))
            .unwrap();
        let expected_sales: i64 = out
            .accounts
            .values()
            .flat_map(|a| a.positions.values())
            .map(|p| p.lots_sold.len() as i64)
            .sum();
        assert_eq!(sale_count, expected_sales);

        // Drift rows match the summary map.
        let drift_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM rebalance_sleeve_drift", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(drift_count, out.summary.sleeve_drift_bps.len() as i64);
    }

    #[test]
    fn two_writes_create_two_runs() {
        let pool = open_memory_pool().unwrap();
        let (p, pr, t) = sample();
        ingest_inputs(&pool, &p, &pr, &t).unwrap();
        let source = SqlitePortfolioSource::new(pool.clone());
        let engine = DefaultEngine::new(LotSelector::Fifo, Some(date!(2025 - 01 - 01)));
        let out = engine.rebalance(&source).unwrap();
        let sink = SqliteOutputSink {
            pool: pool.clone(),
            label: None,
        };
        sink.write(&out).unwrap();
        sink.write(&out).unwrap();
        let conn = pool.get().unwrap();
        let run_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM rebalance_runs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(run_count, 2);
    }

    #[test]
    fn totals_persisted_match_summary() {
        let pool = open_memory_pool().unwrap();
        let (p, pr, t) = sample();
        ingest_inputs(&pool, &p, &pr, &t).unwrap();
        let source = SqlitePortfolioSource::new(pool.clone());
        let engine = DefaultEngine::new(LotSelector::Fifo, Some(date!(2025 - 01 - 01)));
        let out = engine.rebalance(&source).unwrap();
        let sink = SqliteOutputSink {
            pool: pool.clone(),
            label: None,
        };
        sink.write(&out).unwrap();
        let conn = pool.get().unwrap();
        let total_value_str: String = conn
            .query_row("SELECT total_value FROM rebalance_runs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(total_value_str, out.summary.total_value.to_string());
        let total_realized_str: String = conn
            .query_row("SELECT total_realized_gain FROM rebalance_runs", [], |r| {
                r.get(0)
            })
            .unwrap();
        let parsed: Decimal = total_realized_str.parse().unwrap();
        assert_eq!(parsed, out.summary.total_realized_gain);
    }
}
