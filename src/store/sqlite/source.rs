use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rust_decimal::Decimal;
use time::macros::format_description;
use time::Date;

use crate::errors::RebalanceError;
use crate::id::{AccountId, LotId, SecurityId, SleeveId};
use crate::lot::LotData;
use crate::registry::{Registry, SharedRegistry};
use crate::source::{AccountData, PortfolioSource, SleeveData};

const ISO_DATE: &[time::format_description::BorrowedFormatItem<'_>] =
    format_description!("[year]-[month]-[day]");

/// SQLite-backed [`PortfolioSource`].
///
/// Holds the connection pool plus a lazily-initialised in-memory snapshot.
/// The snapshot is loaded the first time any `PortfolioSource` accessor is
/// called and reused thereafter, mirroring [`InMemoryPortfolio`]'s zero-copy
/// access pattern.
pub struct SqlitePortfolioSource {
    pool: Pool<SqliteConnectionManager>,
    cache: OnceLock<SourceCache>,
}

struct SourceCache {
    registry: Arc<SharedRegistry>,
    accounts: Vec<AccountData>,
    accounts_by_id: HashMap<AccountId, usize>,
    securities: Vec<SecurityId>,
    prices: HashMap<SecurityId, Decimal>,
    sleeves: Vec<SleeveData>,
    sleeves_by_id: HashMap<SleeveId, usize>,
}

impl SqlitePortfolioSource {
    pub fn new(pool: Pool<SqliteConnectionManager>) -> Self {
        Self {
            pool,
            cache: OnceLock::new(),
        }
    }

    fn cache(&self) -> &SourceCache {
        // OnceLock::get_or_init can't propagate Result, so we panic on a
        // load failure. Construction of a SqliteStoreLoader already validated
        // that the schema is reachable; a failure here means the database
        // disappeared mid-run, which is unrecoverable.
        self.cache
            .get_or_init(|| load_cache(&self.pool).expect("sqlite cache load"))
    }
}

impl PortfolioSource for SqlitePortfolioSource {
    fn registry(&self) -> &dyn Registry {
        &*self.cache().registry
    }

    fn accounts(&self) -> &[AccountData] {
        &self.cache().accounts
    }

    fn account(&self, id: AccountId) -> Option<&AccountData> {
        let c = self.cache();
        c.accounts_by_id.get(&id).map(|&i| &c.accounts[i])
    }

    fn securities(&self) -> &[SecurityId] {
        &self.cache().securities
    }

    fn price(&self, id: SecurityId) -> Option<Decimal> {
        self.cache().prices.get(&id).copied()
    }

    fn sleeves(&self) -> &[SleeveData] {
        &self.cache().sleeves
    }

    fn sleeve(&self, id: SleeveId) -> Option<&SleeveData> {
        let c = self.cache();
        c.sleeves_by_id.get(&id).map(|&i| &c.sleeves[i])
    }
}

fn parse_decimal(s: &str, field: &'static str) -> Result<Decimal, RebalanceError> {
    Decimal::from_str(s)
        .map_err(|e| RebalanceError::SqliteStore(format!("invalid {field} `{s}`: {e}")))
}

fn parse_date(s: &str, field: &'static str) -> Result<Date, RebalanceError> {
    Date::parse(s, ISO_DATE)
        .map_err(|e| RebalanceError::SqliteStore(format!("invalid {field} `{s}`: {e}")))
}

fn load_cache(pool: &Pool<SqliteConnectionManager>) -> Result<SourceCache, RebalanceError> {
    let conn = pool.get()?;
    let registry = Arc::new(SharedRegistry::new());

    // Securities + prices.
    let mut prices: HashMap<SecurityId, Decimal> = HashMap::new();
    {
        let mut stmt = conn.prepare("SELECT ticker, price FROM securities ORDER BY ticker")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (ticker, price) = row?;
            let id = registry.intern_security(&ticker);
            prices.insert(id, parse_decimal(&price, "price")?);
        }
    }
    let mut securities: Vec<SecurityId> = prices.keys().copied().collect();
    securities.sort();

    // Accounts.
    let raw_accounts: Vec<(String, Option<String>, String)> = {
        let mut stmt = conn.prepare("SELECT name, type, cash FROM accounts ORDER BY name")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };

    let mut accounts: Vec<AccountData> = Vec::with_capacity(raw_accounts.len());
    let mut accounts_by_id: HashMap<AccountId, usize> = HashMap::with_capacity(raw_accounts.len());
    let mut next_lot: u32 = 0;

    for (name, kind, cash) in raw_accounts {
        let aid = registry.intern_account(&name);
        let cash = parse_decimal(&cash, "cash")?;

        // Positions for this account.
        let mut positions: Vec<(SecurityId, i64)> = {
            let mut stmt = conn.prepare(
                "SELECT ticker, shares FROM positions WHERE account = ?1 ORDER BY ticker",
            )?;
            let rows = stmt.query_map([&name], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?;
            let mut out = Vec::new();
            for row in rows {
                let (ticker, shares) = row?;
                let sid = registry.intern_security(&ticker);
                out.push((sid, shares));
            }
            out
        };
        positions.sort_by_key(|(s, _)| *s);

        // Lots for this account.
        let mut lots: Vec<LotData> = {
            let mut stmt = conn.prepare(
                "SELECT ticker, external_id, quantity, cost_basis, acquired \
                 FROM lots WHERE account = ?1 ORDER BY ticker, acquired, seq",
            )?;
            let rows = stmt.query_map([&name], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                let (ticker, external_id, quantity, cost_basis, acquired) = row?;
                let sid = registry.intern_security(&ticker);
                let id = LotId(next_lot);
                next_lot += 1;
                out.push(LotData {
                    id,
                    external_id: Arc::<str>::from(external_id),
                    account: aid,
                    security: sid,
                    quantity,
                    cost_basis_per_share: parse_decimal(&cost_basis, "cost_basis")?,
                    acquired: parse_date(&acquired, "acquired")?,
                });
            }
            out
        };
        lots.sort_by(|a, b| {
            a.security
                .cmp(&b.security)
                .then_with(|| a.acquired.cmp(&b.acquired))
                .then_with(|| a.id.cmp(&b.id))
        });

        let name_arc = registry.account_name(aid).expect("just interned");
        let kind_arc = kind.as_deref().map(Arc::<str>::from);
        let idx = accounts.len();
        accounts.push(AccountData {
            id: aid,
            name: name_arc,
            kind: kind_arc,
            cash,
            positions: Arc::<[(SecurityId, i64)]>::from(positions),
            lots: Arc::<[LotData]>::from(lots),
        });
        accounts_by_id.insert(aid, idx);
    }

    // Sleeves.
    let raw_sleeves: Vec<(String, String)> = {
        let mut stmt = conn.prepare("SELECT name, target_weight FROM sleeves ORDER BY name")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let mut sleeves: Vec<SleeveData> = Vec::with_capacity(raw_sleeves.len());
    let mut sleeves_by_id: HashMap<SleeveId, usize> = HashMap::with_capacity(raw_sleeves.len());
    for (name, target_weight) in raw_sleeves {
        let id = registry.intern_sleeve(&name);
        let mut holdings: Vec<(SecurityId, Decimal)> = {
            let mut stmt = conn.prepare(
                "SELECT ticker, weight FROM sleeve_holdings WHERE sleeve = ?1 ORDER BY ticker",
            )?;
            let rows = stmt.query_map([&name], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            let mut out = Vec::new();
            for row in rows {
                let (ticker, weight) = row?;
                let sid = registry.intern_security(&ticker);
                out.push((sid, parse_decimal(&weight, "weight")?));
            }
            out
        };
        holdings.sort_by_key(|(s, _)| *s);
        let preferred: Vec<AccountId> = {
            let mut stmt = conn.prepare(
                "SELECT account FROM sleeve_preferred_accounts \
                 WHERE sleeve = ?1 ORDER BY position",
            )?;
            let rows = stmt.query_map([&name], |row| row.get::<_, String>(0))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(registry.intern_account(&row?));
            }
            out
        };
        let name_arc = registry.sleeve_name(id).expect("just interned");
        let idx = sleeves.len();
        sleeves.push(SleeveData {
            id,
            name: name_arc,
            target_weight: parse_decimal(&target_weight, "target_weight")?,
            holdings: Arc::<[(SecurityId, Decimal)]>::from(holdings),
            preferred_accounts: Arc::<[AccountId]>::from(preferred),
        });
        sleeves_by_id.insert(id, idx);
    }

    Ok(SourceCache {
        registry,
        accounts,
        accounts_by_id,
        securities,
        prices,
        sleeves,
        sleeves_by_id,
    })
}

/// Populate the schema from in-memory DTOs. Used by tests and by the CLI's
/// `--ingest-json` path so users can promote a JSON snapshot into a SQLite
/// database without writing SQL by hand.
pub fn ingest_inputs(
    pool: &Pool<SqliteConnectionManager>,
    positions: &crate::model::PositionsFile,
    prices: &crate::model::PricesFile,
    targets: &crate::model::TargetsFile,
) -> Result<(), RebalanceError> {
    let mut conn = pool.get()?;
    let tx = conn.transaction()?;

    // Wipe any prior portfolio rows so re-ingesting is idempotent. Cascades
    // take care of positions/lots/sleeve children.
    tx.execute("DELETE FROM sleeve_preferred_accounts", [])?;
    tx.execute("DELETE FROM sleeve_holdings", [])?;
    tx.execute("DELETE FROM sleeves", [])?;
    tx.execute("DELETE FROM lots", [])?;
    tx.execute("DELETE FROM positions", [])?;
    tx.execute("DELETE FROM accounts", [])?;
    tx.execute("DELETE FROM securities", [])?;

    for (ticker, price) in &prices.prices {
        tx.execute(
            "INSERT INTO securities (ticker, price) VALUES (?1, ?2)",
            (ticker, price.0.to_string()),
        )?;
    }

    for (account_name, account) in &positions.accounts {
        tx.execute(
            "INSERT INTO accounts (name, type, cash) VALUES (?1, ?2, ?3)",
            (
                account_name,
                account.r#type.as_deref(),
                account.cash.to_string(),
            ),
        )?;
        for (ticker, entry) in &account.positions {
            // Auto-create the security row if a position references a ticker
            // that isn't priced yet — keeps round-tripping symmetric with
            // `InMemoryPortfolio::from_dtos`, which interns silently.
            tx.execute(
                "INSERT OR IGNORE INTO securities (ticker, price) VALUES (?1, '0')",
                [ticker],
            )?;
            tx.execute(
                "INSERT INTO positions (account, ticker, shares) VALUES (?1, ?2, ?3)",
                (account_name, ticker, entry.total_shares()),
            )?;
            if let crate::model::PositionEntry::Lots(list) = entry {
                for (seq, lot) in list.lots.iter().enumerate() {
                    let acquired = lot
                        .acquired
                        .format(ISO_DATE)
                        .map_err(|e| RebalanceError::SqliteStore(format!("format date: {e}")))?;
                    tx.execute(
                        "INSERT INTO lots \
                         (account, ticker, external_id, quantity, cost_basis, acquired, seq) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        (
                            account_name,
                            ticker,
                            lot.id.as_deref().unwrap_or(""),
                            lot.quantity,
                            lot.cost_basis.to_string(),
                            acquired,
                            seq as i64,
                        ),
                    )?;
                }
            }
        }
    }

    for (sleeve_name, sleeve) in &targets.sleeves {
        tx.execute(
            "INSERT INTO sleeves (name, target_weight) VALUES (?1, ?2)",
            (sleeve_name, sleeve.target_weight.to_string()),
        )?;
        for (ticker, weight) in &sleeve.holdings {
            tx.execute(
                "INSERT OR IGNORE INTO securities (ticker, price) VALUES (?1, '0')",
                [ticker],
            )?;
            tx.execute(
                "INSERT INTO sleeve_holdings (sleeve, ticker, weight) VALUES (?1, ?2, ?3)",
                (sleeve_name, ticker, weight.0.to_string()),
            )?;
        }
        for (position, account_name) in sleeve.preferred_accounts.iter().enumerate() {
            tx.execute(
                "INSERT INTO sleeve_preferred_accounts (sleeve, position, account) \
                 VALUES (?1, ?2, ?3)",
                (sleeve_name, position as i64, account_name),
            )?;
        }
    }

    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Account, DecimalStr, LotEntry, LotList, PositionEntry, PositionsFile, PricesFile, Sleeve,
        TargetsFile,
    };
    use crate::store::sqlite::open_memory_pool;
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;
    use time::macros::date;

    fn sample() -> (PositionsFile, PricesFile, TargetsFile) {
        let positions = PositionsFile {
            accounts: BTreeMap::from([(
                "taxable".into(),
                Account {
                    r#type: Some("taxable".into()),
                    cash: dec!(750),
                    positions: BTreeMap::from([
                        (
                            "VTI".into(),
                            PositionEntry::Lots(LotList {
                                lots: vec![
                                    LotEntry {
                                        quantity: 5,
                                        cost_basis: dec!(180),
                                        acquired: date!(2022 - 01 - 15),
                                        id: Some("vti-2022".into()),
                                    },
                                    LotEntry {
                                        quantity: 5,
                                        cost_basis: dec!(200),
                                        acquired: date!(2023 - 06 - 01),
                                        id: Some("vti-2023".into()),
                                    },
                                ],
                            }),
                        ),
                        ("VXUS".into(), PositionEntry::Shares(25)),
                    ]),
                },
            )]),
        };
        let prices = PricesFile {
            prices: BTreeMap::from([
                ("VTI".into(), DecimalStr(dec!(250))),
                ("VXUS".into(), DecimalStr(dec!(60))),
            ]),
        };
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "us".into(),
                Sleeve {
                    target_weight: dec!(1.0),
                    holdings: BTreeMap::from([("VTI".into(), DecimalStr(dec!(1.0)))]),
                    preferred_accounts: vec!["taxable".into()],
                },
            )]),
        };
        (positions, prices, targets)
    }

    #[test]
    fn ingest_then_load_round_trips() {
        let pool = open_memory_pool().unwrap();
        let (p, pr, t) = sample();
        ingest_inputs(&pool, &p, &pr, &t).unwrap();
        let source = SqlitePortfolioSource::new(pool);

        let tax = source.registry().account_id("taxable").unwrap();
        let acct = source.account(tax).unwrap();
        assert_eq!(acct.cash, dec!(750));
        assert_eq!(acct.kind.as_deref(), Some("taxable"));

        let vti = source.registry().security_id("VTI").unwrap();
        assert_eq!(source.price(vti), Some(dec!(250)));
        assert_eq!(
            acct.positions.iter().find(|(s, _)| *s == vti).unwrap().1,
            10
        );

        let lots = source.lots_for(tax, vti);
        assert_eq!(lots.len(), 2);
        // Ordered by acquired date, then by seq.
        assert_eq!(&*lots[0].external_id, "vti-2022");
        assert_eq!(lots[0].cost_basis_per_share, dec!(180));
        assert_eq!(&*lots[1].external_id, "vti-2023");

        let sid = source.registry().sleeve_id("us").unwrap();
        let sleeve = source.sleeve(sid).unwrap();
        assert_eq!(sleeve.target_weight, dec!(1.0));
        assert_eq!(&*sleeve.preferred_accounts, &[tax]);
    }

    #[test]
    fn ingest_is_idempotent() {
        let pool = open_memory_pool().unwrap();
        let (p, pr, t) = sample();
        ingest_inputs(&pool, &p, &pr, &t).unwrap();
        ingest_inputs(&pool, &p, &pr, &t).unwrap();
        let source = SqlitePortfolioSource::new(pool);
        assert_eq!(source.accounts().len(), 1);
        assert_eq!(source.sleeves().len(), 1);
    }

    #[test]
    fn securities_referenced_only_in_targets_are_auto_created() {
        let mut prices = PricesFile {
            prices: BTreeMap::from([("VTI".into(), DecimalStr(dec!(250)))]),
        };
        // Drop BND from prices but reference it in a sleeve.
        prices.prices.remove("BND");
        let positions = PositionsFile {
            accounts: BTreeMap::from([(
                "roth".into(),
                Account {
                    r#type: Some("roth".into()),
                    cash: dec!(1000),
                    positions: BTreeMap::new(),
                },
            )]),
        };
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "bonds".into(),
                Sleeve {
                    target_weight: dec!(1.0),
                    holdings: BTreeMap::from([("BND".into(), DecimalStr(dec!(1.0)))]),
                    preferred_accounts: vec!["roth".into()],
                },
            )]),
        };
        let pool = open_memory_pool().unwrap();
        ingest_inputs(&pool, &positions, &prices, &targets).unwrap();
        let source = SqlitePortfolioSource::new(pool);
        let bnd = source.registry().security_id("BND").unwrap();
        // Auto-created with placeholder price 0 — validate() will catch this
        // downstream, but the loader itself must not blow up.
        assert_eq!(source.price(bnd), Some(dec!(0)));
    }

    #[test]
    fn cache_initialises_lazily() {
        let pool = open_memory_pool().unwrap();
        let (p, pr, t) = sample();
        ingest_inputs(&pool, &p, &pr, &t).unwrap();
        let source = SqlitePortfolioSource::new(pool);
        // Cache hasn't been touched yet.
        assert!(source.cache.get().is_none());
        let _ = source.accounts();
        assert!(source.cache.get().is_some());
    }
}
