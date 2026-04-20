//! SQLite-backed [`PortfolioSource`] / [`OutputSink`] implementations.
//!
//! Reads share an `r2d2` connection pool. The first call to a `PortfolioSource`
//! method warms a `OnceLock` snapshot from the database; subsequent calls
//! return references into that snapshot, matching the in-memory
//! implementation's zero-copy contract.

use std::path::PathBuf;
use std::sync::Arc;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Connection;

use crate::errors::RebalanceError;
use crate::sink::OutputSink;
use crate::source::PortfolioSource;
use crate::store::{LoadedStore, StoreLoader};

pub mod sink;
pub mod source;

pub use sink::SqliteOutputSink;
pub use source::{ingest_inputs, SqlitePortfolioSource};

/// The schema is applied once when a connection pool is opened. Idempotent —
/// every statement is `CREATE … IF NOT EXISTS`.
const SCHEMA_SQL: &str = include_str!("schema.sql");

/// Pairs a [`SqlitePortfolioSource`] with a [`SqliteOutputSink`] backed by the
/// same database file.
#[derive(Debug, Clone)]
pub struct SqliteStoreLoader {
    pub db_path: PathBuf,
    /// Optional human label persisted alongside each rebalance run.
    pub run_label: Option<String>,
}

impl SqliteStoreLoader {
    pub fn new(db_path: PathBuf) -> Self {
        Self {
            db_path,
            run_label: None,
        }
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.run_label = Some(label.into());
        self
    }
}

impl StoreLoader for SqliteStoreLoader {
    fn load(&self) -> Result<LoadedStore, RebalanceError> {
        let pool = open_pool(&self.db_path)?;
        let source: Arc<dyn PortfolioSource> = Arc::new(SqlitePortfolioSource::new(pool.clone()));
        let sink: Box<dyn OutputSink> = Box::new(SqliteOutputSink {
            pool,
            label: self.run_label.clone(),
        });
        Ok(LoadedStore { source, sink })
    }
}

/// Open a pool against `path` and ensure the schema exists.
pub fn open_pool(path: &std::path::Path) -> Result<Pool<SqliteConnectionManager>, RebalanceError> {
    let manager = SqliteConnectionManager::file(path)
        .with_init(|c: &mut Connection| c.execute_batch("PRAGMA foreign_keys = ON;"));
    let pool = Pool::builder().build(manager)?;
    {
        let conn = pool.get()?;
        conn.execute_batch(SCHEMA_SQL)?;
    }
    Ok(pool)
}

/// In-memory pool — used for tests. A single shared connection so all
/// callers see the same `:memory:` database.
#[cfg(test)]
pub(crate) fn open_memory_pool() -> Result<Pool<SqliteConnectionManager>, RebalanceError> {
    let manager = SqliteConnectionManager::memory()
        .with_init(|c: &mut Connection| c.execute_batch("PRAGMA foreign_keys = ON;"));
    let pool = Pool::builder().max_size(1).build(manager)?;
    {
        let conn = pool.get()?;
        conn.execute_batch(SCHEMA_SQL)?;
    }
    Ok(pool)
}
