use std::sync::Arc;

use crate::errors::RebalanceError;
use crate::sink::OutputSink;
use crate::source::PortfolioSource;

pub mod json;

pub use json::{JsonOutputSink, JsonStoreLoader};

/// Loads a portfolio source and returns a paired output sink. Backends
/// (JSON, SQLite, ...) implement this trait. Each call returns fresh
/// handles so callers can hold an `Arc<dyn PortfolioSource>` long enough to
/// run any engine they want.
pub trait StoreLoader: Send + Sync {
    fn load(&self) -> Result<LoadedStore, RebalanceError>;
}

/// Owned, thread-safe handles returned by a loader.
pub struct LoadedStore {
    pub source: Arc<dyn PortfolioSource>,
    pub sink: Box<dyn OutputSink>,
}
