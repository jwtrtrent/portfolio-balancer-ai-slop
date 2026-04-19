use time::{Date, OffsetDateTime};

use crate::allocate::allocate;
use crate::errors::RebalanceError;
use crate::lot::LotSelector;
use crate::model::RebalanceOutput;
use crate::rebalance::build_output;
use crate::source::PortfolioSource;
use crate::validate::validate;

/// The top-level rebalance pipeline. Swapping engine implementations lets
/// downstream commits layer on features (CEL blocking, SQLite-native engine,
/// ...) without rewiring callers.
pub trait RebalanceEngine: Send + Sync {
    fn rebalance(&self, source: &dyn PortfolioSource) -> Result<RebalanceOutput, RebalanceError>;
}

/// Validate -> allocate -> build trades. Stateless, clone-free, thread-safe.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultEngine {
    /// Strategy used when lots must be consumed for a sell.
    pub lot_selector: LotSelector,
    /// Trade-date anchor for holding-period math. `None` means "use today
    /// from the system clock". Exposed so callers can run deterministic
    /// historical replays.
    pub as_of: Option<Date>,
}

impl DefaultEngine {
    pub fn new(lot_selector: LotSelector, as_of: Option<Date>) -> Self {
        Self {
            lot_selector,
            as_of,
        }
    }

    pub fn resolve_as_of(&self) -> Date {
        self.as_of
            .unwrap_or_else(|| OffsetDateTime::now_utc().date())
    }
}

impl RebalanceEngine for DefaultEngine {
    fn rebalance(&self, source: &dyn PortfolioSource) -> Result<RebalanceOutput, RebalanceError> {
        validate(source)?;
        let allocation = allocate(source)?;
        build_output(source, &allocation, self.lot_selector, self.resolve_as_of())
    }
}
