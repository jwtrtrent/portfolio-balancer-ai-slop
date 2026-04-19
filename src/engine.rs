use crate::allocate::allocate;
use crate::errors::RebalanceError;
use crate::model::RebalanceOutput;
use crate::rebalance::build_output;
use crate::source::PortfolioSource;
use crate::validate::validate;

/// The top-level rebalance pipeline. Swapping engine implementations lets
/// downstream commits layer on features (lot-aware sells, CEL blocking, ...)
/// without rewiring callers.
pub trait RebalanceEngine: Send + Sync {
    fn rebalance(&self, source: &dyn PortfolioSource) -> Result<RebalanceOutput, RebalanceError>;
}

/// Validate -> allocate -> build trades. Stateless, clone-free, thread-safe.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultEngine;

impl RebalanceEngine for DefaultEngine {
    fn rebalance(&self, source: &dyn PortfolioSource) -> Result<RebalanceOutput, RebalanceError> {
        validate(source)?;
        let allocation = allocate(source)?;
        build_output(source, &allocation)
    }
}
