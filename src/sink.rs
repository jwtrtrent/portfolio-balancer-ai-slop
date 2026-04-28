use crate::errors::RebalanceError;
use crate::model::RebalanceOutput;

/// Write-only destination for a rebalance result. Implementations must be
/// `Send + Sync` so the engine can hand results to a sink from any thread.
pub trait OutputSink: Send + Sync {
    fn write(&self, output: &RebalanceOutput) -> Result<(), RebalanceError>;
}
