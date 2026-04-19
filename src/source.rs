use std::sync::Arc;

use rust_decimal::Decimal;

use crate::id::{AccountId, SecurityId, SleeveId};
use crate::lot::LotData;
use crate::registry::Registry;

/// ID-keyed view of a single account. Fields are shared cheaply via `Arc`.
#[derive(Clone, Debug)]
pub struct AccountData {
    pub id: AccountId,
    pub name: Arc<str>,
    pub kind: Option<Arc<str>>,
    pub cash: Decimal,
    /// Sorted by `SecurityId` so equal views compare stably.
    pub positions: Arc<[(SecurityId, i64)]>,
    /// All tax lots held in this account, in stable order. Lots for a given
    /// security are contiguous and sorted by acquired date then `LotId`.
    pub lots: Arc<[LotData]>,
}

/// ID-keyed view of a sleeve.
#[derive(Clone, Debug)]
pub struct SleeveData {
    pub id: SleeveId,
    pub name: Arc<str>,
    pub target_weight: Decimal,
    /// (security, sub-weight), sorted by `SecurityId`.
    pub holdings: Arc<[(SecurityId, Decimal)]>,
    /// Ordered list of preferred accounts (order matters for the allocator).
    pub preferred_accounts: Arc<[AccountId]>,
}

/// Read-only snapshot of the portfolio. Implementations must be `Send + Sync`
/// so they can be shared across threads via `Arc<dyn PortfolioSource>`.
pub trait PortfolioSource: Send + Sync {
    fn registry(&self) -> &dyn Registry;

    fn accounts(&self) -> &[AccountData];
    fn account(&self, id: AccountId) -> Option<&AccountData>;

    fn securities(&self) -> &[SecurityId];
    fn price(&self, id: SecurityId) -> Option<Decimal>;

    fn sleeves(&self) -> &[SleeveData];
    fn sleeve(&self, id: SleeveId) -> Option<&SleeveData>;

    /// Lots for `(account, security)` in stable order. Default implementation
    /// filters `account.lots`; backends with indexed storage can override.
    fn lots_for(&self, account: AccountId, security: SecurityId) -> Vec<&LotData> {
        match self.account(account) {
            Some(a) => a.lots.iter().filter(|l| l.security == security).collect(),
            None => Vec::new(),
        }
    }
}
