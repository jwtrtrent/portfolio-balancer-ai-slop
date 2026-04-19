use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use rust_decimal::Decimal;
use time::{Date, OffsetDateTime};

use crate::allocate::{allocate_with_exclusions, Allocation};
use crate::errors::RebalanceError;
use crate::id::{AccountId, SecurityId};
use crate::lot::LotSelector;
use crate::model::{PolicyViolation, PositionResult, RebalanceOutput};
use crate::policy::{
    to_f64, AccountContext, PolicyAction, PolicySet, PositionContext, SaleContext, SummaryContext,
    TradeContext, TradeVerdict,
};
use crate::rebalance::build_output;
use crate::source::PortfolioSource;
use crate::validate::validate;

/// Pair that uniquely identifies a proposed trade (for exclusion tracking).
type TradeKey = (AccountId, SecurityId);

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

    fn rebalance_with_exclusions(
        &self,
        source: &dyn PortfolioSource,
        excluded: &HashSet<TradeKey>,
    ) -> Result<(RebalanceOutput, Allocation), RebalanceError> {
        let allocation = allocate_with_exclusions(source, excluded)?;
        let output = build_output(source, &allocation, self.lot_selector, self.resolve_as_of())?;
        Ok((output, allocation))
    }
}

impl RebalanceEngine for DefaultEngine {
    fn rebalance(&self, source: &dyn PortfolioSource) -> Result<RebalanceOutput, RebalanceError> {
        validate(source)?;
        let (output, _) = self.rebalance_with_exclusions(source, &HashSet::new())?;
        Ok(output)
    }
}

/// Maximum number of reallocation attempts triggered by policy denials.
/// Each iteration either strictly grows the excluded set or converges;
/// capping protects against pathological policy combinations.
const MAX_POLICY_ITERATIONS: usize = 4;

/// Engine that layers a [`PolicySet`] over [`DefaultEngine`].
///
/// Pipeline:
/// 1. Validate source.
/// 2. Allocate with the current exclusion set (starts empty).
/// 3. Build a candidate [`RebalanceOutput`].
/// 4. Evaluate every policy against every proposed trade.
///    - `Warn`: record the violation; leave the trade untouched.
///    - `Deny`: add the `(account, ticker)` to the exclusion set.
/// 5. If new denials appeared, loop to step 2 (up to
///    [`MAX_POLICY_ITERATIONS`] times).
/// 6. Zero out any trade whose `(account, ticker)` is still in the
///    exclusion set, preserving the current position and cash.
/// 7. Attach violations to `summary.policy_violations`.
#[derive(Clone)]
pub struct PolicyAwareEngine {
    pub inner: DefaultEngine,
    pub policies: Arc<PolicySet>,
}

impl PolicyAwareEngine {
    pub fn new(inner: DefaultEngine, policies: PolicySet) -> Self {
        Self {
            inner,
            policies: Arc::new(policies),
        }
    }
}

impl std::fmt::Debug for PolicyAwareEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PolicyAwareEngine")
            .field("inner", &self.inner)
            .field("policies", &format!("<{} compiled>", self.policies.len()))
            .finish()
    }
}

impl RebalanceEngine for PolicyAwareEngine {
    fn rebalance(&self, source: &dyn PortfolioSource) -> Result<RebalanceOutput, RebalanceError> {
        validate(source)?;

        let mut excluded: HashSet<TradeKey> = HashSet::new();

        // Fast path: no policies → one allocation pass, no loop.
        if self.policies.is_empty() {
            let (out, _) = self.inner.rebalance_with_exclusions(source, &excluded)?;
            return Ok(out);
        }

        // Accumulate violations across iterations so the output records
        // the policy firings that caused reallocation, not just those that
        // happen to still fire on the final pass. Dedup key is
        // (policy, account, ticker, action).
        let mut seen: BTreeMap<ViolationKey, PolicyViolation> = BTreeMap::new();
        let mut last_output: Option<RebalanceOutput> = None;

        for _ in 0..MAX_POLICY_ITERATIONS {
            let (out, _) = self.inner.rebalance_with_exclusions(source, &excluded)?;
            let (new_excluded, violations) = self.evaluate_output(source, &out, &excluded)?;
            for v in &violations {
                seen.entry(ViolationKey::from(v))
                    .or_insert_with(|| v.clone());
            }
            let converged = new_excluded == excluded;
            last_output = Some(out);
            excluded = new_excluded;
            if converged {
                break;
            }
        }

        // If the loop cap tripped without convergence, `last_output` still
        // reflects the penultimate pass — run one final allocation against
        // the accumulated exclusion set so zeroing lines up with the
        // exclusions we actually report.
        let final_output = match last_output {
            Some(o) => o,
            None => self.inner.rebalance_with_exclusions(source, &excluded)?.0,
        };
        let violations: Vec<PolicyViolation> = seen.into_values().collect();
        Ok(finalize(final_output, source, &excluded, violations))
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct ViolationKey {
    policy: String,
    account: String,
    ticker: Option<String>,
    action: String,
}

impl ViolationKey {
    fn from(v: &PolicyViolation) -> Self {
        ViolationKey {
            policy: v.policy.clone(),
            account: v.account.clone(),
            ticker: v.ticker.clone(),
            action: v.action.clone(),
        }
    }
}

impl PolicyAwareEngine {
    /// Walk every proposed `(account, ticker)` trade, evaluate the policy
    /// set, and return the updated exclusion set plus all violations
    /// emitted by firing policies.
    fn evaluate_output(
        &self,
        source: &dyn PortfolioSource,
        output: &RebalanceOutput,
        current_excluded: &HashSet<TradeKey>,
    ) -> Result<(HashSet<TradeKey>, Vec<PolicyViolation>), RebalanceError> {
        let mut next = current_excluded.clone();
        let mut violations: Vec<PolicyViolation> = Vec::new();
        let total_value = output.summary.total_value;

        for (account_name, acct_result) in &output.accounts {
            let aid = match source.registry().account_id(account_name) {
                Some(a) => a,
                None => continue,
            };
            let account_data = match source.account(aid) {
                Some(a) => a,
                None => continue,
            };
            let kind = account_data.kind.as_deref().unwrap_or("").to_string();
            for (ticker, pos) in &acct_result.positions {
                let sid = match source.registry().security_id(ticker) {
                    Some(s) => s,
                    None => continue,
                };
                let ctx = trade_context(
                    account_name,
                    &kind,
                    acct_result.starting_cash,
                    acct_result.ending_cash,
                    ticker,
                    pos,
                    total_value,
                    aid,
                    sid,
                );
                let TradeVerdict {
                    action,
                    violations: v,
                } = self.policies.evaluate_trade(&ctx)?;
                violations.extend(v);
                if matches!(action, PolicyAction::Deny) {
                    next.insert((aid, sid));
                }
            }
        }
        Ok((next, violations))
    }
}

#[allow(clippy::too_many_arguments)]
fn trade_context(
    account_name: &str,
    kind: &str,
    starting_cash: Decimal,
    ending_cash: Decimal,
    ticker: &str,
    pos: &PositionResult,
    total_value: Decimal,
    aid: AccountId,
    sid: SecurityId,
) -> TradeContext {
    let sale = if pos.trade_shares < 0 {
        let shares_sold: i64 = pos.lots_sold.iter().map(|l| l.shares_sold).sum();
        let short: Decimal = pos
            .lots_sold
            .iter()
            .filter(|l| !l.is_long_term)
            .map(|l| l.realized_gain)
            .sum();
        let long: Decimal = pos
            .lots_sold
            .iter()
            .filter(|l| l.is_long_term)
            .map(|l| l.realized_gain)
            .sum();
        let long_shares: i64 = pos
            .lots_sold
            .iter()
            .filter(|l| l.is_long_term)
            .map(|l| l.shares_sold)
            .sum();
        let long_fraction = if shares_sold > 0 {
            long_shares as f64 / shares_sold as f64
        } else {
            0.0
        };
        Some(SaleContext {
            shares_sold,
            realized_gain: to_f64(short + long),
            short_term_gain: to_f64(short),
            long_term_gain: to_f64(long),
            long_term_fraction: long_fraction,
        })
    } else {
        None
    };
    TradeContext {
        account: AccountContext {
            name: account_name.to_string(),
            kind: kind.to_string(),
            cash: to_f64(starting_cash),
            ending_cash: to_f64(ending_cash),
        },
        position: PositionContext {
            ticker: ticker.to_string(),
            current_shares: pos.current_shares,
            target_shares: pos.target_shares,
            trade_shares: pos.trade_shares,
            trade_value: to_f64(pos.trade_value),
            price: to_f64(pos.price),
            is_buy: pos.trade_shares > 0,
            is_sell: pos.trade_shares < 0,
        },
        sale,
        summary: SummaryContext {
            total_value: to_f64(total_value),
        },
        account_id: aid,
        security_id: sid,
    }
}

/// Apply final policy state to `output`: zero excluded trades, merge
/// violations into `summary.policy_violations`.
fn finalize(
    mut output: RebalanceOutput,
    source: &dyn PortfolioSource,
    excluded: &HashSet<TradeKey>,
    mut violations: Vec<PolicyViolation>,
) -> RebalanceOutput {
    for (account_name, acct) in output.accounts.iter_mut() {
        let aid = match source.registry().account_id(account_name) {
            Some(a) => a,
            None => continue,
        };
        let mut ending_cash_delta = Decimal::ZERO;
        let mut realized_delta = Decimal::ZERO;
        let mut short_delta = Decimal::ZERO;
        let mut long_delta = Decimal::ZERO;
        for (ticker, pos) in acct.positions.iter_mut() {
            let sid = match source.registry().security_id(ticker) {
                Some(s) => s,
                None => continue,
            };
            if !excluded.contains(&(aid, sid)) {
                continue;
            }
            // Reverse this trade. `ending_cash` was built as
            // `starting_cash - Σ trade_value` (sells have negative
            // trade_value, so selling *adds* to ending_cash). Adding
            // `trade_value` back undoes the contribution of this one trade.
            ending_cash_delta += pos.trade_value;
            for lot in &pos.lots_sold {
                realized_delta += lot.realized_gain;
                if lot.is_long_term {
                    long_delta += lot.realized_gain;
                } else {
                    short_delta += lot.realized_gain;
                }
            }
            pos.target_shares = pos.current_shares;
            pos.trade_shares = 0;
            pos.trade_value = Decimal::ZERO;
            pos.lots_sold.clear();
        }
        acct.ending_cash += ending_cash_delta;
        acct.realized_gain -= realized_delta;
        acct.short_term_gain -= short_delta;
        acct.long_term_gain -= long_delta;
        output.summary.total_realized_gain -= realized_delta;
        output.summary.total_short_term_gain -= short_delta;
        output.summary.total_long_term_gain -= long_delta;
    }
    // Stable ordering so CLI output is deterministic.
    violations.sort_by(|a, b| {
        a.policy
            .cmp(&b.policy)
            .then_with(|| a.account.cmp(&b.account))
            .then_with(|| a.ticker.cmp(&b.ticker))
    });
    output.summary.policy_violations = violations;
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::InMemoryPortfolio;
    use crate::model::{
        Account, DecimalStr, PositionEntry, PositionsFile, PricesFile, Sleeve, TargetsFile,
    };
    use crate::policy::PolicySpec;
    use pretty_assertions::assert_eq;
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;
    use time::macros::date;

    fn port() -> InMemoryPortfolio {
        let positions = PositionsFile {
            accounts: BTreeMap::from([
                (
                    "roth".to_string(),
                    Account {
                        r#type: Some("roth".into()),
                        cash: dec!(5000),
                        positions: BTreeMap::new(),
                    },
                ),
                (
                    "taxable".to_string(),
                    Account {
                        r#type: Some("taxable".into()),
                        cash: dec!(5000),
                        positions: BTreeMap::new(),
                    },
                ),
            ]),
        };
        let prices = PricesFile {
            prices: BTreeMap::from([("VTI".to_string(), DecimalStr(dec!(100)))]),
        };
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "us".to_string(),
                Sleeve {
                    target_weight: dec!(1.0),
                    holdings: BTreeMap::from([("VTI".to_string(), DecimalStr(dec!(1.0)))]),
                    preferred_accounts: vec!["taxable".into(), "roth".into()],
                },
            )]),
        };
        InMemoryPortfolio::from_dtos(&positions, &prices, &targets).unwrap()
    }

    fn as_of() -> Date {
        date!(2025 - 01 - 01)
    }

    #[test]
    fn empty_policy_set_matches_default_engine() {
        let p = port();
        let engine = PolicyAwareEngine::new(
            DefaultEngine::new(LotSelector::Fifo, Some(as_of())),
            PolicySet::empty(),
        );
        let out = engine.rebalance(&p).unwrap();
        assert!(out.summary.policy_violations.is_empty());
        // Taxable fills first, roth gets the rest.
        assert_eq!(out.accounts["taxable"].positions["VTI"].trade_shares, 50);
        assert_eq!(out.accounts["roth"].positions["VTI"].trade_shares, 50);
    }

    #[test]
    fn deny_reroutes_to_next_eligible_account() {
        let p = port();
        let set = PolicySet::from_specs(&[PolicySpec {
            name: "no-vti-in-taxable".into(),
            when: "account.name == 'taxable' && position.ticker == 'VTI'".into(),
            action: PolicyAction::Deny,
            message: Some("no VTI in taxable".into()),
        }])
        .unwrap();
        let engine =
            PolicyAwareEngine::new(DefaultEngine::new(LotSelector::Fifo, Some(as_of())), set);
        let out = engine.rebalance(&p).unwrap();
        // Taxable should never buy VTI — either absent or trade = 0.
        assert!(out.accounts["taxable"]
            .positions
            .get("VTI")
            .map(|p| p.trade_shares == 0)
            .unwrap_or(true));
        // Roth absorbs the full sleeve allocation (50 shares at $100 =
        // $5000, its capacity).
        assert_eq!(out.accounts["roth"].positions["VTI"].trade_shares, 50);
        // Taxable keeps its cash.
        assert_eq!(out.accounts["taxable"].ending_cash, dec!(5000));
        assert!(!out.summary.policy_violations.is_empty());
        assert!(out
            .summary
            .policy_violations
            .iter()
            .any(|v| v.action == "deny" && v.account == "taxable"));
    }

    #[test]
    fn deny_with_no_alternative_zeros_trade_and_keeps_cash() {
        // Only one account; denying taxable-VTI means nothing moves.
        let positions = PositionsFile {
            accounts: BTreeMap::from([(
                "taxable".to_string(),
                Account {
                    r#type: Some("taxable".into()),
                    cash: dec!(5000),
                    positions: BTreeMap::new(),
                },
            )]),
        };
        let prices = PricesFile {
            prices: BTreeMap::from([("VTI".to_string(), DecimalStr(dec!(100)))]),
        };
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "us".to_string(),
                Sleeve {
                    target_weight: dec!(1.0),
                    holdings: BTreeMap::from([("VTI".to_string(), DecimalStr(dec!(1.0)))]),
                    preferred_accounts: vec!["taxable".into()],
                },
            )]),
        };
        let p = InMemoryPortfolio::from_dtos(&positions, &prices, &targets).unwrap();
        let set = PolicySet::from_specs(&[PolicySpec {
            name: "no-trading".into(),
            when: "true".into(),
            action: PolicyAction::Deny,
            message: Some("frozen".into()),
        }])
        .unwrap();
        let engine =
            PolicyAwareEngine::new(DefaultEngine::new(LotSelector::Fifo, Some(as_of())), set);
        let out = engine.rebalance(&p).unwrap();
        // No trade occurred — VTI either absent or zeroed.
        assert!(out.accounts["taxable"]
            .positions
            .get("VTI")
            .map(|p| p.trade_shares == 0)
            .unwrap_or(true));
        assert_eq!(out.accounts["taxable"].ending_cash, dec!(5000));
        assert!(out
            .summary
            .policy_violations
            .iter()
            .any(|v| v.policy == "no-trading"));
    }

    #[test]
    fn warn_does_not_change_trades_but_records_violation() {
        let p = port();
        let set = PolicySet::from_specs(&[PolicySpec {
            name: "info".into(),
            when: "position.is_buy".into(),
            action: PolicyAction::Warn,
            message: Some("buying".into()),
        }])
        .unwrap();
        let engine =
            PolicyAwareEngine::new(DefaultEngine::new(LotSelector::Fifo, Some(as_of())), set);
        let out = engine.rebalance(&p).unwrap();
        // Allocation unchanged.
        assert_eq!(out.accounts["taxable"].positions["VTI"].trade_shares, 50);
        assert_eq!(out.accounts["roth"].positions["VTI"].trade_shares, 50);
        // Two buys → two warnings.
        assert_eq!(out.summary.policy_violations.len(), 2);
        assert!(out
            .summary
            .policy_violations
            .iter()
            .all(|v| v.action == "warn"));
    }

    #[test]
    fn finalize_zeroes_denied_sell_and_preserves_current_shares() {
        // `taxable` holds untargeted AAPL that would normally be
        // liquidated to zero. The policy denies that sale, so AAPL stays
        // put at 10 shares. Roth handles the sleeve alone, so excluding
        // AAPL doesn't impact the allocator elsewhere.
        let positions = PositionsFile {
            accounts: BTreeMap::from([
                (
                    "taxable".to_string(),
                    Account {
                        r#type: Some("taxable".into()),
                        cash: dec!(0),
                        positions: BTreeMap::from([(
                            "AAPL".to_string(),
                            PositionEntry::Shares(10),
                        )]),
                    },
                ),
                (
                    "roth".to_string(),
                    Account {
                        r#type: Some("roth".into()),
                        cash: dec!(5000),
                        positions: BTreeMap::new(),
                    },
                ),
            ]),
        };
        let prices = PricesFile {
            prices: BTreeMap::from([
                ("AAPL".to_string(), DecimalStr(dec!(100))),
                ("VTI".to_string(), DecimalStr(dec!(100))),
            ]),
        };
        // Sleeve prefers roth, and taxable won't be eligible (fully used
        // by AAPL we're forbidden to sell).
        let targets = TargetsFile {
            sleeves: BTreeMap::from([(
                "us".to_string(),
                Sleeve {
                    target_weight: dec!(1.0),
                    holdings: BTreeMap::from([("VTI".to_string(), DecimalStr(dec!(1.0)))]),
                    preferred_accounts: vec!["roth".into()],
                },
            )]),
        };
        let p = InMemoryPortfolio::from_dtos(&positions, &prices, &targets).unwrap();
        let set = PolicySet::from_specs(&[PolicySpec {
            name: "keep-aapl".into(),
            when: "position.ticker == 'AAPL' && position.is_sell".into(),
            action: PolicyAction::Deny,
            message: Some("hold AAPL".into()),
        }])
        .unwrap();
        let engine =
            PolicyAwareEngine::new(DefaultEngine::new(LotSelector::Fifo, Some(as_of())), set);
        let out = engine.rebalance(&p).unwrap();
        let aapl = &out.accounts["taxable"].positions["AAPL"];
        assert_eq!(aapl.trade_shares, 0);
        assert_eq!(aapl.target_shares, 10);
        assert_eq!(aapl.trade_value, Decimal::ZERO);
        assert_eq!(out.accounts["taxable"].ending_cash, dec!(0));
        assert!(out
            .summary
            .policy_violations
            .iter()
            .any(|v| v.policy == "keep-aapl"));
    }

    #[test]
    fn loop_terminates_under_pathological_policies() {
        // Every possible target triggers deny. The engine should cap out,
        // zero everything, and produce a result rather than hang.
        let p = port();
        let set = PolicySet::from_specs(&[PolicySpec {
            name: "deny-all".into(),
            when: "true".into(),
            action: PolicyAction::Deny,
            message: None,
        }])
        .unwrap();
        let engine =
            PolicyAwareEngine::new(DefaultEngine::new(LotSelector::Fifo, Some(as_of())), set);
        let out = engine.rebalance(&p).unwrap();
        assert!(out
            .accounts
            .values()
            .all(|a| a.positions.values().all(|p| p.trade_shares == 0)));
    }
}
