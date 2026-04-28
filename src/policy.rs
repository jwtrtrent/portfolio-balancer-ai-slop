//! CEL-based trade policies.
//!
//! A policy is a named, pre-compiled Common Expression Language predicate
//! whose verdict ([`PolicyAction`]) determines whether a proposed
//! `(account, ticker)` trade is allowed, warned about, or denied. The
//! [`PolicyAwareEngine`] consumes a [`PolicySet`] and re-runs allocation
//! when a `Deny` verdict fires.
//!
//! Policy evaluation is stateless: a [`CompiledPolicy`] stores the parsed
//! [`cel_interpreter::Program`] behind an [`Arc`] so it can be shared cheaply
//! across threads. The program is compiled once when the [`PolicySet`] is
//! loaded.

use std::path::Path;
use std::sync::Arc;

use cel_interpreter::{Context, Program, Value};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::errors::RebalanceError;
use crate::id::{AccountId, SecurityId};
use crate::io_json::read_json;
use crate::model::PolicyViolation;

/// The verdict a CEL policy can emit when its `when` expression evaluates
/// to `true`.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PolicyAction {
    /// Explicitly allow the trade. Evaluated mostly for documentation value;
    /// missing policies default to "allow" anyway.
    #[default]
    Allow,
    /// Record a violation row but leave the trade untouched.
    Warn,
    /// Forbid the `(account, ticker)` trade. The engine re-allocates with
    /// that pair excluded; if no alternative exists, the trade is zeroed.
    Deny,
}

impl PolicyAction {
    /// Short lowercase tag used in [`PolicyViolation::action`].
    pub fn as_str(self) -> &'static str {
        match self {
            PolicyAction::Allow => "allow",
            PolicyAction::Warn => "warn",
            PolicyAction::Deny => "deny",
        }
    }
}

/// Serde DTO backing `policies.json`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PolicyFile {
    pub policies: Vec<PolicySpec>,
}

/// Single policy entry as it appears on disk.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PolicySpec {
    pub name: String,
    /// CEL expression. Must evaluate to a boolean at runtime.
    pub when: String,
    pub action: PolicyAction,
    /// Human-readable explanation. Surfaces in
    /// [`crate::model::PolicyViolation::message`] when the policy fires.
    #[serde(default)]
    pub message: Option<String>,
}

/// A pre-compiled policy ready for evaluation.
#[derive(Clone)]
pub struct CompiledPolicy {
    pub name: Arc<str>,
    pub action: PolicyAction,
    pub message: Arc<str>,
    /// `Program` is not `Clone` in cel-interpreter 0.10, so it hides behind
    /// an `Arc`. Evaluation is stateless; sharing is safe.
    program: Arc<Program>,
}

impl std::fmt::Debug for CompiledPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledPolicy")
            .field("name", &self.name)
            .field("action", &self.action)
            .field("message", &self.message)
            .finish()
    }
}

impl CompiledPolicy {
    pub fn compile(spec: &PolicySpec) -> Result<Self, RebalanceError> {
        let program = Program::compile(&spec.when).map_err(|e| RebalanceError::PolicyCompile {
            policy: spec.name.clone(),
            message: e.to_string(),
        })?;
        Ok(CompiledPolicy {
            name: Arc::<str>::from(spec.name.as_str()),
            action: spec.action,
            message: Arc::<str>::from(spec.message.as_deref().unwrap_or("")),
            program: Arc::new(program),
        })
    }

    /// Evaluate `when` against `ctx` and return the [`PolicyAction`] iff the
    /// predicate was true. `None` means the trade is not affected by this
    /// policy.
    pub fn evaluate(&self, ctx: &TradeContext) -> Result<Option<PolicyAction>, RebalanceError> {
        let mut cel = Context::default();
        ctx.install(&mut cel)
            .map_err(|e| RebalanceError::PolicyEval {
                policy: self.name.to_string(),
                message: e.to_string(),
            })?;
        let value = self
            .program
            .execute(&cel)
            .map_err(|e| RebalanceError::PolicyEval {
                policy: self.name.to_string(),
                message: e.to_string(),
            })?;
        match value {
            Value::Bool(true) => Ok(Some(self.action)),
            Value::Bool(false) => Ok(None),
            other => Err(RebalanceError::PolicyEval {
                policy: self.name.to_string(),
                message: format!("`when` must return bool, got {:?}", other.type_of()),
            }),
        }
    }
}

/// A thread-safe, cheaply clonable set of pre-compiled policies.
#[derive(Clone, Debug, Default)]
pub struct PolicySet {
    policies: Arc<[CompiledPolicy]>,
}

impl PolicySet {
    pub fn empty() -> Self {
        PolicySet {
            policies: Arc::<[CompiledPolicy]>::from(Vec::<CompiledPolicy>::new()),
        }
    }

    pub fn from_specs(specs: &[PolicySpec]) -> Result<Self, RebalanceError> {
        let mut out = Vec::with_capacity(specs.len());
        for spec in specs {
            out.push(CompiledPolicy::compile(spec)?);
        }
        Ok(PolicySet {
            policies: Arc::<[CompiledPolicy]>::from(out),
        })
    }

    pub fn from_file(path: &Path) -> Result<Self, RebalanceError> {
        let file: PolicyFile = read_json(path)?;
        Self::from_specs(&file.policies)
    }

    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    pub fn len(&self) -> usize {
        self.policies.len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, CompiledPolicy> {
        self.policies.iter()
    }

    /// Evaluate every policy against a candidate trade. Returns the
    /// strongest action taken by any firing policy (`Deny` > `Warn` >
    /// `Allow`) along with violations emitted by *all* firing
    /// `Warn`/`Deny` policies.
    pub fn evaluate_trade(&self, ctx: &TradeContext) -> Result<TradeVerdict, RebalanceError> {
        let mut strongest: Option<PolicyAction> = None;
        let mut violations: Vec<PolicyViolation> = Vec::new();
        for policy in self.policies.iter() {
            let Some(action) = policy.evaluate(ctx)? else {
                continue;
            };
            if matches!(action, PolicyAction::Warn | PolicyAction::Deny) {
                violations.push(PolicyViolation {
                    policy: policy.name.to_string(),
                    action: action.as_str().to_string(),
                    account: ctx.account.name.to_string(),
                    ticker: Some(ctx.position.ticker.to_string()),
                    message: policy.message.to_string(),
                });
            }
            if stronger(action, strongest) {
                strongest = Some(action);
            }
        }
        Ok(TradeVerdict {
            action: strongest.unwrap_or(PolicyAction::Allow),
            violations,
        })
    }
}

fn stronger(candidate: PolicyAction, current: Option<PolicyAction>) -> bool {
    let rank = |a: PolicyAction| match a {
        PolicyAction::Allow => 0,
        PolicyAction::Warn => 1,
        PolicyAction::Deny => 2,
    };
    match current {
        None => true,
        Some(cur) => rank(candidate) > rank(cur),
    }
}

/// Aggregate verdict across all policies for one candidate trade.
#[derive(Debug, Clone, Default)]
pub struct TradeVerdict {
    pub action: PolicyAction,
    pub violations: Vec<PolicyViolation>,
}

/// Evaluation input for one candidate `(account, security)` trade.
///
/// Built by [`crate::engine::policy_engine::build_contexts`] after a fresh
/// `allocate` pass. The struct is flat so the derived [`serde::Serialize`]
/// impl maps one-to-one onto CEL identifiers (`account.cash`,
/// `position.trade_shares`, ...).
#[derive(Debug, Clone, Serialize)]
pub struct TradeContext {
    pub account: AccountContext,
    pub position: PositionContext,
    pub sale: Option<SaleContext>,
    pub summary: SummaryContext,
    // Exclude typed IDs from the serialized context — they're noise for the
    // policy author and would collide with the string fields.
    #[serde(skip)]
    pub account_id: AccountId,
    #[serde(skip)]
    pub security_id: SecurityId,
}

impl TradeContext {
    fn install(&self, ctx: &mut Context<'_>) -> Result<(), cel_interpreter::SerializationError> {
        ctx.add_variable("account", &self.account)?;
        ctx.add_variable("position", &self.position)?;
        ctx.add_variable("sale", &self.sale)?;
        ctx.add_variable("summary", &self.summary)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountContext {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub cash: f64,
    pub ending_cash: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionContext {
    pub ticker: String,
    pub current_shares: i64,
    pub target_shares: i64,
    pub trade_shares: i64,
    pub trade_value: f64,
    pub price: f64,
    pub is_buy: bool,
    pub is_sell: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SaleContext {
    pub shares_sold: i64,
    pub realized_gain: f64,
    pub short_term_gain: f64,
    pub long_term_gain: f64,
    /// Fraction of the sold shares that were held ≥ 365 days (0.0–1.0).
    pub long_term_fraction: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SummaryContext {
    pub total_value: f64,
}

/// Helper: lossy `Decimal` → `f64` conversion for CEL context fields.
/// CEL has no decimal type; policies operate on floats. Callers should
/// never use these values to re-compute money amounts — they're
/// comparison operands only.
pub fn to_f64(d: Decimal) -> f64 {
    d.to_f64().unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_ctx() -> TradeContext {
        TradeContext {
            account: AccountContext {
                name: "taxable".into(),
                kind: "taxable".into(),
                cash: 1000.0,
                ending_cash: 12.34,
            },
            position: PositionContext {
                ticker: "VTI".into(),
                current_shares: 10,
                target_shares: 5,
                trade_shares: -5,
                trade_value: 1250.0,
                price: 250.0,
                is_buy: false,
                is_sell: true,
            },
            sale: Some(SaleContext {
                shares_sold: 5,
                realized_gain: -100.0,
                short_term_gain: -100.0,
                long_term_gain: 0.0,
                long_term_fraction: 0.0,
            }),
            summary: SummaryContext {
                total_value: 20_000.0,
            },
            account_id: AccountId(0),
            security_id: SecurityId(0),
        }
    }

    fn policy(name: &str, when: &str, action: PolicyAction) -> CompiledPolicy {
        CompiledPolicy::compile(&PolicySpec {
            name: name.into(),
            when: when.into(),
            action,
            message: Some(format!("triggered by {name}")),
        })
        .unwrap()
    }

    #[test]
    fn compile_failure_surfaces_as_policy_compile_error() {
        let err = CompiledPolicy::compile(&PolicySpec {
            name: "bad".into(),
            when: "this is not cel %%%".into(),
            action: PolicyAction::Deny,
            message: None,
        })
        .unwrap_err();
        assert!(matches!(err, RebalanceError::PolicyCompile { .. }));
    }

    #[test]
    fn evaluate_returns_allow_when_predicate_false() {
        let p = policy("allow", "position.trade_shares > 100", PolicyAction::Deny);
        let ctx = sample_ctx();
        assert_eq!(p.evaluate(&ctx).unwrap(), None);
    }

    #[test]
    fn evaluate_fires_when_predicate_true() {
        let p = policy(
            "no-big-sells",
            "position.is_sell && position.trade_shares < -1",
            PolicyAction::Deny,
        );
        let ctx = sample_ctx();
        assert_eq!(p.evaluate(&ctx).unwrap(), Some(PolicyAction::Deny));
    }

    #[test]
    fn evaluate_uses_account_type() {
        let p = policy(
            "taxable-only",
            "account.type == 'taxable' && position.is_sell",
            PolicyAction::Warn,
        );
        let ctx = sample_ctx();
        assert_eq!(p.evaluate(&ctx).unwrap(), Some(PolicyAction::Warn));
    }

    #[test]
    fn non_bool_predicate_errors() {
        let p = policy("num", "position.trade_shares", PolicyAction::Deny);
        let ctx = sample_ctx();
        let err = p.evaluate(&ctx).unwrap_err();
        assert!(matches!(err, RebalanceError::PolicyEval { .. }));
    }

    #[test]
    fn unknown_identifier_errors_as_eval() {
        let p = policy("typo", "typo.foo == 1", PolicyAction::Deny);
        let ctx = sample_ctx();
        let err = p.evaluate(&ctx).unwrap_err();
        assert!(matches!(err, RebalanceError::PolicyEval { .. }));
    }

    #[test]
    fn policy_set_aggregates_strongest_verdict() {
        let set = PolicySet::from_specs(&[
            PolicySpec {
                name: "warn-all".into(),
                when: "true".into(),
                action: PolicyAction::Warn,
                message: Some("w".into()),
            },
            PolicySpec {
                name: "deny-sells".into(),
                when: "position.is_sell".into(),
                action: PolicyAction::Deny,
                message: Some("d".into()),
            },
        ])
        .unwrap();
        let ctx = sample_ctx();
        let verdict = set.evaluate_trade(&ctx).unwrap();
        assert_eq!(verdict.action, PolicyAction::Deny);
        // Both policies fire → two violations.
        assert_eq!(verdict.violations.len(), 2);
        assert_eq!(verdict.violations[0].policy, "warn-all");
        assert_eq!(verdict.violations[1].policy, "deny-sells");
    }

    #[test]
    fn policy_set_allow_means_no_violations() {
        let set = PolicySet::from_specs(&[PolicySpec {
            name: "never".into(),
            when: "false".into(),
            action: PolicyAction::Deny,
            message: None,
        }])
        .unwrap();
        let verdict = set.evaluate_trade(&sample_ctx()).unwrap();
        assert_eq!(verdict.action, PolicyAction::Allow);
        assert!(verdict.violations.is_empty());
    }

    #[test]
    fn empty_policy_set_is_noop() {
        let set = PolicySet::empty();
        assert!(set.is_empty());
        let verdict = set.evaluate_trade(&sample_ctx()).unwrap();
        assert_eq!(verdict.action, PolicyAction::Allow);
        assert!(verdict.violations.is_empty());
    }

    #[test]
    fn policy_file_round_trips_through_json() {
        let json = r#"{
            "policies": [
              { "name": "p1", "when": "true", "action": "deny", "message": "m" },
              { "name": "p2", "when": "false", "action": "warn" }
            ]
          }"#;
        let file: PolicyFile = serde_json::from_str(json).unwrap();
        assert_eq!(file.policies.len(), 2);
        assert_eq!(file.policies[0].action, PolicyAction::Deny);
        assert_eq!(file.policies[1].message, None);
        let set = PolicySet::from_specs(&file.policies).unwrap();
        assert_eq!(set.len(), 2);
    }
}
