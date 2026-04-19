use thiserror::Error;

#[derive(Debug, Error)]
pub enum RebalanceError {
    #[error("sleeve `{sleeve}` sub-weights sum to {actual}, expected 1.0")]
    SleeveSubWeightsSum { sleeve: String, actual: String },

    #[error("sleeve target weights sum to {actual}, expected 1.0")]
    SleeveTargetWeightsSum { actual: String },

    #[error("ticker `{ticker}` is referenced but no price was provided")]
    MissingPrice { ticker: String },

    #[error("price for `{ticker}` must be positive (got {price})")]
    NonPositivePrice { ticker: String, price: String },

    #[error("sleeve `{sleeve}` references unknown account `{account}`")]
    UnknownPreferredAccount { sleeve: String, account: String },

    #[error("negative cash for account `{account}`: {cash}")]
    NegativeCash { account: String, cash: String },

    #[error("negative share count for `{ticker}` in account `{account}`: {shares}")]
    NegativeShares {
        account: String,
        ticker: String,
        shares: i64,
    },

    #[error("sleeve `{sleeve}` has non-positive sub-weight {weight} for `{ticker}`")]
    NonPositiveSubWeight {
        sleeve: String,
        ticker: String,
        weight: String,
    },

    #[error("sleeve `{sleeve}` has non-positive target weight {weight}")]
    NonPositiveTargetWeight { sleeve: String, weight: String },

    #[error("portfolio total value is zero — nothing to rebalance")]
    ZeroPortfolioValue,

    #[error("lot in account `{account}` for `{ticker}` has non-positive quantity {quantity}")]
    NonPositiveLotQuantity {
        account: String,
        ticker: String,
        quantity: i64,
    },

    #[error("lot in account `{account}` for `{ticker}` has negative cost basis {basis}")]
    NegativeLotBasis {
        account: String,
        ticker: String,
        basis: String,
    },

    #[error("lot in account `{account}` for `{ticker}` is acquired in the future: {acquired}")]
    LotAcquiredAfterAsOf {
        account: String,
        ticker: String,
        acquired: String,
    },

    #[error(
        "lots for `{ticker}` in account `{account}` sum to {lot_sum} but position is {aggregate}"
    )]
    LotSumMismatch {
        account: String,
        ticker: String,
        lot_sum: i64,
        aggregate: i64,
    },

    #[error("failed to compile policy `{policy}`: {message}")]
    PolicyCompile { policy: String, message: String },

    #[error("failed to evaluate policy `{policy}`: {message}")]
    PolicyEval { policy: String, message: String },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}
