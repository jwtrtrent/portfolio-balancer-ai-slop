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

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}
