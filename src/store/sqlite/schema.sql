-- Portfolio rebalancer SQLite schema.
--
-- All decimal fields are stored as TEXT (`rust_decimal::Decimal` round-trips
-- losslessly through its string form). Dates are stored as ISO 8601 TEXT.
-- Booleans use INTEGER 0/1.

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS accounts (
    name        TEXT PRIMARY KEY,
    type        TEXT,
    cash        TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS securities (
    ticker      TEXT PRIMARY KEY,
    price       TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS positions (
    account     TEXT NOT NULL REFERENCES accounts(name) ON DELETE CASCADE,
    ticker      TEXT NOT NULL REFERENCES securities(ticker),
    shares      INTEGER NOT NULL,
    PRIMARY KEY (account, ticker)
) STRICT;

CREATE TABLE IF NOT EXISTS lots (
    account       TEXT NOT NULL REFERENCES accounts(name) ON DELETE CASCADE,
    ticker        TEXT NOT NULL REFERENCES securities(ticker),
    external_id   TEXT NOT NULL,
    quantity      INTEGER NOT NULL,
    cost_basis    TEXT NOT NULL,
    acquired      TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    PRIMARY KEY (account, ticker, seq)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_lots_account_ticker ON lots(account, ticker);

CREATE TABLE IF NOT EXISTS sleeves (
    name           TEXT PRIMARY KEY,
    target_weight  TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS sleeve_holdings (
    sleeve  TEXT NOT NULL REFERENCES sleeves(name) ON DELETE CASCADE,
    ticker  TEXT NOT NULL REFERENCES securities(ticker),
    weight  TEXT NOT NULL,
    PRIMARY KEY (sleeve, ticker)
) STRICT;

CREATE TABLE IF NOT EXISTS sleeve_preferred_accounts (
    sleeve   TEXT NOT NULL REFERENCES sleeves(name) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    account  TEXT NOT NULL REFERENCES accounts(name),
    PRIMARY KEY (sleeve, position)
) STRICT;

CREATE TABLE IF NOT EXISTS rebalance_runs (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    label                   TEXT,
    created_at              TEXT NOT NULL,
    total_value             TEXT NOT NULL,
    max_drift_bps           INTEGER NOT NULL,
    total_realized_gain     TEXT NOT NULL,
    total_short_term_gain   TEXT NOT NULL,
    total_long_term_gain    TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS rebalance_account_results (
    run_id          INTEGER NOT NULL REFERENCES rebalance_runs(id) ON DELETE CASCADE,
    account         TEXT NOT NULL,
    starting_cash   TEXT NOT NULL,
    ending_cash     TEXT NOT NULL,
    realized_gain   TEXT NOT NULL,
    short_term_gain TEXT NOT NULL,
    long_term_gain  TEXT NOT NULL,
    PRIMARY KEY (run_id, account)
) STRICT;

CREATE TABLE IF NOT EXISTS rebalance_trades (
    run_id          INTEGER NOT NULL REFERENCES rebalance_runs(id) ON DELETE CASCADE,
    account         TEXT NOT NULL,
    ticker          TEXT NOT NULL,
    current_shares  INTEGER NOT NULL,
    target_shares   INTEGER NOT NULL,
    trade_shares    INTEGER NOT NULL,
    trade_value     TEXT NOT NULL,
    price           TEXT NOT NULL,
    PRIMARY KEY (run_id, account, ticker)
) STRICT;

CREATE TABLE IF NOT EXISTS rebalance_sales (
    run_id          INTEGER NOT NULL REFERENCES rebalance_runs(id) ON DELETE CASCADE,
    account         TEXT NOT NULL,
    ticker          TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    lot_id          TEXT NOT NULL,
    shares_sold     INTEGER NOT NULL,
    acquired        TEXT NOT NULL,
    holding_days    INTEGER NOT NULL,
    is_long_term    INTEGER NOT NULL,
    cost_basis      TEXT NOT NULL,
    proceeds        TEXT NOT NULL,
    realized_gain   TEXT NOT NULL,
    PRIMARY KEY (run_id, account, ticker, seq)
) STRICT;

CREATE TABLE IF NOT EXISTS rebalance_sleeve_drift (
    run_id    INTEGER NOT NULL REFERENCES rebalance_runs(id) ON DELETE CASCADE,
    sleeve    TEXT NOT NULL,
    drift_bps INTEGER NOT NULL,
    PRIMARY KEY (run_id, sleeve)
) STRICT;

CREATE TABLE IF NOT EXISTS rebalance_violations (
    run_id  INTEGER NOT NULL REFERENCES rebalance_runs(id) ON DELETE CASCADE,
    seq     INTEGER NOT NULL,
    policy  TEXT NOT NULL,
    action  TEXT NOT NULL,
    account TEXT NOT NULL,
    ticker  TEXT,
    message TEXT NOT NULL,
    PRIMARY KEY (run_id, seq)
) STRICT;
