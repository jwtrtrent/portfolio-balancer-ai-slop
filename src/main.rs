use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use portfolio_rebalancer::{
    DefaultEngine, JsonStoreLoader, LoadedStore, LotSelector, PolicyAwareEngine, PolicySet,
    RebalanceEngine, StoreLoader,
};
use time::Date;

/// Multi-account stock portfolio rebalancer.
///
/// Reads three JSON inputs (positions, prices, targets) and writes a JSON
/// output containing the trades, resulting positions, and realised gains for
/// each account.
#[derive(Parser, Debug)]
#[command(name = "portfolio-rebalancer", version, about)]
struct Cli {
    /// Path to positions JSON.
    #[arg(long)]
    positions: PathBuf,
    /// Path to prices JSON.
    #[arg(long)]
    prices: PathBuf,
    /// Path to targets JSON.
    #[arg(long)]
    targets: PathBuf,
    /// Path to write the rebalance output JSON.
    #[arg(long)]
    output: PathBuf,
    /// Lot selection strategy for sells. Applies only to positions declared
    /// with explicit tax lots; bare share counts are liquidated without any
    /// per-lot reporting.
    #[arg(long, value_enum, default_value_t = LotStrategy::Fifo)]
    lot_strategy: LotStrategy,
    /// Trade-date anchor (YYYY-MM-DD). Defaults to today's UTC date.
    #[arg(long, value_parser = parse_date)]
    as_of: Option<Date>,
    /// Optional path to a CEL policy file. Omitted → no policies applied.
    #[arg(long)]
    policies: Option<PathBuf>,
}

fn parse_date(s: &str) -> Result<Date, String> {
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    Date::parse(s, fmt).map_err(|e| format!("expected YYYY-MM-DD: {e}"))
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum LotStrategy {
    Fifo,
    Lifo,
    Hifo,
    Lofo,
}

impl From<LotStrategy> for LotSelector {
    fn from(s: LotStrategy) -> Self {
        match s {
            LotStrategy::Fifo => LotSelector::Fifo,
            LotStrategy::Lifo => LotSelector::Lifo,
            LotStrategy::Hifo => LotSelector::Hifo,
            LotStrategy::Lofo => LotSelector::Lofo,
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let loader = JsonStoreLoader {
        positions_path: cli.positions.clone(),
        prices_path: cli.prices.clone(),
        targets_path: cli.targets.clone(),
        output_path: cli.output.clone(),
    };
    let LoadedStore { source, sink } = loader
        .load()
        .with_context(|| format!("loading portfolio from {}", cli.positions.display()))?;
    let default = DefaultEngine::new(cli.lot_strategy.into(), cli.as_of);
    let policies = match &cli.policies {
        Some(path) => PolicySet::from_file(path)
            .with_context(|| format!("loading policies from {}", path.display()))?,
        None => PolicySet::empty(),
    };
    let engine = PolicyAwareEngine::new(default, policies);
    let output = engine.rebalance(&*source)?;
    sink.write(&output)
        .with_context(|| format!("writing output to {}", cli.output.display()))?;
    Ok(())
}
