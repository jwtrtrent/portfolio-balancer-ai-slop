use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use portfolio_rebalancer::{
    DefaultEngine, JsonStoreLoader, LoadedStore, RebalanceEngine, StoreLoader,
};

/// Multi-account stock portfolio rebalancer.
///
/// Reads three JSON inputs (positions, prices, targets) and writes a JSON
/// output containing the trades and resulting positions for each account.
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
    let output = DefaultEngine.rebalance(&*source)?;
    sink.write(&output)
        .with_context(|| format!("writing output to {}", cli.output.display()))?;
    Ok(())
}
