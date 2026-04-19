use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use portfolio_rebalancer::{
    io_json::{read_json, write_json},
    rebalance, PositionsFile, PricesFile, TargetsFile,
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
    let positions: PositionsFile = read_json(&cli.positions)
        .with_context(|| format!("reading positions from {}", cli.positions.display()))?;
    let prices: PricesFile = read_json(&cli.prices)
        .with_context(|| format!("reading prices from {}", cli.prices.display()))?;
    let targets: TargetsFile = read_json(&cli.targets)
        .with_context(|| format!("reading targets from {}", cli.targets.display()))?;

    let output = rebalance(&positions, &prices, &targets)?;

    write_json(&cli.output, &output)
        .with_context(|| format!("writing output to {}", cli.output.display()))?;
    Ok(())
}
