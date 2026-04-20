use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use portfolio_rebalancer::{
    io_json::read_json, sqlite_ingest_inputs, DefaultEngine, JsonStoreLoader, LoadedStore,
    LotSelector, PolicyAwareEngine, PolicySet, PositionsFile, PricesFile, RebalanceEngine,
    SqliteStoreLoader, StoreLoader, TargetsFile,
};
use time::Date;

/// Multi-account stock portfolio rebalancer.
///
/// Reads portfolio inputs from a JSON triplet (positions/prices/targets) or a
/// SQLite database, runs the configured engine, and writes the result back to
/// the same backend.
#[derive(Parser, Debug)]
#[command(name = "portfolio-rebalancer", version, about)]
struct Cli {
    /// Storage backend for portfolio inputs and rebalance output.
    #[arg(long, value_enum, default_value_t = StoreKind::Json)]
    store: StoreKind,

    /// Path to positions JSON. Required with `--store json`.
    #[arg(long)]
    positions: Option<PathBuf>,
    /// Path to prices JSON. Required with `--store json`.
    #[arg(long)]
    prices: Option<PathBuf>,
    /// Path to targets JSON. Required with `--store json`.
    #[arg(long)]
    targets: Option<PathBuf>,
    /// Path to write the rebalance output JSON. Required with `--store json`.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Path to the SQLite database. Required with `--store sqlite`.
    #[arg(long)]
    db: Option<PathBuf>,
    /// Optional human label persisted on the SQLite run row.
    #[arg(long)]
    run_label: Option<String>,

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
enum StoreKind {
    Json,
    Sqlite,
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

fn build_loader(cli: &Cli) -> Result<Box<dyn StoreLoader>> {
    match cli.store {
        StoreKind::Json => {
            let positions = cli
                .positions
                .clone()
                .ok_or_else(|| anyhow!("--positions is required with --store json"))?;
            let prices = cli
                .prices
                .clone()
                .ok_or_else(|| anyhow!("--prices is required with --store json"))?;
            let targets = cli
                .targets
                .clone()
                .ok_or_else(|| anyhow!("--targets is required with --store json"))?;
            let output = cli
                .output
                .clone()
                .ok_or_else(|| anyhow!("--output is required with --store json"))?;
            Ok(Box::new(JsonStoreLoader {
                positions_path: positions,
                prices_path: prices,
                targets_path: targets,
                output_path: output,
            }))
        }
        StoreKind::Sqlite => {
            let db = cli
                .db
                .clone()
                .ok_or_else(|| anyhow!("--db is required with --store sqlite"))?;
            let mut loader = SqliteStoreLoader::new(db);
            if let Some(label) = cli.run_label.clone() {
                loader = loader.with_label(label);
            }
            Ok(Box::new(loader))
        }
    }
}

/// Optional JSON-into-SQLite ingest: when `--store sqlite` is paired with the
/// JSON input flags, load the JSON and overwrite the database's portfolio
/// tables before running the engine.
fn maybe_ingest_into_sqlite(cli: &Cli) -> Result<()> {
    if !matches!(cli.store, StoreKind::Sqlite) {
        return Ok(());
    }
    let any_json_flag = cli.positions.is_some() || cli.prices.is_some() || cli.targets.is_some();
    if !any_json_flag {
        return Ok(());
    }
    let positions_path = cli
        .positions
        .as_ref()
        .ok_or_else(|| anyhow!("--positions required when ingesting JSON into sqlite"))?;
    let prices_path = cli
        .prices
        .as_ref()
        .ok_or_else(|| anyhow!("--prices required when ingesting JSON into sqlite"))?;
    let targets_path = cli
        .targets
        .as_ref()
        .ok_or_else(|| anyhow!("--targets required when ingesting JSON into sqlite"))?;
    let db = cli
        .db
        .as_ref()
        .ok_or_else(|| anyhow!("--db required with --store sqlite"))?;

    let positions: PositionsFile = read_json(positions_path)?;
    let prices: PricesFile = read_json(prices_path)?;
    let targets: TargetsFile = read_json(targets_path)?;

    let pool = portfolio_rebalancer::store::sqlite::open_pool(db)?;
    sqlite_ingest_inputs(&pool, &positions, &prices, &targets)?;
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    maybe_ingest_into_sqlite(&cli).context("ingesting JSON into sqlite")?;
    let loader = build_loader(&cli)?;
    let LoadedStore { source, sink } = loader.load().context("loading portfolio")?;
    let default = DefaultEngine::new(cli.lot_strategy.into(), cli.as_of);
    let policies = match &cli.policies {
        Some(path) => PolicySet::from_file(path)
            .with_context(|| format!("loading policies from {}", path.display()))?,
        None => PolicySet::empty(),
    };
    let engine = PolicyAwareEngine::new(default, policies);
    let output = engine.rebalance(&*source)?;
    sink.write(&output).context("writing output")?;
    Ok(())
}
