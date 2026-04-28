use std::path::PathBuf;
use std::sync::Arc;

use crate::core::InMemoryPortfolio;
use crate::errors::RebalanceError;
use crate::io_json::{read_json, write_json};
use crate::model::{PositionsFile, PricesFile, RebalanceOutput, TargetsFile};
use crate::sink::OutputSink;
use crate::source::PortfolioSource;
use crate::store::{LoadedStore, StoreLoader};

/// Reads the three JSON input files and writes the output JSON.
#[derive(Debug, Clone)]
pub struct JsonStoreLoader {
    pub positions_path: PathBuf,
    pub prices_path: PathBuf,
    pub targets_path: PathBuf,
    pub output_path: PathBuf,
}

impl StoreLoader for JsonStoreLoader {
    fn load(&self) -> Result<LoadedStore, RebalanceError> {
        let positions: PositionsFile = read_json(&self.positions_path)?;
        let prices: PricesFile = read_json(&self.prices_path)?;
        let targets: TargetsFile = read_json(&self.targets_path)?;
        let portfolio = InMemoryPortfolio::from_dtos(&positions, &prices, &targets)?;
        let source: Arc<dyn PortfolioSource> = Arc::new(portfolio);
        let sink: Box<dyn OutputSink> = Box::new(JsonOutputSink {
            path: self.output_path.clone(),
        });
        Ok(LoadedStore { source, sink })
    }
}

/// Writes a `RebalanceOutput` as pretty-printed JSON.
#[derive(Debug, Clone)]
pub struct JsonOutputSink {
    pub path: PathBuf,
}

impl OutputSink for JsonOutputSink {
    fn write(&self, output: &RebalanceOutput) -> Result<(), RebalanceError> {
        write_json(&self.path, output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{DefaultEngine, RebalanceEngine};
    use std::fs;
    use tempfile::tempdir;

    fn write(path: &std::path::Path, content: &str) {
        fs::write(path, content).unwrap();
    }

    #[test]
    fn json_loader_round_trips_through_default_engine() {
        let dir = tempdir().unwrap();
        let positions = dir.path().join("positions.json");
        let prices = dir.path().join("prices.json");
        let targets = dir.path().join("targets.json");
        let output = dir.path().join("out.json");

        write(
            &positions,
            r#"{
              "accounts": {
                "roth": { "type": "roth", "cash": "1000.00", "positions": { "VTI": 0 } }
              }
            }"#,
        );
        write(&prices, r#"{ "VTI": "100.00" }"#);
        write(
            &targets,
            r#"{
              "sleeves": {
                "us": {
                  "target_weight": "1.00",
                  "holdings": { "VTI": "1.00" },
                  "preferred_accounts": ["roth"]
                }
              }
            }"#,
        );

        let loader = JsonStoreLoader {
            positions_path: positions,
            prices_path: prices,
            targets_path: targets,
            output_path: output.clone(),
        };
        let LoadedStore { source, sink } = loader.load().unwrap();
        let out = DefaultEngine::default().rebalance(&*source).unwrap();
        sink.write(&out).unwrap();

        let written: RebalanceOutput = read_json(&output).unwrap();
        assert_eq!(written.accounts["roth"].positions["VTI"].target_shares, 10);
    }
}
