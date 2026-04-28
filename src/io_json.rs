use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::errors::RebalanceError;

pub fn read_json<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<T, RebalanceError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let value = serde_json::from_reader(reader)?;
    Ok(value)
}

pub fn write_json<T: Serialize>(path: impl AsRef<Path>, value: &T) -> Result<(), RebalanceError> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)?;
    use std::io::Write;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DecimalStr, PricesFile};
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;
    use tempfile::NamedTempFile;

    #[test]
    fn write_then_read_round_trips() {
        let original = PricesFile {
            prices: BTreeMap::from([
                ("VTI".to_string(), DecimalStr(dec!(250.00))),
                ("BND".to_string(), DecimalStr(dec!(75.00))),
            ]),
        };
        let tmp = NamedTempFile::new().unwrap();
        write_json(tmp.path(), &original).unwrap();
        let read_back: PricesFile = read_json(tmp.path()).unwrap();
        assert_eq!(read_back, original);
    }

    #[test]
    fn read_missing_file_errors() {
        let err = read_json::<PricesFile>("/definitely/not/a/real/path.json").unwrap_err();
        assert!(matches!(err, RebalanceError::Io(_)));
    }

    #[test]
    fn read_invalid_json_errors() {
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"{not json").unwrap();
        let err = read_json::<PricesFile>(tmp.path()).unwrap_err();
        assert!(matches!(err, RebalanceError::Json(_)));
    }
}
