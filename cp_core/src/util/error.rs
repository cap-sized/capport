extern crate proc_macro;
use polars::error::PolarsError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CpError {
    #[error("ERROR [COMPONENT >> {0}]: {1}")]
    ComponentError(&'static str, String),
    #[error("ERROR [CONFIG >> {0}]: {1}")]
    ConfigError(&'static str, String),
    #[error("ERROR [PIPELINE >> {0}]: {1}")]
    PipelineError(&'static str, String),
    #[error("ERROR [TASK >> {0}]: {1}")]
    TaskError(&'static str, String),
    #[error("ERROR [POLARS]: {0}")]
    PolarsError(PolarsError),
    #[error("ERROR [POISON]: {0}")]
    PoisonError(String),
    #[error("ERROR [CONNECTION]: {0}")]
    ConnectionError(String),
    #[error("ERROR [_raw_]: {0}")]
    RawError(std::io::Error),
}

impl From<polars::error::PolarsError> for CpError {
    fn from(value: polars::error::PolarsError) -> Self {
        Self::PolarsError(value)
    }
}

impl From<serde_yaml_ng::Error> for CpError {
    fn from(value: serde_yaml_ng::Error) -> Self {
        Self::ConfigError("YML Parsing Error", value.to_string())
    }
}

impl From<std::io::Error> for CpError {
    fn from(value: std::io::Error) -> Self {
        Self::RawError(value)
    }
}

impl<T> From<std::sync::PoisonError<T>> for CpError {
    fn from(value: std::sync::PoisonError<T>) -> Self {
        Self::PoisonError(value.to_string())
    }
}

pub type CpResult<T, E = CpError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::{CpError, CpResult};

    fn cp_err() -> Result<Vec<u8>, std::io::Error> {
        Err(std::io::Error::other("default"))
    }

    fn handle_cp_err() -> CpResult<()> {
        cp_err()?;
        Ok(())
    }

    fn yaml_err() -> Result<serde_yaml_ng::Value, serde_yaml_ng::Error> {
        serde_yaml_ng::from_str("@ [] {}")
    }

    fn handle_yaml_err() -> CpResult<()> {
        yaml_err()?;
        Ok(())
    }

    fn poison_err() -> Result<i64, std::sync::PoisonError<i64>> {
        Err(std::sync::PoisonError::new(1))
    }

    fn handle_poison_err() -> CpResult<()> {
        poison_err()?;
        Ok(())
    }

    #[test]
    fn cp_err_to_err() {
        assert_eq!(
            handle_cp_err().unwrap_err().to_string(),
            CpError::RawError(cp_err().unwrap_err()).to_string()
        );
    }

    #[test]
    fn poison_err_to_err() {
        assert_eq!(
            handle_poison_err().unwrap_err().to_string(),
            CpError::PoisonError(poison_err().unwrap_err().to_string()).to_string()
        );
    }

    #[test]
    fn yaml_err_to_err() {
        assert_eq!(
            handle_yaml_err().unwrap_err().to_string(),
            CpError::ConfigError("YML Parsing Error", yaml_err().unwrap_err().to_string()).to_string()
        );
    }
}
