extern crate proc_macro;
use polars::error::PolarsError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CpError {
    #[error("ERROR [COMPONENT >> {0}]: {1}")]
    ComponentError(&'static str, String),
    #[error("ERROR [PIPELINE >> {0}]: {1}")]
    PipelineError(&'static str, String),
    #[error("ERROR [TASK >> {0}]: {1}")]
    TaskError(&'static str, String),
    #[error("ERROR [TABLE]: {0}")]
    TableError(PolarsError),
    #[error("ERROR [POISON]: {0}")]
    PoisonError(String),
    #[error("ERROR [CONNECTION]: {0}")]
    ConnectionError(String),
    #[error("ERROR [_raw_]: {0}")]
    RawError(std::io::Error),
}

impl From<PolarsError> for CpError {
    fn from(value: PolarsError) -> Self {
        Self::TableError(value)
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
pub type SubResult<T, E = String> = std::result::Result<T, E>;
pub type PlResult<T, E = PolarsError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::{CpError, CpResult};

    fn will_throw() -> Result<Vec<u8>, std::io::Error> {
        Err(std::io::Error::other("default"))
    }

    fn handle() -> CpResult<()> {
        will_throw()?;
        Ok(())
    }

    #[test]
    fn cp_err_to_err() {
        assert_eq!(
            handle().unwrap_err().to_string(),
            CpError::RawError(will_throw().unwrap_err()).to_string()
        );
    }
}
