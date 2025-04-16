extern crate proc_macro;
use polars::error::PolarsError;
use proc_macro::TokenStream;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CpError {
    #[error("ERROR [COMPONENT >> {0}]: {1}")]
    ComponentError(&'static str, String),
    #[error("ERROR [PIPELINE >> {0}]: {1}")]
    PipelineError(&'static str, String),
    #[error("ERROR [_raw_]: {0}")]
    RawError(std::io::Error),
}

impl From<std::io::Error> for CpError {
    fn from(value: std::io::Error) -> Self {
        Self::RawError(value)
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

    fn handle() -> CpResult<u8> {
        match will_throw()?.into_iter().reduce(|x, y| x + y) {
            Some(x) => Ok(x),
            None => Ok(0),
        }
    }

    #[test]
    fn cp_err_to_err() {
        assert_eq!(
            handle().unwrap_err().to_string(),
            CpError::RawError(will_throw().unwrap_err()).to_string()
        );
    }
}
