extern crate proc_macro;
use proc_macro::TokenStream;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CpError {
    #[error("ERROR [{0}]: {1}")]
    ComponentError(&'static str, String),
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
