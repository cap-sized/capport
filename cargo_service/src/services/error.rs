use thiserror::Error;

#[derive(Error, Debug)]
pub enum CpError {
    #[error("ERROR [MongoDB]: {0}")]
    MongoError(String),
    #[error("ERROR [serde_json]: {0}")]
    JsonError(String),
    #[error("ERROR [json_to_bson]: {0}")]
    JsonToBsonError(String),
    #[error("ERROR [PolarsError]: {0}")]
    PolarsError(String),
    #[error("{0}")]
    CoreError(String),
}

impl From<mongodb::error::Error> for CpError {
    fn from(value: mongodb::error::Error) -> Self {
        Self::MongoError(value.to_string())
    }
}

impl From<serde_json::error::Error> for CpError {
    fn from(value: serde_json::error::Error) -> Self {
        Self::JsonError(value.to_string())
    }
}

impl From<bson::extjson::de::Error> for CpError {
    fn from(value: bson::extjson::de::Error) -> Self {
        Self::JsonToBsonError(value.to_string())
    }
}

impl From<polars::error::PolarsError> for CpError {
    fn from(value: polars::error::PolarsError) -> Self {
        Self::PolarsError(value.to_string())
    }
}

impl From<capport_core::util::error::CpError> for CpError {
    fn from(value: capport_core::util::error::CpError) -> Self {
        Self::CoreError(value.to_string())
    }
}

pub type CpResult<T, E = CpError> = std::result::Result<T, E>;
