use thiserror::Error;

#[derive(Error, Debug)]
pub enum CpSvcError {
    #[error("ERROR [MongoDB]: {0}")]
    MongoError(String),
    #[error("ERROR [json_to_bson]: {0}")]
    BsonDeserializeError(String),
    #[error("ERROR [PolarsError]: {0}")]
    PolarsError(String),
    #[error("{0}")]
    CoreError(String),
}

impl From<mongodb::error::Error> for CpSvcError {
    fn from(value: mongodb::error::Error) -> Self {
        Self::MongoError(value.to_string())
    }
}

impl From<bson::extjson::de::Error> for CpSvcError {
    fn from(value: bson::extjson::de::Error) -> Self {
        Self::BsonDeserializeError(value.to_string())
    }
}

impl From<polars::error::PolarsError> for CpSvcError {
    fn from(value: polars::error::PolarsError) -> Self {
        Self::PolarsError(value.to_string())
    }
}

impl From<capport_core::util::error::CpError> for CpSvcError {
    fn from(value: capport_core::util::error::CpError) -> Self {
        Self::CoreError(value.to_string())
    }
}

impl From<CpSvcError> for capport_core::util::error::CpError {
    fn from(value: CpSvcError) -> Self {
        capport_core::util::error::CpError::ComponentError("SvcError", value.to_string())
    }
}

pub type CpSvcResult<T, E = CpSvcError> = std::result::Result<T, E>;
