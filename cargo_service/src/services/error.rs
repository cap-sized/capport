use thiserror::Error;

#[derive(Error, Debug)]
pub enum CpError {
    #[error("ERROR [MongoDB]: {0}")]
    MongoError(String),
    #[error("ERROR [serde_json]: {0}")]
    JsonError(String),
    #[error("ERROR [json_to_bson]: {0}")]
    JsonToBsonError(String),
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

pub type CpResult<T, E = CpError> = std::result::Result<T, E>;
