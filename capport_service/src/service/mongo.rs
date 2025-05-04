use serde::{Deserialize, Serialize};

use crate::util::error::CpSvcResult;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MongoClientConfig {
    pub uri: String,
    pub default_db: String,
}

#[derive(Clone)]
pub struct MongoClient {
    pub sync_client: Option<mongodb::sync::Client>,
    pub config: MongoClientConfig,
}
pub trait HasMongoClient {
    fn get_mongo_client(&self, name: Option<&str>) -> Option<MongoClient>;
    fn get_mongo_syncdb(&self, dbname: Option<&str>) -> Option<mongodb::sync::Database>;
}

impl MongoClientConfig {
    pub fn new(uri: &str, default_db: &str) -> MongoClientConfig {
        MongoClientConfig {
            uri: uri.to_string(),
            default_db: default_db.to_string(),
        }
    }
}

impl MongoClient {
    pub fn new(config: MongoClientConfig) -> CpSvcResult<MongoClient> {
        Ok(MongoClient {
            sync_client: Some(mongodb::sync::Client::with_uri_str(&config.uri)?),
            config,
        })
    }
}
