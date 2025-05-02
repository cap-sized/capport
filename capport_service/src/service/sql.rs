use crate::util::error::CpSvcResult;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SqlClientConfig {
    pub uri: String,
    pub max_connections: u32,
    pub min_connections: u32,
}

#[derive(Clone)]
pub struct SqlClient {
    pub pool: Option<PgPool>,
    pub config: SqlClientConfig,
}

pub trait HasSqlClient {
    fn get_sql_client(&self, name: Option<&str>) -> Option<SqlClient>;
    fn get_pool(&self) -> Option<PgPool>;
}

impl SqlClientConfig {
    pub fn new(uri: &str, max_connections: u32, min_connections: u32) -> SqlClientConfig {
        SqlClientConfig {
            uri: uri.to_string(),
            max_connections,
            min_connections,
        }
    }
}

impl SqlClient {
    pub async fn new(config: SqlClientConfig) -> CpSvcResult<SqlClient> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .connect(&config.uri)
            .await?;

        Ok(SqlClient {
            pool: Some(pool),
            config,
        })
    }

    pub fn get_pool(&self) -> Option<PgPool> {
        self.pool.clone()
    }
}
