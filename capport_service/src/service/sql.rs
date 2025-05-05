use crate::util::error::CpSvcResult;
use connectorx::prelude::*;
use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::convert::TryFrom;

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
    fn get_pool_connection(&self) -> Option<PgPool>;
    fn read_sql(&self, sql: &str) -> CpSvcResult<DataFrame>;
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

    pub fn get_pool_connection(&self) -> Option<PgPool> {
        self.pool.clone()
    }

    pub fn read_sql(&self, query: &str) -> CpSvcResult<DataFrame> {
        let source_conn = SourceConn::try_from(self.config.uri.clone().as_str()).expect("parse conn str failed");
        let destination = get_arrow(&source_conn, None, &[CXQuery::from(query)], None).expect("run failed");
        let data = destination.polars();
        Ok(data?)
    }
}
