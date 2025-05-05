use crate::service::mongo::{HasMongoClient, MongoClient, MongoClientConfig};
use crate::service::sql::{HasSqlClient, SqlClient, SqlClientConfig};
use crate::util::error::{CpSvcError, CpSvcResult};
use capport_core::{
    context::common::Configurable,
    util::error::{CpError, CpResult},
};
use polars::frame::DataFrame;
use sqlx::PgPool;
use std::collections::HashMap;

#[derive(Default, Debug, Clone)]
pub struct DefaultSvcConfig {
    pub mongo: Option<MongoClientConfig>,
    pub sql: Option<SqlClientConfig>,
}

unsafe impl Send for DefaultSvcDistributor {}
unsafe impl Sync for DefaultSvcDistributor {}

#[derive(Default, Clone)]
pub struct DefaultSvcDistributor {
    pub config: DefaultSvcConfig,
    pub mongo_client: Option<MongoClient>,
    pub sql_client: Option<SqlClient>,
}

impl Configurable for DefaultSvcDistributor {
    fn get_node_name() -> &'static str {
        "service"
    }

    fn extract_parse_config(
        &mut self,
        config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    ) -> CpResult<()> {
        let configs = config_pack
            .remove(DefaultSvcDistributor::get_node_name())
            .unwrap_or_default();
        let mut svc_config = DefaultSvcConfig::default();
        for (config_name, node) in configs {
            match config_name.as_str() {
                "mongo" => {
                    let _ = svc_config.mongo.insert(serde_yaml_ng::from_value(node)?);
                }
                "sql" => {
                    let _ = svc_config.sql.insert(serde_yaml_ng::from_value(node)?);
                }
                _ => {
                    return Err(CpError::ComponentError(
                        "Service not recognized",
                        format!("Received unrecognized service configuration: {}", &config_name),
                    ));
                }
            }
        }
        self.config = svc_config;
        Ok(())
    }
}

impl DefaultSvcDistributor {
    pub fn new() -> Self {
        Self {
            config: DefaultSvcConfig::default(),
            mongo_client: None,
            sql_client: None,
        }
    }
    pub fn setup(&mut self, required_svcs: &[&str]) -> CpResult<()> {
        for &svc in required_svcs {
            match svc {
                "mongo" => match &self.config.mongo {
                    Some(x) => {
                        let _ = self.mongo_client.insert(MongoClient::new(x.clone())?);
                    }
                    None => {
                        return Err(CpError::ComponentError(
                            "Config not found: mongo",
                            "Please check for a node `mongo` under `services`".to_owned(),
                        ));
                    }
                },
                "sql" => match &self.config.sql {
                    Some(config) => {
                        let client = if tokio::runtime::Handle::try_current().is_ok() {
                            tokio::task::block_in_place(move || {
                                tokio::runtime::Handle::current().block_on(SqlClient::new(config.clone()))
                            })?
                        } else {
                            let mut rt_builder = tokio::runtime::Builder::new_current_thread();
                            rt_builder.enable_all();
                            let rt = rt_builder.build()?;
                            rt.block_on(SqlClient::new(config.clone()))?
                        };
                        let _ = self.sql_client.insert(client);
                    }
                    None => {
                        return Err(CpError::ComponentError(
                            "Config not found: sql",
                            "Please check for a node `sql` under `services`".to_owned(),
                        ));
                    }
                },
                s => {
                    return Err(CpError::ComponentError(
                        "Config not recognized",
                        format!("Invalid node `{}` under `services`", s),
                    ));
                }
            };
        }
        Ok(())
    }
}

impl HasMongoClient for DefaultSvcDistributor {
    fn get_mongo_client(&self, _name: Option<&str>) -> Option<MongoClient> {
        self.mongo_client.clone()
    }
    fn get_db_sync(&self, dbname: Option<&str>) -> Option<mongodb::sync::Database> {
        let default_db = &self
            .mongo_client
            .as_ref()
            .map(|mc| mc.config.default_db.as_str())
            .unwrap_or("");
        let db = &self
            .mongo_client
            .as_ref()
            .map(|mc| {
                mc.sync_client
                    .as_ref()
                    .map(|sc| sc.database(dbname.unwrap_or(default_db)))
            })
            .unwrap_or(None);
        db.clone()
    }
}

impl HasSqlClient for DefaultSvcDistributor {
    fn get_sql_client(&self, _name: Option<&str>) -> Option<SqlClient> {
        self.sql_client.clone()
    }
    fn get_pool_connection(&self) -> Option<PgPool> {
        let sql_client = self.sql_client.as_ref();
        sql_client?.get_pool_connection()
    }
    fn read_sql(&self, query: &str) -> CpSvcResult<DataFrame> {
        let sql_client = self.sql_client.as_ref();
        if sql_client.is_none() {
            return Err(CpSvcError::SqlError("no sql client found".to_owned()));
        }

        sql_client.unwrap().read_sql(query)
    }
}
