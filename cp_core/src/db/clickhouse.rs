use crate::{model::common::ModelConfig, parser::connection::NetworkConnection, util::error::CpResult};


pub struct ClickhouseClient {
    pool: clickhouse_rs::pool::Pool,
    db_name: String,
}

impl ClickhouseClient {
    pub fn new(network: NetworkConnection) -> ClickhouseClient {
        ClickhouseClient { pool: clickhouse_rs::pool::Pool::new(network.to_uri()), db_name: network.db_name }
    }

    pub fn get_model_query(&self, model: ModelConfig, table: &str, extra_clauses: &str) -> CpResult<String> {
        let schema = model.schema().unwrap();
        let columns = schema.iter_names().map(|x| x.as_str()).collect::<Vec<&str>>();
        let col_list = columns.join(", ");
        Ok(format!("SELECT {} FROM {}.{} FINAL {}", col_list, &self.db_name, table, extra_clauses))
    }
}
