use std::sync::Arc;

use async_trait::async_trait;
use connectorx::prelude::CXQuery;
use polars::{frame::DataFrame, prelude::Expr};
use postgres::{tls::TlsConnect, NoTls};
use reqwest::tls::TlsInfo;

use crate::{parser::merge_type::MergeTypeEnum, pipeline::context::DefaultPipelineContext, util::error::CpResult};

use super::common::Sink;


pub struct PostgresSqlSink {
    params: String,
    merge_type: MergeTypeEnum,
    query: Vec<CXQuery>,
    schema: Vec<Expr>,
}

#[async_trait]
impl Sink for PostgresSqlSink {
    fn connection_type(&self) -> &str {
        "postgres_sql"
    }
    fn run(&self, _df: DataFrame, _ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        todo!()
    }
    async fn fetch(&self, _df: DataFrame, _ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        todo!()
    }
}


#[cfg(test)]
mod tests {
    use polars::{df, prelude::CompatLevel};
    use postgres::NoTls;

    use crate::util::test::tests::DbTools;

    #[test]
    fn test_insert_wtf_is_this() {
        let expected = DbTools::populate_pg_person("defuser", "password", "person3");
        let mut client = postgres::Client::connect("host=localhost user=defuser password=password dbname=person3", NoTls).unwrap();
        let dd = df!(
            "id" => [1, 2, 3, 4, 5],
            "name" => ["why", "am", "i", "doing", "this"],
        ).unwrap();
        let id = dd.column("id").unwrap().as_series().unwrap();
        let name = dd.column("name").unwrap().as_series().unwrap();
        let idarr = id.to_arrow(0, CompatLevel::newest());
        let nmarr = name.to_arrow(0, CompatLevel::newest());
        println!("{:?}", idarr);

        client.execute("
        INSERT INTO person2 (id, name)
            SELECT * FROM unnest (
                $1::int[],
                $2::text[],
            )
            ", 
            &[&idarr, &nmarr]
        );

    }

}
