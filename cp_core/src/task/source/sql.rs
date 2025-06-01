use async_trait::async_trait;
use connectorx::prelude::{CXQuery, SourceConn, get_arrow};
use polars::prelude::{IntoLazy, LazyFrame};
use std::sync::Arc;

use crate::{
    pipeline::context::DefaultPipelineContext,
    util::error::{CpError, CpResult},
};

use super::common::Source;

pub struct SqlSource {
    uri: String,
    queries: Vec<CXQuery>,
    output: String,
}

#[async_trait]
impl Source for SqlSource {
    fn connection_type(&self) -> &str {
        "sql"
    }
    fn name(&self) -> &str {
        self.output.as_str()
    }
    fn run(&self, _ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        let source_conn = match SourceConn::try_from(self.uri.as_str()) {
            Ok(x) => x,
            Err(e) => {
                return Err(CpError::ConnectionError(format!(
                    "Failed to parse sql database uri {}: {}",
                    self.uri.as_str(),
                    e
                )));
            }
        };
        let destination = get_arrow(&source_conn, None, self.queries.as_slice(), None).expect("run failed");
        match destination.polars() {
            Ok(x) => Ok(x.lazy()),
            Err(e) => Err(CpError::TaskError("Failed to parse SqlSource result", e.to_string())),
        }
    }
    async fn fetch(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        self.run(ctx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use connectorx::prelude::CXQuery;

    use crate::{
        pipeline::context::DefaultPipelineContext,
        task::source::common::Source,
    };

    use super::SqlSource;

    #[test]
    fn valid_sql_src_postgres() {
        let sql = SqlSource {
            uri: "postgres://defuser:password@localhost:5432/postgres".to_string(),
            output: "test".to_string(),
            queries: vec![CXQuery::naked("select now()")],
        };
        let ctx = Arc::new(DefaultPipelineContext::new());
        let val = sql.run(ctx.clone()).unwrap();
        let actual = val.collect().unwrap();
        assert_eq!(actual.shape(), (1, 1));
    }

    #[test]
    fn valid_sql_src_mysql() {
        let sql = SqlSource {
            uri: "mysql://root:rsecret@localhost:3306/".to_string(),
            output: "test".to_string(),
            queries: vec![CXQuery::naked("show databases;")],
        };
        let ctx = Arc::new(DefaultPipelineContext::new());
        let val = sql.run(ctx.clone()).unwrap();
        let actual = val.collect().unwrap();
        assert_eq!(actual.shape().1, 1);
    }
}
