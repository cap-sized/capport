use async_trait::async_trait;
use connectorx::prelude::{CXQuery, SourceConn, get_arrow};
use polars::prelude::{Expr, IntoLazy, LazyFrame};
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
    strict: bool,
    schema: Option<Vec<Expr>>,
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
        let lf = match destination.polars() {
            Ok(x) => x.lazy(),
            Err(e) => {
                return Err(CpError::TaskError("Failed to parse SqlSource result", e.to_string()));
            }
        };
        let frame_modelled = if let Some(schema) = &self.schema {
            if self.strict {
                lf.select(schema.clone())
            } else {
                lf.with_columns(schema.clone())
            }
        } else {
            lf
        };
        Ok(frame_modelled)
    }
    async fn fetch(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        self.run(ctx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use connectorx::prelude::CXQuery;
    use polars::prelude::{DataType, TimeUnit, col};

    use crate::{pipeline::context::DefaultPipelineContext, task::source::common::Source};

    use super::SqlSource;

    #[test]
    fn valid_sql_src_postgres_nostrict() {
        let dtype = DataType::Datetime(TimeUnit::Milliseconds, Some("utc".into()));
        let sql = SqlSource {
            uri: "postgres://defuser:password@localhost:5432/postgres".to_string(),
            output: "TEST".to_string(),
            queries: vec![CXQuery::naked("select now()")],
            strict: false,
            schema: Some(vec![col("now").cast(dtype.clone()).alias("time")]),
        };
        let ctx = Arc::new(DefaultPipelineContext::new());
        let val = sql.run(ctx.clone()).unwrap();
        let actual = val.collect().unwrap();
        assert_eq!(actual.shape(), (1, 2));
        assert_eq!(actual.column("time").unwrap().dtype(), &dtype);
        assert!(actual.column("now").is_ok());
    }

    #[test]
    fn valid_sql_src_postgres_strict() {
        let dtype = DataType::Datetime(TimeUnit::Milliseconds, Some("utc".into()));
        let sql = SqlSource {
            uri: "postgres://defuser:password@localhost:5432/postgres".to_string(),
            output: "TEST".to_string(),
            queries: vec![CXQuery::naked("select now()")],
            strict: true,
            schema: Some(vec![col("now").cast(dtype.clone()).alias("time")]),
        };
        let ctx = Arc::new(DefaultPipelineContext::new());
        let val = sql.run(ctx.clone()).unwrap();
        let actual = val.collect().unwrap();
        assert_eq!(actual.shape(), (1, 1));
        assert_eq!(actual.column("time").unwrap().dtype(), &dtype);
        assert_eq!(actual.column("time").unwrap().name().as_str(), "time");
    }

    #[test]
    fn valid_sql_src_mysql() {
        let sql = SqlSource {
            uri: "mysql://root:rsecret@localhost:3306/".to_string(),
            output: "TEST".to_string(),
            queries: vec![CXQuery::naked("show databases;")],
            strict: false,
            schema: None,
        };
        let ctx = Arc::new(DefaultPipelineContext::new());
        let val = sql.run(ctx.clone()).unwrap();
        let actual = val.collect().unwrap();
        assert_eq!(actual.shape().1, 1);
    }
}
