use async_trait::async_trait;
use connectorx::prelude::{CXQuery, SourceConn, get_arrow};
use polars::prelude::{Expr, IntoLazy, LazyFrame};
use std::sync::Arc;

use crate::{
    db_url_emplace,
    model::common::ModelConfig,
    model_emplace,
    parser::keyword::{Keyword, StrKeyword},
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    util::error::{CpError, CpResult},
    valid_or_insert_error,
};

use super::{
    common::{Source, SourceConfig},
    config::{MySqlSourceConfig, PostgresSourceConfig},
};

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
        let destination = match get_arrow(&source_conn, None, self.queries.as_slice(), None) {
            Ok(x) => x,
            Err(e) => {
                return Err(CpError::ConnectionError(format!(
                    "SqlSource {} failed: {}",
                    self.uri.as_str(),
                    e
                )));
            }
        };
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

impl SourceConfig for MySqlSourceConfig {
    fn emplace(&mut self, ctx: &DefaultPipelineContext, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        db_url_emplace!(self.mysql, ctx, context, "mysql://{}");
        self.mysql.output.insert_value_from_context(context)?;
        if let Some(mut sql) = self.mysql.sql.take() {
            sql.insert_value_from_context(context)?;
            let _ = self.mysql.sql.insert(sql);
        }
        model_emplace!(self.mysql, ctx, context);
        Ok(())
    }
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        // Only url has to be inserted
        valid_or_insert_error!(errors, self.mysql.url.as_ref().unwrap(), "source[mysql].url");
        valid_or_insert_error!(errors, self.mysql.output, "source[mysql].output");
        if let Some(model_fields) = &self.mysql.model_fields {
            for (key_kw, field_kw) in model_fields {
                valid_or_insert_error!(errors, key_kw, "source[mysql].model.key");
                valid_or_insert_error!(errors, field_kw, "source[mysql].model.field");
            }
        }
        errors
    }
    fn transform(&self) -> Box<dyn Source> {
        let mut queries = vec![];
        let query = self.mysql.sql.clone().unwrap_or_else(|| {
            let selector = if self.mysql.strict.unwrap_or(false) && self.mysql.model_fields.is_some() {
                let vals = self
                    .mysql
                    .model_fields
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|mf| mf.0.value().expect("source[mysql].model_fields").as_str())
                    .collect::<Vec<_>>();
                vals.join(", ")
            } else {
                "*".to_owned()
            };
            StrKeyword::with_value(format!(
                "SELECT {} from {}",
                selector,
                self.mysql.table.value().expect("source[mysql].table")
            ))
        });
        queries.push(CXQuery::from(query.value().expect("source[mysql].sql").as_str()));
        let schema = self.mysql.model_fields.as_ref().map(|x| {
            ModelConfig {
                label: "".to_string(),
                fields: x.clone(),
            }
            .columns()
            .expect("failed to build schema")
        });
        Box::new(SqlSource {
            output: self.mysql.output.value().expect("source[mysql].output").to_string(),
            uri: self
                .mysql
                .url
                .as_ref()
                .map(|x| x.value().unwrap())
                .expect("source[mysql].uri(val)")
                .to_string(),
            queries,
            schema,
            strict: self.mysql.strict.unwrap_or(false),
        })
    }
}

impl SourceConfig for PostgresSourceConfig {
    fn emplace(&mut self, ctx: &DefaultPipelineContext, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        db_url_emplace!(self.postgres, ctx, context, "postgres://{}");
        self.postgres.output.insert_value_from_context(context)?;
        if let Some(mut sql) = self.postgres.sql.take() {
            sql.insert_value_from_context(context)?;
            let _ = self.postgres.sql.insert(sql);
        }
        model_emplace!(self.postgres, ctx, context);
        Ok(())
    }
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        // Only url has to be inserted
        valid_or_insert_error!(errors, self.postgres.url.as_ref().unwrap(), "source[postgres].url");
        valid_or_insert_error!(errors, self.postgres.output, "source[postgres].output");
        if let Some(model_fields) = &self.postgres.model_fields {
            for (key_kw, field_kw) in model_fields {
                valid_or_insert_error!(errors, key_kw, "source[postgres].model.key");
                valid_or_insert_error!(errors, field_kw, "source[postgres].model.field");
            }
        }
        errors
    }
    fn transform(&self) -> Box<dyn Source> {
        let mut queries = vec![];
        let query = self.postgres.sql.clone().unwrap_or_else(|| {
            let selector = if self.postgres.strict.unwrap_or(false) && self.postgres.model_fields.is_some() {
                let vals = self
                    .postgres
                    .model_fields
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|mf| mf.0.value().expect("source[postgres].model_fields").as_str())
                    .collect::<Vec<_>>();
                vals.join(", ")
            } else {
                "*".to_owned()
            };
            StrKeyword::with_value(format!(
                "SELECT {} from {}",
                selector,
                self.postgres.table.value().expect("source[postgres].table")
            ))
        });
        queries.push(CXQuery::from(query.value().expect("source[postgres].sql").as_str()));
        let schema = self.postgres.model_fields.as_ref().map(|x| {
            ModelConfig {
                label: "".to_string(),
                fields: x.clone(),
            }
            .columns()
            .expect("failed to build schema")
        });
        Box::new(SqlSource {
            output: self
                .postgres
                .output
                .value()
                .expect("source[postgres].output")
                .to_string(),
            uri: self
                .postgres
                .url
                .as_ref()
                .map(|x| x.value().unwrap())
                .expect("source[postgres].uri(val)")
                .to_string(),
            queries,
            schema,
            strict: self.postgres.strict.unwrap_or(false),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use connectorx::prelude::CXQuery;
    use polars::prelude::{DataType, IntoLazy, TimeUnit, col};

    use crate::{
        context::{connection::ConnectionRegistry, envvar::EnvironmentVariableRegistry, model::ModelRegistry},
        model::common::{ModelConfig, ModelFields},
        parser::keyword::{Keyword, StrKeyword},
        pipeline::context::DefaultPipelineContext,
        task::source::{
            common::{Source, SourceConfig},
            config::{MySqlSourceConfig, PostgresSourceConfig, SqlConnection},
        },
        util::{common::create_config_pack, test::tests::DbTools},
    };

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
            uri: "mysql://root:root@localhost:3306/".to_string(),
            output: "TEST".to_string(),
            queries: vec![CXQuery::from("show databases")],
            strict: false,
            schema: None,
        };
        let ctx = Arc::new(DefaultPipelineContext::new());
        let val = sql.run(ctx.clone()).unwrap();
        let actual = val.collect().unwrap();
        assert_eq!(actual.shape().1, 1);
    }

    #[test]
    fn valid_pg_src_config_to_sql_src_fully_qualified_values() {
        let expected = DbTools::populate_pg_person("defuser", "password", "person");
        let mut config = PostgresSourceConfig {
            postgres: SqlConnection {
                sql: Some(StrKeyword::with_symbol("query")),
                model_fields: Some(serde_yaml_ng::from_str::<ModelFields>("name: str").unwrap()),
                // this is irrelevant
                table: StrKeyword::with_value("table".to_owned()),
                strict: Some(true),
                output: StrKeyword::with_symbol("output"),
                url: Some(StrKeyword::with_symbol("url")),
                env_connection: None,
                model: None,
            },
        };
        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>(
            "
output: TEST
query: SELECT id, name, data FROM person
url: postgres://defuser:password@localhost:5432/defuser
            ",
        )
        .unwrap();
        let ctx = DefaultPipelineContext::with_results(&["TEST"], 1);
        config.emplace(&ctx, &context).unwrap();
        config.validate();
        let sqlsrc = config.transform();
        let actx = Arc::new(ctx);
        assert_eq!(sqlsrc.connection_type(), "sql");
        assert_eq!(sqlsrc.name(), "TEST");
        let lf = sqlsrc.run(actx.clone()).unwrap();
        let exp_name_col = expected.lazy().select([col("name")]).collect().unwrap();
        assert_eq!(lf.select([col("name")]).collect().unwrap(), exp_name_col);
        DbTools::drop_pg("defuser", "password", "person");
    }

    #[test]
    fn valid_pg_src_config_to_sql_src_model_env() {
        let expected = DbTools::populate_pg_person("defuser", "password", "person2");
        let mut config = PostgresSourceConfig {
            postgres: SqlConnection {
                sql: Some(StrKeyword::with_value("SELECT id, name, data FROM person2".to_owned())),
                model_fields: None,
                table: StrKeyword::with_symbol("lookup"),
                strict: Some(false),
                output: StrKeyword::with_value("TEST".to_owned()),
                url: Some(StrKeyword::with_value(
                    "postgres://defuser:password@localhost:5432/defuser".to_owned(),
                )),
                env_connection: None,
                model: Some(StrKeyword::with_symbol("model")),
            },
        };
        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>(
            "
lookup: table
model: person
            ",
        )
        .unwrap();
        let mut model_registry = ModelRegistry::default();
        model_registry.insert(ModelConfig {
            label: "person".to_owned(),
            fields: serde_yaml_ng::from_str::<ModelFields>("{name: str, id: uint64}").unwrap(),
        });
        let ctx = DefaultPipelineContext::with_results(&["TEST"], 1).with_model_registry(model_registry);
        config.emplace(&ctx, &context).unwrap();
        config.validate();
        let sqlsrc = config.transform();
        let actx = Arc::new(ctx);
        assert_eq!(sqlsrc.connection_type(), "sql");
        assert_eq!(sqlsrc.name(), "TEST");
        let lf = sqlsrc.run(actx.clone()).unwrap();
        let df = lf.clone().collect().unwrap();
        let name_col = lf.clone().select([col("name")]).collect().unwrap();
        let id_col = df.column("id").unwrap();
        assert_eq!(name_col, expected.lazy().select([col("name")]).collect().unwrap());
        assert_eq!(id_col.dtype(), &DataType::UInt64);
        DbTools::drop_pg("defuser", "password", "person2");
    }

    #[test]
    fn valid_my_src_config_to_sql_src_no_query() {
        let mut env_var = EnvironmentVariableRegistry::new();
        env_var.set_str("MYSQL_PW", "dev".to_owned()).unwrap();
        env_var.set_str("MYSQL_HOST", "localhost".to_owned()).unwrap();
        env_var.set_str("MYSQL_USER", "dev".to_owned()).unwrap();
        env_var.set_str("MYSQL_DB", "dev".to_owned()).unwrap();
        let expected = DbTools::populate_my_accounts("dev", "dev", "payments", 3306);
        let mut config = MySqlSourceConfig {
            mysql: SqlConnection {
                sql: None,
                model_fields: Some(serde_yaml_ng::from_str::<ModelFields>("{id: uint32, amt: uint8}").unwrap()),
                table: StrKeyword::with_value("payments".to_owned()),
                strict: Some(false),
                output: StrKeyword::with_symbol("output"),
                url: None,
                env_connection: Some(StrKeyword::with_symbol("mysql_conn")),
                model: None,
            },
        };
        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>(
            "
output: TEST
mysql_conn: mysql
            ",
        )
        .unwrap();
        let mut config_packs = create_config_pack([r#"
connection:
    mysql:
        password_env: MYSQL_PW
        host_env: MYSQL_HOST
        port: 3306
        user_env: MYSQL_USER
        db_env: MYSQL_DB
                "#]);
        let connection_registry = ConnectionRegistry::from(&mut config_packs).unwrap();
        let ctx = DefaultPipelineContext::with_results(&["test"], 1).with_connection_registry(connection_registry);

        config.emplace(&ctx, &context).unwrap();
        config.validate();
        let sqlsrc = config.transform();
        let actx = Arc::new(ctx);
        assert_eq!(sqlsrc.connection_type(), "sql");
        assert_eq!(sqlsrc.name(), "TEST");
        let df = sqlsrc.run(actx.clone()).unwrap().collect().unwrap();
        assert_eq!(df.column("id").unwrap().dtype(), &DataType::UInt32);
        assert_eq!(df.column("amt").unwrap().dtype(), &DataType::UInt8);
        assert_eq!(df, expected);
        DbTools::drop_my("dev", "dev", "payments", 3306);
    }
}
