use std::{io::Read, sync::Arc};

use async_trait::async_trait;
use inserter_x::clickhouse::ClickhouseInserter;
use polars::{
    frame::DataFrame,
    prelude::{Expr, IntoLazy, col},
};
use reqwest::header::HeaderMap;

use crate::{
    parser::{
        keyword::{Keyword, StrKeyword},
        merge_type::MergeTypeEnum,
    },
    pipeline::context::DefaultPipelineContext,
    util::error::{CpError, CpResult},
    valid_or_insert_error,
};

use super::{
    common::{Sink, SinkConfig},
    config::{ClickhouseSinkConfig, ClickhouseTableOptions},
};

pub struct ClickhouseSink {
    uri: String,
    inserter: ClickhouseInserter,
    strict: bool,
    create_table_if_not_exists: bool,
    columns: Vec<Expr>,
    headers: HeaderMap,
}

impl ClickhouseSink {
    pub fn new(
        uri: &str,
        inserter: ClickhouseInserter,
        strict: bool,
        example_frame: DataFrame,
        create_table_if_not_exists: bool,
    ) -> Self {
        let columns = example_frame
            .get_columns()
            .iter()
            .map(|column| col(column.name().to_owned()).cast(column.dtype().to_owned()))
            .collect();
        let inserter = inserter
            .with_schema_from_cols(example_frame.get_columns())
            .unwrap()
            .build_queries()
            .unwrap();
        Self {
            uri: uri.to_owned(),
            inserter,
            strict,
            create_table_if_not_exists,
            columns,
            headers: HeaderMap::new(),
        }
    }
}

macro_rules! parse_clickhouse_async {
    ($request:expr) => {
        let response = ($request).send().await?;
        if response.status().is_success() {
            log::info!("Success: {}", response.url());
            return Ok(());
        }
        match response.error_for_status() {
            Ok(err) => {
                let ss = err.text().await?;
                return Err(CpError::ConnectionError(ss));
            }
            Err(_) => {}
        }
    };
}

macro_rules! parse_clickhouse_sync {
    ($request:expr) => {
        let response = ($request).send()?;
        if response.status().is_success() {
            log::info!("Success: {}", response.url());
        } else {
            match response.error_for_status() {
                Ok(mut err) => {
                    log::error!("Bad response: {:?}", err);
                    let mut buf = String::new();
                    err.read_to_string(&mut buf).unwrap();
                    return Err(CpError::ConnectionError(buf));
                }
                Err(_) => {}
            }
        }
    };
}

#[async_trait]
impl Sink for ClickhouseSink {
    fn connection_type(&self) -> &str {
        "clickhouse"
    }
    fn run(&self, frame: DataFrame, _ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        let client = reqwest::blocking::Client::new();
        let lf = frame.lazy();
        let final_frame = (if self.strict {
            lf.select(&self.columns)
        } else {
            lf.with_columns(&self.columns)
        })
        .collect()?;
        if self.create_table_if_not_exists {
            let create = self.inserter.get_create_query()?;
            let request = client
                .post(&self.uri)
                .query(&[("query", create)])
                .headers(self.headers.clone())
                .header("Content-Length", 0);
            log::debug!("Table update: {}", create);
            parse_clickhouse_sync!(request);
        }
        let insert = self.inserter.get_insert_query()?;
        log::debug!("Inserting: {}", insert);
        let body = self.inserter.get_arrow_body(&final_frame)?;
        let request = client
            .post(&self.uri)
            .query(&[("query", insert)])
            .headers(self.headers.clone())
            .header("Content-Length", body.len())
            .body(body);
        parse_clickhouse_sync!(request);
        Ok(())
    }
    async fn fetch(&self, frame: DataFrame, _ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        let client = reqwest::Client::new();
        let lf = frame.lazy();
        let final_frame = (if self.strict {
            lf.select(&self.columns)
        } else {
            lf.with_columns(&self.columns)
        })
        .collect()?;
        if self.create_table_if_not_exists {
            let create = self.inserter.get_create_query()?;
            let request = client
                .post(&self.uri)
                .query(&[("query", create)])
                .headers(self.headers.clone())
                .header("Content-Length", 0);
            parse_clickhouse_async!(request);
        }
        let insert = self.inserter.get_insert_query()?;
        let body = self.inserter.get_arrow_body(&final_frame)?;
        let request = client
            .post(&self.uri)
            .query(&[("query", insert)])
            .headers(self.headers.clone())
            .header("Content-Length", body.len())
            .body(body);
        parse_clickhouse_async!(request);
        Ok(())
    }
}

fn from_merge_type(merge_type: MergeTypeEnum) -> (&'static str, &'static str) {
    match merge_type {
        MergeTypeEnum::Replace => ("ReplacingMergeTree", "CREATE OR REPLACE TABLE"),
        MergeTypeEnum::Insert => ("MergeTree", "CREATE TABLE"),
        MergeTypeEnum::MakeNext => ("MergeTree", "CREATE OR REPLACE TABLE"),
    }
}

impl ClickhouseTableOptions {
    fn emplace_vec(old: &mut Vec<StrKeyword>, context: &serde_yaml_ng::Mapping) -> CpResult<Vec<StrKeyword>> {
        old.reverse();
        let mut new_vec = vec![];
        while let Some(mut kw) = old.pop() {
            kw.insert_value_from_context(context)?;
            new_vec.push(kw);
        }
        Ok(new_vec)
    }
    pub fn emplace(&mut self, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        self.order_by = Self::emplace_vec(&mut self.order_by, context)?;
        self.primary_key = Self::emplace_vec(&mut self.primary_key, context)?;
        if let Some(mut not_null) = self.not_null.take() {
            let not_null = Self::emplace_vec(&mut not_null, context)?;
            let _ = self.not_null.insert(not_null);
        }
        if let Some(mut db_name) = self.db_name.take() {
            db_name.insert_value_from_context(context)?;
            let _ = self.db_name.insert(db_name);
        }
        Ok(())
    }
    pub fn validate(&self, errors: &mut Vec<CpError>) {
        for kw in self.order_by.iter() {
            valid_or_insert_error!(errors, kw, "source[clickhouse.options].order_by");
        }
        for kw in self.primary_key.iter() {
            valid_or_insert_error!(errors, kw, "source[clickhouse.options].primary_key");
        }
        if let Some(not_null) = self.not_null.as_ref() {
            for kw in not_null.iter() {
                valid_or_insert_error!(errors, kw, "source[clickhouse.options].not_null");
            }
        }
        if let Some(db_name) = self.db_name.as_ref() {
            valid_or_insert_error!(errors, db_name, "source[clickhouse.options].db_name");
        }
    }
    fn get_keys(keywords: &[StrKeyword]) -> Vec<String> {
        let empty = String::new();
        keywords
            .iter()
            .map(|x| x.value().unwrap_or(&empty).trim())
            .filter(|x| !x.is_empty())
            .map(|x| x.to_owned())
            .collect()
    }
    pub fn get_order_keys(&self) -> Vec<String> {
        Self::get_keys(self.order_by.as_slice())
    }
    pub fn get_primary_keys(&self) -> Vec<String> {
        Self::get_keys(self.primary_key.as_slice())
    }
    pub fn get_not_null(&self) -> Vec<String> {
        if let Some(not_null) = &self.not_null {
            Self::get_keys(not_null.as_slice())
        } else {
            vec![]
        }
    }
}

impl SinkConfig for ClickhouseSinkConfig {
    fn emplace(&mut self, ctx: &DefaultPipelineContext, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        self.clickhouse.emplace(ctx, context, "http://")?;
        if let Some(url) = self.clickhouse.url.take() {
            let mut parts: Vec<&str> = url
                .value()
                .expect("source[clickhouse].url")
                .split_terminator("/")
                .collect();
            if parts.len() > 3 {
                if let Some(db_name) = parts.pop() {
                    if self.options.db_name.is_none() {
                        let _ = self.options.db_name.insert(StrKeyword::with_value(db_name.to_string()));
                    }
                }
            }
            let _ = self.clickhouse.url.insert(StrKeyword::with_value(parts.join("/")));
        };
        self.options.emplace(context)
    }
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        // Url is mandatory
        valid_or_insert_error!(
            errors,
            self.clickhouse
                .url
                .as_ref()
                .expect("source[clickhouse].url not provided or deduced"),
            "source[clickhouse].url"
        );
        if let Some(db_name) = &self.options.db_name {
            valid_or_insert_error!(errors, db_name, "source[clickhouse].db_name");
        }
        if let Some(output) = &self.clickhouse.output {
            log::warn!("Output {:?} ignored in ClickhouseSinkConfig", output);
        }
        if let Some(sql) = &self.clickhouse.sql {
            log::warn!("Sql {:?} ignored in ClickhouseSinkConfig", sql);
        }
        if self.clickhouse.merge_type.is_none() {
            errors.push(CpError::ConfigValidationError(
                "source[clickhouse].merge_type",
                "Missing table merge_type, please declare".to_owned(),
            ));
        }
        // Model is mandatory
        let model_fields = self
            .clickhouse
            .model_fields
            .as_ref()
            .expect("source[clickhouse].model not provided or deduced");
        for (key_kw, field_kw) in model_fields {
            valid_or_insert_error!(errors, key_kw, "source[clickhouse].model.key");
            valid_or_insert_error!(errors, field_kw, "source[clickhouse].model.field");
        }
        self.options.validate(&mut errors);
        errors
    }
    fn transform(&self) -> Box<dyn Sink> {
        let columns = self.clickhouse.columns().expect("could not create columns");
        let schema = self.clickhouse.schema().expect("could not create schema");
        let empty_df = DataFrame::empty_with_schema(&schema);
        let mut ch = ClickhouseInserter::default(self.clickhouse.table.value().expect("source[clickhouse].table"));
        ch = if let Some(merge_type) = self.clickhouse.merge_type {
            let (engine, creator) = from_merge_type(merge_type);
            let tmp = ch.with_engine(engine);
            if self.options.create_table_if_not_exists.unwrap_or(false) {
                tmp.with_create_method(creator)
            } else {
                tmp
            }
        } else {
            ch.with_create_method("CREATE TABLE")
        };
        ch = ch.with_order_by(self.options.get_order_keys());
        ch = ch.with_primary_key(self.options.get_primary_keys());
        ch = ch.with_not_null(self.options.get_not_null());
        ch = if let Some(db_name_kw) = &self.options.db_name {
            ch.with_dbname(db_name_kw.value().expect("source[clickhouse].db_name"))
        } else {
            ch
        };
        Box::new(ClickhouseSink {
            uri: self
                .clickhouse
                .url
                .as_ref()
                .map(|x| x.value().unwrap())
                .expect("source[clickhouse].uri(val)")
                .to_string(),
            inserter: ch
                .with_schema_from_cols(empty_df.get_columns())
                .expect("bad schema (ClickhouseInserter)")
                .build_queries()
                .expect("bad queries (ClickhouseInserter)"),
            columns,
            strict: self.clickhouse.strict.unwrap_or(false),
            create_table_if_not_exists: self.options.create_table_if_not_exists.unwrap_or(false),
            headers: HeaderMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use inserter_x::clickhouse::ClickhouseInserter;
    use polars::{
        frame::DataFrame,
        io::SerReader,
        prelude::{CsvParseOptions, CsvReadOptions},
    };

    use crate::{
        async_st,
        context::{connection::ConnectionRegistry, envvar::EnvironmentVariableRegistry},
        model::common::ModelFields,
        parser::{
            connection::ConnectionConfig,
            keyword::{Keyword, StrKeyword},
            merge_type::MergeTypeEnum,
            sql_connection::SqlConnection,
        },
        pipeline::context::DefaultPipelineContext,
        task::sink::{
            common::{Sink, SinkConfig},
            config::{ClickhouseSinkConfig, ClickhouseTableOptions},
        },
    };

    use super::ClickhouseSink;

    fn get_example_frame() -> DataFrame {
        let my_file = std::path::PathBuf::from_str("../config/canadian_players.csv").unwrap();
        CsvReadOptions::default()
            .with_parse_options(CsvParseOptions::default().with_try_parse_dates(true))
            .try_into_reader_with_file_path(Some(my_file))
            .unwrap()
            .finish()
            .expect("bad frame")
    }

    fn get_model_fields() -> ModelFields {
        serde_yaml_ng::from_str(
            "
id: int32
birthdate: date
full_name: str
birth_city: str
birth_state_province_code: str
            ",
        )
        .unwrap()
    }

    const URL: &str = "http://default:password@localhost:8123/";

    #[test]
    fn valid_clickhouse_sink_nostrict() {
        // fern::Dispatch::new().level(log::LevelFilter::Debug).chain(std::io::stdout()).apply().unwrap();
        let example_frame = get_example_frame();
        let inserter = ClickhouseInserter::default("canadian_nhlers")
            .with_dbname("default")
            .with_engine("ReplacingMergeTree")
            .with_primary_key(vec!["id".to_string()])
            .with_create_method("CREATE OR REPLACE TABLE");
        let ch = ClickhouseSink::new(
            URL,
            inserter,
            false,
            DataFrame::empty_with_schema(&example_frame.schema()),
            true,
        );
        let ctx = Arc::new(DefaultPipelineContext::new());
        ch.run(example_frame.clone(), ctx).unwrap();
        // TODO: check against clickhouse that this is correct
    }

    #[test]
    fn valid_clickhouse_sink_nostrict_async() {
        async_st!(async || {
            let example_frame = get_example_frame();
            let inserter = ClickhouseInserter::default("canadian_nhlers")
                .with_dbname("default")
                .with_engine("ReplacingMergeTree")
                .with_primary_key(vec!["id".to_string()])
                .with_create_method("CREATE OR REPLACE TABLE");
            let ch = ClickhouseSink::new(
                URL,
                inserter,
                false,
                DataFrame::empty_with_schema(&example_frame.schema()),
                true,
            );
            let ctx = Arc::new(DefaultPipelineContext::new());
            ch.fetch(example_frame.clone(), ctx).await.unwrap();
        });
    }

    #[test]
    fn valid_clickhouse_sink_strict() {
        // fern::Dispatch::new().level(log::LevelFilter::Debug).chain(std::io::stdout()).apply().unwrap();
        let example_frame = get_example_frame();
        let inserter = ClickhouseInserter::default("canadian_nhlers_narrow")
            .with_dbname("default")
            .with_engine("ReplacingMergeTree")
            .with_primary_key(vec!["id".to_string()])
            .with_create_method("CREATE OR REPLACE TABLE");
        let narrow = example_frame.schema().filter(|_, dtype| !dtype.is_string());
        let ch = ClickhouseSink::new(URL, inserter, true, DataFrame::empty_with_schema(&narrow), true);
        let ctx = Arc::new(DefaultPipelineContext::new());
        ch.run(example_frame, ctx).unwrap();
        // TODO: check against clickhouse that this is correct
    }

    #[test]
    fn valid_clickhouse_sink_cfg_to_clickhouse_sink_exec_mode_off() {
        let cfg = ClickhouseSinkConfig {
            clickhouse: SqlConnection {
                url: Some(StrKeyword::with_value(URL.to_owned())),
                sql: None,
                table: StrKeyword::with_value("canadian_nhlers_no_exec".to_owned()),
                model: None,
                model_fields: Some(get_model_fields()),
                env_connection: None,
                merge_type: Some(MergeTypeEnum::Replace),
                output: None,
                strict: Some(true),
            },
            options: ClickhouseTableOptions {
                db_name: None,
                order_by: vec![],
                primary_key: vec![StrKeyword::with_value("id".to_owned())],
                not_null: None,
                create_table_if_not_exists: Some(true),
            },
        };
        let ctx = Arc::new(DefaultPipelineContext::new().with_executing_sink(false));
        let errors = cfg.validate();
        assert!(errors.is_empty());
        let actual_node = cfg.transform();
        actual_node.run(get_example_frame(), ctx.clone()).unwrap();
        // TODO: check against clickhouse that nothing was pushed
    }

    #[test]
    fn invalid_clickhouse_configs() {
        let cfg = "
clickhouse:
    url: http://default:password@localhost:8123/
    table: canadian_badconf
    model_fields:
        id: uint64
        birthdate: date
        birth_state_province_code: str
    {merge_type}
options:
    order_by: [test]
    primary_key: []
";
        let bad_cfg =
            serde_yaml_ng::from_str::<ClickhouseSinkConfig>(cfg.replace("{merge_type}", "").as_str()).unwrap();
        let bad_errors = bad_cfg.validate();
        assert_eq!(bad_errors.len(), 1);
        let good_cfg =
            serde_yaml_ng::from_str::<ClickhouseSinkConfig>(cfg.replace("{merge_type}", "merge_type: insert").as_str())
                .unwrap();
        let good_errors = good_cfg.validate();
        assert!(good_errors.is_empty());
    }

    fn get_node(
        table: &str,
        merge_type: MergeTypeEnum,
    ) -> (Arc<DefaultPipelineContext>, serde_yaml_ng::Mapping, Box<dyn Sink>) {
        let env_key_pw = format!("CLICKHOUSE_SINK_PW_{}", table);
        let env_key_user = format!("CLICKHOUSE_SINK_USER_{}", table);
        let env_key_db = format!("CLICKHOUSE_SINK_DB_{}", table);
        let mut env_var = EnvironmentVariableRegistry::new();
        env_var.set_str(&env_key_pw, "password".to_owned()).unwrap();
        env_var.set_str(&env_key_user, "default".to_owned()).unwrap();
        env_var.set_str(&env_key_db, "default".to_owned()).unwrap();
        let mut connection_registry = ConnectionRegistry::new();
        connection_registry.insert(ConnectionConfig {
            label: "ch".to_owned(),
            port: Some(8123),
            db_env: Some(env_key_db),
            host_env: None,
            user_env: Some(env_key_user),
            password_env: Some(env_key_pw),
        });
        let mut cfg = ClickhouseSinkConfig {
            clickhouse: SqlConnection {
                url: None,
                sql: None,
                table: StrKeyword::with_symbol("table"),
                model: None,
                model_fields: Some(get_model_fields()),
                output: None,
                merge_type: Some(merge_type),
                env_connection: Some(StrKeyword::with_value("ch".to_owned())),
                strict: Some(true),
            },
            options: ClickhouseTableOptions {
                db_name: None,
                order_by: vec![],
                primary_key: vec![StrKeyword::with_value("id".to_owned())],
                not_null: Some(vec![StrKeyword::with_value("birthdate".to_owned())]),
                create_table_if_not_exists: Some(true),
            },
        };
        let ctx = Arc::new(
            DefaultPipelineContext::new()
                .with_executing_sink(true)
                .with_connection_registry(connection_registry),
        );
        let config = format!("table: {}", table);
        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>(config.as_str()).unwrap();
        let _ = cfg.emplace(&ctx, &context);
        let errors = cfg.validate();
        assert!(errors.is_empty());
        (ctx, context, cfg.transform())
    }

    #[test]
    fn valid_clickhouse_sink_config_to_clickhouse_sink_replace() {
        let frame = get_example_frame();
        let (ctx, _, actual_node) = get_node("canadian_replace", MergeTypeEnum::Replace);
        actual_node.run(frame.clone(), ctx.clone()).unwrap();
        actual_node.run(frame.clone(), ctx.clone()).unwrap();
    }

    #[test]
    fn valid_clickhouse_sink_config_to_clickhouse_sink_insert() {
        let frame = get_example_frame();
        let (ctx, _, actual_node) = get_node("canadian_insert", MergeTypeEnum::Insert);
        actual_node.run(frame.clone(), ctx.clone()).unwrap();
        actual_node.run(frame.clone(), ctx.clone()).unwrap();
    }

    #[test]
    fn valid_clickhouse_sink_config_to_clickhouse_sink_make_next() {
        let frame = get_example_frame();
        let (ctx, _, actual_node) = get_node("canadian_make_next", MergeTypeEnum::MakeNext);
        actual_node.run(frame.clone(), ctx.clone()).unwrap();
        actual_node.run(frame.clone(), ctx.clone()).unwrap();
    }
}
