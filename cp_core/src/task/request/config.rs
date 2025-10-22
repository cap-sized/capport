use serde::Deserialize;

use crate::{
    model::common::ModelFields,
    parser::{
        http::{HttpMethod, HttpOptionsConfig},
        keyword::{PolarsExprKeyword, StrKeyword}, sql_connection::{SqlGetModelConnection, SqlReqConnection},
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RequestGroupConfig {
    pub label: String,
    pub input: StrKeyword,
    pub max_threads: usize,
    pub requests: Vec<serde_yaml_ng::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct HttpParamConfig {
    pub df: StrKeyword,
    pub col: PolarsExprKeyword,
    pub template: Option<String>,
    pub separator: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct HttpReqConfig {
    pub method: HttpMethod,
    pub content_type: String,
    pub output: StrKeyword,
    pub url_column: PolarsExprKeyword,
    pub url_params: Option<Vec<HttpParamConfig>>,
    pub model: Option<StrKeyword>,
    pub model_fields: Option<ModelFields>,
    pub options: Option<HttpOptionsConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct HttpBatchConfig {
    pub http_batch: HttpReqConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct HttpSingleConfig {
    pub http_single: HttpReqConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ClickhouseReqConfig {
    pub ch_sql: SqlReqConnection,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ClickhouseModelConfig {
    pub ch_model: SqlGetModelConnection,
}

#[cfg(test)]
mod tests {
    use polars::prelude::DataType;

    use crate::{model::common::{ModelFieldInfo, ModelFields}, parser::{
        dtype::DType, http::HttpMethod, keyword::{Keyword, ModelFieldKeyword, StrKeyword}, sql_connection::{SqlConnection, SqlGetModelConnection, SqlReqConnection}
    }, task::request::config::ClickhouseModelConfig};

    use super::{ClickhouseReqConfig, HttpBatchConfig, HttpOptionsConfig, HttpParamConfig, HttpReqConfig, HttpSingleConfig};
    fn get_http() -> [HttpReqConfig; 2] {
        [
            HttpReqConfig {
                method: HttpMethod::Get,
                content_type: "application/json".to_owned(),
                url_column: serde_yaml_ng::from_str("url").unwrap(),
                output: StrKeyword::with_symbol("output"),
                url_params: None,
                model: Some(StrKeyword::with_value("player".to_owned())),
                model_fields: None,
                options: Some(HttpOptionsConfig {
                    max_retry: Some(1),
                    init_retry_interval_ms: Some(1000),
                }),
            },
            HttpReqConfig {
                method: HttpMethod::Get,
                content_type: "application/json".to_owned(),
                url_column: serde_yaml_ng::from_str("{str: https://api-web.nhle.com/v1/meta}").unwrap(),
                output: StrKeyword::with_value("WEB_PLAYERS".to_owned()),
                url_params: Some(vec![HttpParamConfig {
                    df: StrKeyword::with_value("PLAYERS_DF".to_owned()),
                    col: serde_yaml_ng::from_str("player").unwrap(),
                    template: Some("players={}".to_owned()),
                    separator: Some(",".to_owned()),
                }]),
                model: None,
                model_fields: Some(serde_yaml_ng::from_str("test: str").unwrap()),
                options: None,
            },
        ]
    }

    fn get_http_configs() -> [&'static str; 2] {
        [
            "
http_batch:
    method: get
    content_type: application/json
    output: $output
    url_column: url
    model: player
    options:
        max_retry: 1
        init_retry_interval_ms: 1000
",
            r#"
http_single:
    method: GET
    content_type: application/json
    output: WEB_PLAYERS
    url_column: 
        str: https://api-web.nhle.com/v1/meta
    url_params:
        - df: PLAYERS_DF
          col: player
          template: players={}
          separator: ","
    model_fields:
        test: str
"#,
        ]
    }

    #[test]
    fn valid_source_config_http() {
        let configs = get_http_configs();
        let expecteds = get_http();
        let http_batch = serde_yaml_ng::from_str::<HttpBatchConfig>(configs[0]).unwrap();
        let http_single = serde_yaml_ng::from_str::<HttpSingleConfig>(configs[1]).unwrap();
        assert_eq!(
            http_batch,
            HttpBatchConfig {
                http_batch: expecteds[0].clone()
            }
        );
        assert_eq!(
            http_single,
            HttpSingleConfig {
                http_single: expecteds[1].clone()
            }
        );
    }

    #[test]
    fn valid_source_config_ch_sql() {
        let config = "
ch_sql:
    conn: 
        label: sample
        user: default
    query_column: $query
            ";
        let expected = ClickhouseReqConfig {
            ch_sql: SqlReqConnection {
                conn: SqlConnection {
                    label: StrKeyword::with_value("sample".to_owned()),
                    user: StrKeyword::with_value("default".to_owned()),
                },
                query_column: StrKeyword::with_symbol("query")
            }
        };
        assert_eq!(serde_yaml_ng::from_str::<ClickhouseReqConfig>(config).unwrap(), expected);
    }

    #[test]
    fn valid_source_config_ch_model() {
        let config_a = "
ch_model:
    conn: 
        label: $sample
        user: default
    table: $table
    model: PLAYERS
            ";
        let expected_a = ClickhouseModelConfig {
            ch_model: SqlGetModelConnection {
                conn: SqlConnection {
                    label: StrKeyword::with_symbol("sample"),
                    user: StrKeyword::with_value("default".to_owned()),
                },
                table: StrKeyword::with_symbol("table"),
                model: Some(StrKeyword::with_value("PLAYERS".to_owned())),
                model_fields: None,
                extra_clauses: None
            }
        };
        let config_b = "
ch_model:
    conn: 
        label: $sample
        user: default
    table: $table
    model_fields:
        id: uint64
        name: str
        $fake: $field
    extra_clauses: $id_list
            ";
        let mut expected_b = expected_a.clone();
        let _ = expected_b.ch_model.model.take();
        let _ = expected_b.ch_model.model_fields.insert(ModelFields::from([
                (StrKeyword::with_value("id".to_owned()), ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::UInt64)))),
                (StrKeyword::with_value("name".to_owned()), ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::String)))),
                (StrKeyword::with_symbol("fake"), ModelFieldKeyword::with_symbol("field")),
        ]));
        let _ = expected_b.ch_model.extra_clauses.insert(StrKeyword::with_symbol("id_list"));
        assert_eq!(serde_yaml_ng::from_str::<ClickhouseModelConfig>(config_a).unwrap(), expected_a);
        assert_eq!(serde_yaml_ng::from_str::<ClickhouseModelConfig>(config_b).unwrap(), expected_b);
    }
}

