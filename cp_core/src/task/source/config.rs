use serde::Deserialize;

use crate::{model::common::ModelFields, parser::keyword::StrKeyword};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SourceGroupConfig {
    pub label: String,
    pub max_threads: usize,
    pub sources: Vec<serde_yaml_ng::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LocalFileSourceConfig {
    pub filepath: StrKeyword,
    pub output: StrKeyword,
    // model name, takes precedence over model_fields
    pub model: Option<StrKeyword>,
    // holds a fully substituted ModelConfig
    pub model_fields: Option<ModelFields>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct JsonSourceConfig {
    pub json: LocalFileSourceConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CsvSourceConfig {
    pub csv: LocalFileSourceConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SqlConnection {
    pub url: Option<String>,
    pub db: Option<StrKeyword>,
    pub env_connection: Option<String>, // use a preset
    pub output: StrKeyword,
    pub model: Option<StrKeyword>,
    pub model_fields: Option<ModelFields>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ClickhouseSourceConfig {
    pub clickhouse: SqlConnection,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use polars::prelude::DataType;

    use crate::{
        model::common::ModelFieldInfo,
        parser::{
            dtype::DType,
            keyword::{Keyword, ModelFieldKeyword, StrKeyword},
        },
        task::source::config::{ClickhouseSourceConfig, CsvSourceConfig, JsonSourceConfig},
    };

    use super::{LocalFileSourceConfig, SqlConnection};

    fn get_locals() -> [LocalFileSourceConfig; 5] {
        [
            LocalFileSourceConfig {
                filepath: StrKeyword::with_symbol("fp"),
                output: StrKeyword::with_symbol("output"),
                model: Some(StrKeyword::with_value("test".to_string())),
                model_fields: None,
            },
            LocalFileSourceConfig {
                filepath: StrKeyword::with_symbol("fp"),
                output: StrKeyword::with_value("OUT".to_string()),
                model: Some(StrKeyword::with_value("mymod".to_string())),
                model_fields: Some(HashMap::from([(
                    StrKeyword::with_value("test".to_owned()),
                    ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::Int8))),
                )])),
            },
            LocalFileSourceConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
                output: StrKeyword::with_symbol("output"),
                model: Some(StrKeyword::with_symbol("test")),
                model_fields: None,
            },
            LocalFileSourceConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
                output: StrKeyword::with_value("output".to_string()),
                model: Some(StrKeyword::with_symbol("test")),
                model_fields: None,
            },
            LocalFileSourceConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
                output: StrKeyword::with_value("output".to_string()),
                model: Some(StrKeyword::with_value("test".to_string())),
                model_fields: Some(HashMap::from([
                    (
                        StrKeyword::with_value("aaa".to_owned()),
                        ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::Int64))),
                    ),
                    (
                        StrKeyword::with_value("bbb".to_owned()),
                        ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::String))),
                    ),
                ])),
            },
        ]
    }

    fn get_connections() -> [SqlConnection; 2] {
        [
            SqlConnection {
                db: Some(StrKeyword::with_symbol("defdb")),
                env_connection: None,
                url: None,
                model: None,
                output: StrKeyword::with_value("output".to_owned()),
                model_fields: None,
            },
            SqlConnection {
                db: None,
                env_connection: Some("env_conn".to_owned()),
                url: None,
                model: Some(StrKeyword::with_value("mymod".to_owned())),
                output: StrKeyword::with_symbol("actual"),
                model_fields: Some(HashMap::from([(
                    StrKeyword::with_symbol("test"),
                    ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::Int8))),
                )])),
            },
        ]
    }

    fn get_configs() -> [&'static str; 5] {
        [
            "
{}:
    filepath: $fp
    output: $output
    model: test
",
            "
{}:
    filepath: $fp
    model: mymod
    output: OUT
    model_fields:
        test: int8
",
            "
{}:
    filepath: fp
    output: $output
    model: $test
",
            "
{}:
    filepath: fp
    output: output
    model: $test
",
            "
{}:
    filepath: fp
    output: output
    model: test
    model_fields: 
        aaa: int64
        bbb: str
",
        ]
    }

    fn get_connection_configs() -> [&'static str; 2] {
        [
            "
{}:
    db: $defdb
    output: output
",
            "
{}:
    output: $actual
    env_connection: env_conn
    model: mymod
    model_fields: 
        $test: int8
",
        ]
    }

    #[test]
    fn valid_source_config_json() {
        let configs = get_configs()
            .iter()
            .map(|c| c.replace("{}", "json"))
            .collect::<Vec<String>>();
        let locals = get_locals();
        for i in 0..5 {
            assert_eq!(
                JsonSourceConfig {
                    json: locals[i].clone()
                },
                serde_yaml_ng::from_str::<JsonSourceConfig>(&configs[i]).unwrap()
            );
        }
    }

    #[test]
    fn valid_source_config_csv() {
        let configs = get_configs()
            .iter()
            .map(|c| c.replace("{}", "csv"))
            .collect::<Vec<String>>();
        let locals = get_locals();
        for i in 0..5 {
            assert_eq!(
                CsvSourceConfig { csv: locals[i].clone() },
                serde_yaml_ng::from_str::<CsvSourceConfig>(&configs[i]).unwrap()
            );
        }
    }

    #[test]
    fn valid_connection_config_clickhouse() {
        let configs = get_connection_configs()
            .iter()
            .map(|c| c.replace("{}", "clickhouse"))
            .collect::<Vec<String>>();
        let locals = get_connections();
        for i in 0..2 {
            assert_eq!(
                ClickhouseSourceConfig {
                    clickhouse: locals[i].clone()
                },
                serde_yaml_ng::from_str::<ClickhouseSourceConfig>(&configs[i]).unwrap()
            );
        }
    }
}
