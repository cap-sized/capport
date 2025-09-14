use serde::Deserialize;

use crate::{
    model::common::ModelFields,
    parser::{http::HttpOptionsConfig, keyword::StrKeyword, sql_connection::SqlConnection},
};

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
pub struct _CsvSourceConfig {
    pub filepath: StrKeyword,
    pub output: StrKeyword,
    // model name, takes precedence over model_fields
    pub model: Option<StrKeyword>,
    // holds a fully substituted ModelConfig
    pub model_fields: Option<ModelFields>,
    pub separator: Option<StrKeyword>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct JsonSourceConfig {
    pub json: LocalFileSourceConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CsvSourceConfig {
    pub csv: _CsvSourceConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PostgresSourceConfig {
    pub postgres: SqlConnection,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct MySqlSourceConfig {
    pub mysql: SqlConnection,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SingleLinkConfig {
    pub url: StrKeyword,
    pub output: StrKeyword,
    // todo: add specification of content-type and request type (optional)
    pub model: Option<StrKeyword>,
    pub model_fields: Option<ModelFields>,
    pub options: Option<HttpOptionsConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct MongoConnection {
    pub url: Option<StrKeyword>,
    pub env_connection: Option<StrKeyword>, // use a preset
    pub output: Option<StrKeyword>,
    pub collection: StrKeyword,
    pub find: mongodb::bson::Document,
    pub projection: Option<mongodb::bson::Document>,
    pub model: Option<StrKeyword>,
    pub model_fields: Option<ModelFields>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct HttpSourceConfig {
    pub http: SingleLinkConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct MongoSourceConfig {
    pub mongo: MongoConnection,
}

impl Eq for MongoConnection {}

#[cfg(test)]
mod tests {

    use bson::doc;
    use polars::prelude::DataType;

    use crate::{
        model::common::{ModelFieldInfo, ModelFields},
        parser::{
            dtype::DType,
            http::HttpOptionsConfig,
            keyword::{Keyword, ModelFieldKeyword, StrKeyword},
        },
        task::source::config::{CsvSourceConfig, JsonSourceConfig},
    };

    use super::{
        _CsvSourceConfig, HttpSourceConfig, LocalFileSourceConfig, MongoConnection, MongoSourceConfig, SingleLinkConfig,
    };

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
                model_fields: Some(ModelFields::from([(
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
                model_fields: Some(ModelFields::from([
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

    fn local_to_csv_src_config(local: LocalFileSourceConfig, separator: Option<StrKeyword>) -> _CsvSourceConfig {
        _CsvSourceConfig {
            separator,
            output: local.output,
            filepath: local.filepath,
            model: local.model,
            model_fields: local.model_fields,
        }
    }

    fn get_configs() -> [&'static str; 5] {
        [
            "
{}:
    filepath: $fp
    output: $output
    sql: select * from test;
    model: test
    {csv}
",
            "
{}:
    filepath: $fp
    model: mymod
    output: OUT
    model_fields:
        test: int8
    {csv}
",
            "
{}:
    filepath: fp
    output: $output
    model: $test
    {csv}
",
            "
{}:
    filepath: fp
    output: output
    model: $test
    {csv}
",
            "
{}:
    filepath: fp
    output: output
    model: test
    model_fields: 
        aaa: int64
        bbb: str
    {csv}
",
        ]
    }

    #[test]
    fn valid_source_config_json() {
        let configs = get_configs()
            .iter()
            .map(|c| c.replace("{}", "json").replace("{csv}", ""))
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
            .map(|c| c.replace("{}", "csv").replace("{csv}", "separator: \",\""))
            .collect::<Vec<String>>();
        let locals = get_locals();
        for i in 0..5 {
            assert_eq!(
                CsvSourceConfig {
                    csv: local_to_csv_src_config(locals[i].clone(), Some(StrKeyword::with_value(",".to_owned())))
                },
                serde_yaml_ng::from_str::<CsvSourceConfig>(&configs[i]).unwrap()
            );
        }
    }

    #[test]
    fn valid_source_config_http() {
        let configs = [
            "
http:
    url: http://mywebsite.com
    output: $example
    model_fields:
        $test: int8
",
            "
http:
    url: http://mywebsite.com
    output: $example
    model_fields:
        $test: int8
    options: { max_retry: 8, init_retry_interval_ms: 100 }
",
        ];
        let expected = [
            None,
            Some(HttpOptionsConfig {
                max_retry: Some(8),
                init_retry_interval_ms: Some(100),
            }),
        ];
        for i in 0..2 {
            assert_eq!(
                HttpSourceConfig {
                    http: SingleLinkConfig {
                        url: StrKeyword::with_value("http://mywebsite.com".to_owned()),
                        output: StrKeyword::with_symbol("example"),
                        model: None,
                        model_fields: Some(ModelFields::from([(
                            StrKeyword::with_symbol("test"),
                            ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::Int8)))
                        )])),
                        options: expected[i].clone()
                    },
                },
                serde_yaml_ng::from_str::<HttpSourceConfig>(configs[i]).unwrap()
            )
        }
    }

    #[test]
    fn valid_source_config_mongo() {
        let configs = [
            r#"
mongo:
    url: mongo+src://
    output: $output
    collection: table
    model: model
    find: { "a": { "$ne": "b" } }
"#,
            r#"
mongo:
    env_connection: $fallback
    output: $output
    collection: table
    model: model
    find: {}
    projection: { id : 1 }
"#,
        ];
        let expected = [
            MongoConnection {
                url: Some(StrKeyword::with_value("mongo+src://".to_owned())),
                env_connection: None,
                output: Some(StrKeyword::with_symbol("output")),
                collection: StrKeyword::with_value("table".to_owned()),
                model: Some(StrKeyword::with_value("model".to_owned())),
                model_fields: None,
                find: doc! { "a": { "$ne" : "b" } },
                projection: None,
            },
            MongoConnection {
                url: None,
                env_connection: Some(StrKeyword::with_symbol("fallback")),
                output: Some(StrKeyword::with_symbol("output")),
                collection: StrKeyword::with_value("table".to_owned()),
                model: Some(StrKeyword::with_value("model".to_owned())),
                model_fields: None,
                find: doc! {},
                projection: Some(doc! { "id" : 1 }),
            },
        ];

        for i in 0..2 {
            assert_eq!(
                MongoSourceConfig {
                    mongo: expected[i].to_owned()
                },
                serde_yaml_ng::from_str::<MongoSourceConfig>(configs[i]).unwrap()
            );
        }
    }
}
