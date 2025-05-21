use serde::Deserialize;

use crate::{model::common::ModelFields, parser::keyword::StrKeyword};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RootSourceConfig {
    pub label: String,
    pub sources: Vec<serde_yaml_ng::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct JsonSourceConfig {
    pub filepath: StrKeyword,
    pub output: StrKeyword,
    // model name
    pub model: StrKeyword,
    // holds a fully substituted ModelConfig
    pub model_fields: Option<ModelFields>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CsvSourceConfig {
    pub filepath: StrKeyword,
    pub output: StrKeyword,
    // model name
    pub model: StrKeyword,
    // holds a fully substituted ModelConfig
    pub model_fields: Option<ModelFields>,
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
        task::source::config::CsvSourceConfig,
    };

    use super::JsonSourceConfig;

    #[test]
    fn valid_source_config_json_csv() {
        let configs = [
            "
filepath: $fp
output: $output
model: test
",
            "
filepath: $fp
output: output
model: test
",
            "
filepath: fp
output: $output
model: $test
",
            "
filepath: fp
output: output
model: $test
",
            "
filepath: fp
output: output
model: test
model_fields: 
    aaa: int64
    bbb: str
",
        ];
        let jsons = [
            JsonSourceConfig {
                filepath: StrKeyword::with_symbol("fp"),
                output: StrKeyword::with_symbol("output"),
                model: StrKeyword::with_value("test".to_string()),
                model_fields: None,
            },
            JsonSourceConfig {
                filepath: StrKeyword::with_symbol("fp"),
                output: StrKeyword::with_value("output".to_string()),
                model: StrKeyword::with_value("test".to_string()),
                model_fields: None,
            },
            JsonSourceConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
                output: StrKeyword::with_symbol("output"),
                model: StrKeyword::with_symbol("test"),
                model_fields: None,
            },
            JsonSourceConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
                output: StrKeyword::with_value("output".to_string()),
                model: StrKeyword::with_symbol("test"),
                model_fields: None,
            },
            JsonSourceConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
                output: StrKeyword::with_value("output".to_string()),
                model: StrKeyword::with_value("test".to_string()),
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
        ];
        let csvs = [
            CsvSourceConfig {
                filepath: StrKeyword::with_symbol("fp"),
                output: StrKeyword::with_symbol("output"),
                model: StrKeyword::with_value("test".to_string()),
                model_fields: None,
            },
            CsvSourceConfig {
                filepath: StrKeyword::with_symbol("fp"),
                output: StrKeyword::with_value("output".to_string()),
                model: StrKeyword::with_value("test".to_string()),
                model_fields: None,
            },
            CsvSourceConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
                output: StrKeyword::with_symbol("output"),
                model: StrKeyword::with_symbol("test"),
                model_fields: None,
            },
            CsvSourceConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
                output: StrKeyword::with_value("output".to_string()),
                model: StrKeyword::with_symbol("test"),
                model_fields: None,
            },
            CsvSourceConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
                output: StrKeyword::with_value("output".to_string()),
                model: StrKeyword::with_value("test".to_string()),
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
        ];
        for i in 0..5 {
            assert_eq!(
                jsons[i],
                serde_yaml_ng::from_str::<JsonSourceConfig>(configs[i]).unwrap()
            );
            assert_eq!(csvs[i], serde_yaml_ng::from_str::<CsvSourceConfig>(configs[i]).unwrap());
        }
    }
}
