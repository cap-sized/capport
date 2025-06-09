use serde::Deserialize;

use crate::{
    model::common::ModelFields,
    parser::{keyword::StrKeyword, merge_type::MergeTypeEnum},
};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SinkGroupConfig {
    pub label: String,
    pub input: StrKeyword,
    pub max_threads: usize,
    pub sinks: Vec<serde_yaml_ng::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LocalFileSinkConfig {
    pub filepath: StrKeyword,
    pub merge_type: MergeTypeEnum,
    pub model: Option<StrKeyword>,
    pub model_fields: Option<ModelFields>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct JsonSinkConfig {
    pub json: LocalFileSinkConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CsvSinkConfig {
    pub csv: LocalFileSinkConfig,
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
            merge_type::MergeTypeEnum,
        },
        task::sink::config::{CsvSinkConfig, JsonSinkConfig},
    };

    use super::LocalFileSinkConfig;

    fn get_locals() -> [LocalFileSinkConfig; 2] {
        [
            LocalFileSinkConfig {
                filepath: StrKeyword::with_symbol("fp"),
                merge_type: MergeTypeEnum::MakeNext,
                model: None,
                model_fields: Some(HashMap::from([(
                    StrKeyword::with_value("a".to_owned()),
                    ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::Int8))),
                )])),
            },
            LocalFileSinkConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
                merge_type: MergeTypeEnum::Replace,
                model: Some(StrKeyword::with_symbol("test")),
                model_fields: None,
            },
        ]
    }

    fn get_configs() -> [&'static str; 2] {
        [
            "
{}:
    filepath: $fp
    merge_type: next
    model_fields: 
        a: int8
",
            "
{}:
    filepath: fp
    merge_type: REPLACE
    model: $test
",
        ]
    }

    #[test]
    fn valid_sink_config_json() {
        let configs = get_configs()
            .iter()
            .map(|c| c.replace("{}", "json"))
            .collect::<Vec<String>>();
        let locals = get_locals();
        for i in 0..2 {
            assert_eq!(
                JsonSinkConfig {
                    json: locals[i].clone()
                },
                serde_yaml_ng::from_str::<JsonSinkConfig>(&configs[i]).unwrap()
            );
        }
    }

    #[test]
    fn valid_sink_config_csv() {
        let configs = get_configs()
            .iter()
            .map(|c| c.replace("{}", "csv"))
            .collect::<Vec<String>>();
        let locals = get_locals();
        for i in 0..2 {
            assert_eq!(
                CsvSinkConfig { csv: locals[i].clone() },
                serde_yaml_ng::from_str::<CsvSinkConfig>(&configs[i]).unwrap()
            );
        }
    }
}
