use serde::Deserialize;

use crate::{
    model::common::ModelFields,
    parser::{keyword::StrKeyword, merge_type::MergeTypeEnum, sql_connection::SqlSendConnection},
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
pub struct TableOptions {
    pub order_by: Vec<StrKeyword>,
    pub primary_key: Vec<StrKeyword>,
    pub not_null: Option<Vec<StrKeyword>>,
    pub db_name: Option<StrKeyword>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ClickhouseSinkConfig {
    pub clickhouse: SqlSendConnection,
    pub options: Option<TableOptions>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct JsonSinkConfig {
    pub json: LocalFileSinkConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CsvSinkConfig {
    pub csv: LocalFileSinkConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ParquetSinkConfig {
    pub pq: LocalFileSinkConfig,
}

#[cfg(test)]
mod tests {

    use polars::prelude::DataType;

    use crate::{
        model::common::{ModelFieldInfo, ModelFields},
        parser::{
            dtype::DType,
            keyword::{Keyword, ModelFieldKeyword, StrKeyword},
            merge_type::MergeTypeEnum, sql_connection::{SqlConnection, SqlSendConnection},
        },
        task::sink::config::{CsvSinkConfig, JsonSinkConfig, ParquetSinkConfig},
    };

    use super::{ClickhouseSinkConfig, TableOptions, LocalFileSinkConfig};

    #[test]
    fn valid_clickhouse_sink_config() {
        let config = "
clickhouse:
    conn:
        label: default
        user: default
    input: $test
    table: $data
options:
    order_by: [first]
    primary_key: [second, $key]
            ";
        let clickhouse = SqlSendConnection {
            conn: SqlConnection {
                label: StrKeyword::with_value("default".to_owned()),
                user: StrKeyword::with_value("default".to_owned()),
            },
            input: StrKeyword::with_symbol("test"),
            merge_type: None,
            table: StrKeyword::with_symbol("data"),
        };
        let options = TableOptions {
            order_by: vec![StrKeyword::with_value("first".to_owned())],
            primary_key: vec![
                StrKeyword::with_value("second".to_owned()),
                StrKeyword::with_symbol("key"),
            ],
            not_null: None,
            db_name: None,
        };
        let expected = ClickhouseSinkConfig {
            clickhouse,
            options: Some(options),
        };
        assert_eq!(
            serde_yaml_ng::from_str::<ClickhouseSinkConfig>(config).unwrap(),
            expected
        );
    }

    fn get_locals() -> [LocalFileSinkConfig; 2] {
        [
            LocalFileSinkConfig {
                filepath: StrKeyword::with_symbol("fp"),
                merge_type: MergeTypeEnum::MakeNext,
                model: None,
                model_fields: Some(ModelFields::from([(
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

    #[test]
    fn valid_sink_config_pq() {
        let configs = get_configs()
            .iter()
            .map(|c| c.replace("{}", "pq"))
            .collect::<Vec<String>>();
        let locals = get_locals();
        for i in 0..2 {
            assert_eq!(
                ParquetSinkConfig { pq: locals[i].clone() },
                serde_yaml_ng::from_str::<ParquetSinkConfig>(&configs[i]).unwrap()
            );
        }
    }
}
