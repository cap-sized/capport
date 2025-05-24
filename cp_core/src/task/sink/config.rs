use serde::Deserialize;

use crate::parser::keyword::StrKeyword;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RootSinkConfig {
    pub label: String,
    pub input: StrKeyword,
    pub sinks: Vec<serde_yaml_ng::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct JsonSinkConfig {
    pub filepath: StrKeyword,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CsvSinkConfig {
    pub filepath: StrKeyword,
}

#[cfg(test)]
mod tests {

    use crate::{
        parser::keyword::{Keyword, StrKeyword},
        task::sink::config::{CsvSinkConfig, JsonSinkConfig},
    };

    #[test]
    fn valid_source_config_json_csv() {
        let configs = [
            "
filepath: $fp
",
            "
filepath: fp
",
        ];
        let jsons = [
            JsonSinkConfig {
                filepath: StrKeyword::with_symbol("fp"),
            },
            JsonSinkConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
            },
        ];
        let csvs = [
            CsvSinkConfig {
                filepath: StrKeyword::with_symbol("fp"),
            },
            CsvSinkConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
            },
        ];
        for i in 0..2 {
            assert_eq!(
                jsons[i],
                serde_yaml_ng::from_str::<JsonSinkConfig>(configs[i]).unwrap()
            );
            assert_eq!(csvs[i], serde_yaml_ng::from_str::<CsvSinkConfig>(configs[i]).unwrap());
        }
    }
}
