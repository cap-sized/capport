use serde::Deserialize;

use crate::parser::keyword::StrKeyword;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RootSourceConfig {
    pub label: String,
    pub sources: Vec<serde_yaml_ng::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct JsonSourceConfig {
    pub filepath: StrKeyword,
    pub output: StrKeyword,
    pub model: StrKeyword,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CsvSourceConfig {
    pub filepath: StrKeyword,
    pub output: StrKeyword,
    pub model: StrKeyword,
}

#[cfg(test)]
mod tests {
    use crate::{
        parser::keyword::{Keyword, StrKeyword},
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
        ];
        let jsons = [
            JsonSourceConfig {
                filepath: StrKeyword::with_symbol("fp"),
                output: StrKeyword::with_symbol("output"),
                model: StrKeyword::with_value("test".to_string()),
            },
            JsonSourceConfig {
                filepath: StrKeyword::with_symbol("fp"),
                output: StrKeyword::with_value("output".to_string()),
                model: StrKeyword::with_value("test".to_string()),
            },
            JsonSourceConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
                output: StrKeyword::with_symbol("output"),
                model: StrKeyword::with_symbol("test"),
            },
            JsonSourceConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
                output: StrKeyword::with_value("output".to_string()),
                model: StrKeyword::with_symbol("test"),
            },
        ];
        let csvs = [
            CsvSourceConfig {
                filepath: StrKeyword::with_symbol("fp"),
                output: StrKeyword::with_symbol("output"),
                model: StrKeyword::with_value("test".to_string()),
            },
            CsvSourceConfig {
                filepath: StrKeyword::with_symbol("fp"),
                output: StrKeyword::with_value("output".to_string()),
                model: StrKeyword::with_value("test".to_string()),
            },
            CsvSourceConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
                output: StrKeyword::with_symbol("output"),
                model: StrKeyword::with_symbol("test"),
            },
            CsvSourceConfig {
                filepath: StrKeyword::with_value("fp".to_string()),
                output: StrKeyword::with_value("output".to_string()),
                model: StrKeyword::with_symbol("test"),
            },
        ];
        for i in 0..4 {
            assert_eq!(
                jsons[i],
                serde_yaml_ng::from_str::<JsonSourceConfig>(configs[i]).unwrap()
            );
            assert_eq!(csvs[i], serde_yaml_ng::from_str::<CsvSourceConfig>(configs[i]).unwrap());
        }
    }
}
