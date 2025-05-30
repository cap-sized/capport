use serde::Deserialize;

use crate::{
    model::common::ModelFields,
    parser::{
        http::HttpMethod,
        keyword::{PolarsExprKeyword, StrKeyword},
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
pub struct HttpOptionsConfig {
    pub workers: Option<u8>,
    pub max_retry: Option<u8>,
    pub init_retry_interval_ms: Option<u64>,
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

#[cfg(test)]
mod tests {
    use crate::parser::{
        http::HttpMethod,
        keyword::{Keyword, StrKeyword},
    };

    use super::{HttpBatchConfig, HttpOptionsConfig, HttpParamConfig, HttpReqConfig, HttpSingleConfig};
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
                    workers: None,
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
}
