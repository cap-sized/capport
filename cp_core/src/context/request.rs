use std::collections::HashMap;

use crate::{
    parser::common::YamlRead,
    task::request::config::RequestGroupConfig,
    util::error::{CpError, CpResult},
};

use super::common::Configurable;

#[derive(Debug)]
pub struct RequestRegistry {
    configs: HashMap<String, RequestGroupConfig>,
}

impl Default for RequestRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestRegistry {
    pub fn new() -> RequestRegistry {
        RequestRegistry {
            configs: HashMap::new(),
        }
    }
    pub fn insert(&mut self, request: RequestGroupConfig) -> Option<RequestGroupConfig> {
        let prev = self.configs.remove(&request.label);
        self.configs.insert(request.label.clone(), request);
        prev
    }
    pub fn from(config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>) -> CpResult<RequestRegistry> {
        let mut reg = RequestRegistry {
            configs: HashMap::new(),
        };
        reg.extract_parse_config(config_pack)?;
        Ok(reg)
    }
    pub fn get_request_config(&self, request_name: &str) -> Option<RequestGroupConfig> {
        self.configs.get(request_name).map(|x| x.to_owned())
    }
}

impl Configurable for RequestRegistry {
    fn get_node_name() -> &'static str {
        "request"
    }
    fn extract_parse_config(
        &mut self,
        config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    ) -> CpResult<()> {
        let configs = config_pack.remove(RequestRegistry::get_node_name()).unwrap_or_default();
        let mut errors = vec![];
        for (label, mut fields) in configs {
            fields.add_to_map(
                serde_yaml_ng::Value::String("label".to_owned()),
                serde_yaml_ng::Value::String(label.clone()),
            )?;
            match serde_yaml_ng::from_value::<RequestGroupConfig>(fields) {
                Ok(request) => {
                    self.configs.insert(label, request);
                }
                Err(e) => {
                    errors.push(CpError::ConfigError(
                        "Request",
                        format!("{}: {:?}", label, e.to_string()),
                    ));
                }
            };
        }
        if !errors.is_empty() {
            Err(CpError::ConfigError(
                "RequestRegistry: request",
                format!("Errors parsing:\n{:?}", errors),
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        parser::keyword::{Keyword, StrKeyword},
        task::request::config::RequestGroupConfig,
        util::common::create_config_pack,
    };

    use super::RequestRegistry;

    #[test]
    fn valid_unpack_request_registry() {
        let configs = [
            "
request:
    empty: 
        input: $input
        max_threads: 1
        requests:
irrelevant_node:
    for_testing:
        a: b
        ",
            "
request:
    request_a: 
        input: input
        max_threads: 3
        requests:
            - http_batch:
                filepath: $fp
            - http_single:
                filepath: fp
",
        ];
        let mut config_pack = create_config_pack(configs);
        let actual = RequestRegistry::from(&mut config_pack).unwrap();
        assert_eq!(
            actual.get_request_config("empty").unwrap(),
            RequestGroupConfig {
                label: "empty".to_owned(),
                input: StrKeyword::with_symbol("input"),
                max_threads: 1,
                requests: vec![]
            }
        );
        assert_eq!(
            actual.get_request_config("request_a").unwrap(),
            RequestGroupConfig {
                label: "request_a".to_owned(),
                input: StrKeyword::with_value("input".to_owned()),
                max_threads: 3,
                requests: serde_yaml_ng::from_str::<Vec<serde_yaml_ng::Value>>(
                    "
- http_batch:
    filepath: $fp
- http_single:
    filepath: fp
"
                )
                .unwrap(),
            }
        );
    }
}
