use std::collections::HashMap;

use crate::{
    parser::common::YamlRead,
    task::sink::config::SinkGroupConfig,
    util::error::{CpError, CpResult},
};

use super::common::Configurable;

#[derive(Debug)]
pub struct SinkRegistry {
    configs: HashMap<String, SinkGroupConfig>,
}

impl Default for SinkRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SinkRegistry {
    pub fn new() -> SinkRegistry {
        SinkRegistry {
            configs: HashMap::new(),
        }
    }
    pub fn insert(&mut self, sink: SinkGroupConfig) -> Option<SinkGroupConfig> {
        let prev = self.configs.remove(&sink.label);
        self.configs.insert(sink.label.clone(), sink);
        prev
    }
    pub fn from(config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>) -> CpResult<SinkRegistry> {
        let mut reg = SinkRegistry {
            configs: HashMap::new(),
        };
        reg.extract_parse_config(config_pack)?;
        Ok(reg)
    }
    pub fn get_sink_config(&self, sink_name: &str) -> Option<SinkGroupConfig> {
        self.configs.get(sink_name).map(|x| x.to_owned())
    }
}

impl Configurable for SinkRegistry {
    fn get_node_name() -> &'static str {
        "sink"
    }
    fn extract_parse_config(
        &mut self,
        config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    ) -> CpResult<()> {
        let configs = config_pack.remove(SinkRegistry::get_node_name()).unwrap_or_default();
        let mut errors = vec![];
        for (label, mut fields) in configs {
            fields.add_to_map(
                serde_yaml_ng::Value::String("label".to_owned()),
                serde_yaml_ng::Value::String(label.clone()),
            )?;
            match serde_yaml_ng::from_value::<SinkGroupConfig>(fields) {
                Ok(sink) => {
                    self.configs.insert(label, sink);
                }
                Err(e) => {
                    errors.push(CpError::ConfigError("Sink", format!("{}: {:?}", label, e.to_string())));
                }
            };
        }
        if !errors.is_empty() {
            Err(CpError::ConfigError(
                "SinkRegistry: sink",
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
        task::sink::config::SinkGroupConfig,
        util::common::create_config_pack,
    };

    use super::SinkRegistry;

    #[test]
    fn valid_unpack_sink_registry() {
        let configs = [
            "
sink:
    empty: 
        input: $input
        max_threads: 1
        sinks:
irrelevant_node:
    for_testing:
        a: b
        ",
            "
sink:
    sink_a: 
        input: input
        max_threads: 3
        sinks:
            - json:
                filepath: $fp
            - json:
                filepath: fp
",
        ];
        let mut config_pack = create_config_pack(configs);
        let actual = SinkRegistry::from(&mut config_pack).unwrap();
        assert_eq!(
            actual.get_sink_config("empty").unwrap(),
            SinkGroupConfig {
                label: "empty".to_owned(),
                input: StrKeyword::with_symbol("input"),
                max_threads: 1,
                sinks: vec![]
            }
        );
        assert_eq!(
            actual.get_sink_config("sink_a").unwrap(),
            SinkGroupConfig {
                label: "sink_a".to_owned(),
                input: StrKeyword::with_value("input".to_owned()),
                max_threads: 3,
                sinks: serde_yaml_ng::from_str::<Vec<serde_yaml_ng::Value>>(
                    "
- json:
    filepath: $fp
- json:
    filepath: fp
"
                )
                .unwrap(),
            }
        );
    }
}
