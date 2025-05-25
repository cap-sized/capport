
use std::collections::HashMap;

use crate::{parser::common::YamlRead, task::{stage::StageTaskConfig, source::{common::SourceGroup, config::SourceGroupConfig}}, util::error::{CpError, CpResult}};

use super::common::Configurable;


#[derive(Debug)]
pub struct SourceRegistry {
    configs: HashMap<String, SourceGroupConfig>,
}

impl Default for SourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceRegistry {
    pub fn new() -> SourceRegistry {
        SourceRegistry {
            configs: HashMap::new(),
        }
    }
    pub fn insert(&mut self, source: SourceGroupConfig) -> Option<SourceGroupConfig> {
        let prev = self.configs.remove(&source.label);
        self.configs.insert(source.label.clone(), source);
        prev
    }
    pub fn from(config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>) -> CpResult<SourceRegistry> {
        let mut reg = SourceRegistry {
            configs: HashMap::new(),
        };
        reg.extract_parse_config(config_pack)?;
        Ok(reg)
    }
    pub fn get_source_config(&self, source_name: &str) -> Option<SourceGroupConfig> {
        self.configs.get(source_name).map(|x| x.to_owned())
    }
}

impl Configurable for SourceRegistry {
    fn get_node_name() -> &'static str {
        "source"
    }
    fn extract_parse_config(
        &mut self,
        config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    ) -> CpResult<()> {
        let configs = config_pack.remove(SourceRegistry::get_node_name()).unwrap_or_default();
        let mut errors = vec![];
        for (label, mut fields) in configs {
            fields.add_to_map(
                serde_yaml_ng::Value::String("label".to_owned()), 
                serde_yaml_ng::Value::String(label.clone()), 
            )?;
            match serde_yaml_ng::from_value::<SourceGroupConfig>(fields) {
                Ok(source) => {
                    self.configs.insert(label, source);
                }
                Err(e) => {
                    errors.push(CpError::ConfigError(
                        "Source",
                        format!("{}: {:?}", label, e.to_string()),
                    ));
                }
            };
        }
        if !errors.is_empty() {
            Err(CpError::ConfigError(
                "SourceRegistry: source",
                format!("Errors parsing:\n{:?}", errors),
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{parser::keyword::{Keyword, StrKeyword}, task::source::config::SourceGroupConfig, util::common::create_config_pack};

    use super::SourceRegistry;


    #[test]
    fn valid_unpack_source_registry() {
        let configs = ["
source:
    empty: 
        max_threads: 1
        sources:
irrelevant_node:
    for_testing:
        a: b
        ",
        "
source:
    source_a: 
        max_threads: 3
        sources:
            - json:
                filepath: $fp
                output: A
                model: BACKUP
                model_fields:
                    priority: str
            - json:
                filepath: $fp
                output: B
                model: MAIN
"];
        let mut config_pack = create_config_pack(configs);
        let actual = SourceRegistry::from(&mut config_pack).unwrap();
        assert_eq!(actual.get_source_config("empty").unwrap(), SourceGroupConfig {
            label: "empty".to_owned(),
            max_threads: 1,
            sources: vec![]

        });
        assert_eq!(actual.get_source_config("source_a").unwrap(), SourceGroupConfig {
            label: "source_a".to_owned(),
            max_threads: 3,
            sources: 
                serde_yaml_ng::from_str::<Vec<serde_yaml_ng::Value>>("
- json:
    filepath: $fp
    output: A
    model: BACKUP
    model_fields:
        priority: str
- json:
    filepath: $fp
    output: B
    model: MAIN
").unwrap(),
        });

    }

}
