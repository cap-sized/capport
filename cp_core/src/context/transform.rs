use std::collections::HashMap;

use crate::{parser::common::YamlRead, task::{stage::StageTaskConfig, transform::{common::RootTransform, config::RootTransformConfig}}, util::error::{CpError, CpResult}};

use super::common::Configurable;


#[derive(Debug)]
pub struct TransformRegistry {
    configs: HashMap<String, RootTransformConfig>,
}

impl Default for TransformRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TransformRegistry {
    pub fn new() -> TransformRegistry {
        TransformRegistry {
            configs: HashMap::new(),
        }
    }
    pub fn insert(&mut self, transform: RootTransformConfig) -> Option<RootTransformConfig> {
        let prev = self.configs.remove(&transform.label);
        self.configs.insert(transform.label.clone(), transform);
        prev
    }
    pub fn from(config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>) -> CpResult<TransformRegistry> {
        let mut reg = TransformRegistry {
            configs: HashMap::new(),
        };
        reg.extract_parse_config(config_pack)?;
        Ok(reg)
    }
    pub fn get_transform_config(&self, transform_name: &str) -> Option<RootTransformConfig> {
        self.configs.get(transform_name).map(|x| x.to_owned())
    }
}

impl Configurable for TransformRegistry {
    fn get_node_name() -> &'static str {
        "transform"
    }
    fn extract_parse_config(
        &mut self,
        config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    ) -> CpResult<()> {
        let configs = config_pack.remove(TransformRegistry::get_node_name()).unwrap_or_default();
        let mut errors = vec![];
        for (label, mut fields) in configs {
            fields.add_to_map(
                serde_yaml_ng::Value::String("label".to_owned()), 
                serde_yaml_ng::Value::String(label.clone()), 
            )?;
            match serde_yaml_ng::from_value::<RootTransformConfig>(fields) {
                Ok(transform) => {
                    self.configs.insert(label, transform);
                }
                Err(e) => {
                    errors.push(CpError::ConfigError(
                        "Transform",
                        format!("{}: {:?}", label, e.to_string()),
                    ));
                }
            };
        }
        if !errors.is_empty() {
            Err(CpError::ConfigError(
                "TransformRegistry: transform",
                format!("Errors parsing:\n{:?}", errors),
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{parser::keyword::{Keyword, StrKeyword}, task::transform::config::RootTransformConfig, util::common::create_config_pack};

    use super::TransformRegistry;


    #[test]
    fn valid_unpack_transform_registry() {
        let configs = ["
transform:
    empty_trf: 
        input: TEST
        output: $output
        steps:
irrelevant_node:
    for_testing:
        a: b
        ",
        "
transform:
    transform_a: 
        input: TEST
        output: $output
        steps:
            - select:
                a: uint64

"];
        let mut config_pack = create_config_pack(configs);
        let actual = TransformRegistry::from(&mut config_pack).unwrap();
        assert_eq!(actual.get_transform_config("empty_trf").unwrap(), RootTransformConfig {
            label: "empty_trf".to_owned(),
            input: StrKeyword::with_value("TEST".to_owned()),
            output: StrKeyword::with_symbol("output"),
            steps: vec![]
        });
        assert_eq!(actual.get_transform_config("transform_a").unwrap(), RootTransformConfig {
            label: "transform_a".to_owned(),
            input: StrKeyword::with_value("TEST".to_owned()),
            output: StrKeyword::with_symbol("output"),
            steps: vec![
                serde_yaml_ng::from_str::<serde_yaml_ng::Value>("
select:
    a: uint64
").unwrap(),
            ]
        });

    }

}
