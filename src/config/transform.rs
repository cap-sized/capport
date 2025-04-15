use crate::config::common::Configurable;
use crate::task::transform::common::{RootTransform, Transform};
use crate::task::transform::select::{SelectField, SelectTransform};
use crate::util::error::{CpError, CpResult, SubResult};
use std::collections::HashMap;
use std::{fmt, fs};
use yaml_rust2::Yaml;

use super::parser::transform::parse_root_transform;

#[derive(Debug)]
pub struct TransformRegistry {
    registry: HashMap<String, RootTransform>,
}

impl TransformRegistry {
    pub fn new() -> TransformRegistry {
        TransformRegistry {
            registry: HashMap::new(),
        }
    }
    pub fn from(config_pack: &mut HashMap<String, HashMap<String, Yaml>>) -> TransformRegistry {
        let mut reg = TransformRegistry {
            registry: HashMap::new(),
        };
        reg.extract_parse_config(config_pack).unwrap();
        reg
    }
    pub fn get_transform(&self, transform_name: &str) -> Option<&RootTransform> {
        match self.registry.get(transform_name) {
            Some(x) => Some(x),
            None => None,
        }
    }
}

impl Configurable for TransformRegistry {
    fn get_node_name() -> &'static str {
        "transform"
    }
    fn extract_parse_config(&mut self, config_pack: &mut HashMap<String, HashMap<String, Yaml>>) -> CpResult<()> {
        let configs = config_pack
            .remove(TransformRegistry::get_node_name())
            .unwrap_or(HashMap::new());
        for (config_name, node) in configs {
            let model = match parse_root_transform(&config_name, &node) {
                Ok(x) => x,
                Err(e) => {
                    return Err(CpError::ComponentError(
                        "config.model",
                        format!["Transform {}: {}", config_name, e],
                    ));
                }
            };
            self.registry.insert(config_name.to_string(), model);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use yaml_rust2::{YamlLoader, yaml};

    use crate::util::common::create_config_pack;

    use super::*;
    fn create_transform_registry(yaml_str: &str) -> TransformRegistry {
        let mut reg = TransformRegistry::new();
        let mut config_pack = create_config_pack(yaml_str, "transform");
        reg.extract_parse_config(&mut config_pack).unwrap();
        reg
    }

    fn assert_invalid_transform(yaml_str: &str) {
        let mut reg = TransformRegistry::new();
        let mut config_pack = create_config_pack(yaml_str, "transform");
        reg.extract_parse_config(&mut config_pack).unwrap_err();
    }

    #[test]
    fn valid_one_stage_mapping_transform() {
        let tr = create_transform_registry(
            "
player_to_person:
    - select:
        id: csid 
",
        );
        println!("{:?}", tr);
        let actual_transform = tr.get_transform("player_to_person").unwrap();
        // let expected_transform: RootTransform = RootTransform::new("player_to_person", vec![]);
    }
}
