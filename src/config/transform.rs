use crate::config::common::Configurable;
use crate::task::transform::{MappingField, MappingTransform};
use crate::util::error::{CpError, CpResult, SubResult};
use std::collections::HashMap;
use std::fs;
use yaml_rust2::Yaml;

const ACTION_KEYWORD: &str = "action";
const ARGS_KEYWORD: &str = "args";
const KWARGS_KEYWORD: &str = "kwargs";

pub struct TransformRegistry {
    registry: HashMap<String, MappingTransform>,
}

fn parse_mapping_field(name: &str, node: &Yaml) -> SubResult<MappingField> {
    let action_key = Yaml::from_str(ACTION_KEYWORD);
    let args_key = Yaml::from_str(ARGS_KEYWORD);
    let kwargs_key = Yaml::from_str(KWARGS_KEYWORD);
    if node.is_null() {
        return Err(format!("Field {} is null", name));
    }
    if !node.is_hash() {
        Ok(MappingField {
            label: String::from(name),
            action: None,
            args: node.clone(),
            kwargs: None,
        })
    } else {
        let node_map = node.as_hash().unwrap();
        let action = match node_map.get(&action_key) {
            Some(x) => match x.as_str() {
                Some(a) => String::from(a),
                None => return Err(format!("action in field {:?} is not a string", node_map)),
            },
            None => return Err(format!("no action found for MappingField {}", name)),
        };
        Ok(MappingField {
            label: String::from(name),
            action: Some(action),
            args: match node_map.get(&args_key) {
                Some(x) => x.clone(),
                None => return Err(format!("args not found in MappingField {}", name)),
            },
            kwargs: node_map.get(&kwargs_key).cloned(),
        })
    }
}

fn parse_transform(name: &str, node: &Yaml) -> SubResult<MappingTransform> {
    let nodemap = match node.as_hash() {
        Some(x) => x.iter(),
        None => {
            return Err(format!("Model config {} is not a map: {:?}", name, node));
        }
    };
    let mut fields: Vec<MappingField> = vec![];
    for (field_name_node, field) in nodemap {
        let field_name = match field_name_node.as_str() {
            Some(x) => x,
            None => {
                return Err(format!(
                    "Field in model config {} is not a str: {:?}",
                    name, field_name_node
                ));
            }
        };
        let field = match parse_mapping_field(field_name, field) {
            Ok(mf) => mf,
            Err(e) => {
                return Err(e);
            }
        };
        fields.push(field);
    }
    Ok(MappingTransform {
        label: String::from(name),
        mappings: fields,
    })
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
    pub fn get_transform(&self, transform_name: &str) -> Option<MappingTransform> {
        match self.registry.get(transform_name) {
            Some(x) => Some(x.to_owned()),
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
            let model = match parse_transform(&config_name, &node) {
                Ok(x) => x,
                Err(e) => {
                    return Err(CpError::ComponentError(
                        "config.model",
                        format!["TransformMapping {}: {}", config_name, e],
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
    fn create_model_registry(yaml_str: &str) -> TransformRegistry {
        let mut mr = TransformRegistry::new();
        let mut config_pack = create_config_pack(yaml_str);
        mr.extract_parse_config(&mut config_pack).unwrap();
        mr
    }

    fn assert_invalid_model(yaml_str: &str) {
        let mut mr = TransformRegistry::new();
        let mut config_pack = create_config_pack(yaml_str);
        mr.extract_parse_config(&mut config_pack).unwrap_err();
    }
}
