use yaml_rust2::Yaml;

use crate::model::common::{Model, ModelField};
use crate::util::common::{NYT, UTC};
use crate::util::error::{CpError, CpResult, SubResult};
use polars::datatypes::{DataType, TimeUnit, TimeZone};
use std::collections::HashMap;
use std::{fmt, fs};

use crate::parser::model::parse_model;

use super::common::Configurable;

#[derive(Debug)]
pub struct ModelRegistry {
    registry: HashMap<String, Model>,
}

impl Default for ModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ModelRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _ = write!(f, "[ ");
        self.registry.values().for_each(|x| {
            let _ = write!(f, "{}, ", x);
        });
        write!(f, " ]")
    }
}

impl ModelRegistry {
    pub fn new() -> ModelRegistry {
        ModelRegistry {
            registry: HashMap::new(),
        }
    }
    pub fn insert(&mut self, model: Model) -> Option<Model> {
        let prev = self.registry.remove(&model.name);
        self.registry.insert(model.name.clone(), model);
        prev
    }
    pub fn from(config_pack: &mut HashMap<String, HashMap<String, Yaml>>) -> CpResult<ModelRegistry> {
        let mut reg = ModelRegistry {
            registry: HashMap::new(),
        };
        reg.extract_parse_config(config_pack)?;
        Ok(reg)
    }
    pub fn get_model(&self, model_name: &str) -> Option<Model> {
        self.registry.get(model_name).map(|x| x.to_owned())
    }
}

impl Configurable for ModelRegistry {
    fn get_node_name() -> &'static str {
        "model"
    }
    fn extract_parse_config(&mut self, config_pack: &mut HashMap<String, HashMap<String, Yaml>>) -> CpResult<()> {
        let configs = config_pack.remove(ModelRegistry::get_node_name()).unwrap_or_default();
        for (config_name, node) in configs {
            let model = match parse_model(&config_name, &node) {
                Ok(x) => x,
                Err(e) => {
                    return Err(CpError::ComponentError(
                        "config.model",
                        format!["Model {}: {}", config_name, e],
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

    fn create_model_registry(yaml_str: &str) -> ModelRegistry {
        let mut config_pack = create_config_pack(yaml_str, "model");
        ModelRegistry::from(&mut config_pack).unwrap()
    }

    fn assert_invalid_model(yaml_str: &str) {
        let mut reg = ModelRegistry::new();
        let mut config_pack = create_config_pack(yaml_str, "model");
        reg.extract_parse_config(&mut config_pack).unwrap_err();
    }

    #[test]
    fn valid_basic_model() {
        let mr = create_model_registry(
            "
person:
    full_name: str
    first_name: str
    last_name: str
",
        );
        let actual_model = mr.get_model("person").unwrap();
        let expected_model: Model = Model::new(
            "person",
            &[
                ModelField::new("full_name", DataType::String, None),
                ModelField::new("first_name", DataType::String, None),
                ModelField::new("last_name", DataType::String, None),
            ],
        );
        assert_eq!(actual_model, expected_model);
    }

    #[test]
    fn valid_basic_model_details() {
        let mr = create_model_registry(
            "
person:
    id:
      dtype: int64
    pid:
      dtype: str
      constraints: [ primary ]
    full_name: str
    first_name: str
    last_name: str
",
        );
        println!("{}", mr);
        let actual_model = mr.get_model("person").unwrap();
        let expected_model: Model = Model::new(
            "person",
            &[
                ModelField::new("id", DataType::Int64, None),
                ModelField::new("pid", DataType::String, Some(&["primary"])),
                ModelField::new("full_name", DataType::String, None),
                ModelField::new("first_name", DataType::String, None),
                ModelField::new("last_name", DataType::String, None),
            ],
        );
        assert_eq!(actual_model, expected_model);
    }

    #[test]
    fn valid_multiple_models() {
        let mr = create_model_registry(
            "
person:
    id:
      dtype: int64
    full_name: str
player:
    id: 
        dtype: int64
        constraints: [foreign, unique]
    pid: 
        dtype: int32
        constraints: [primary, unique]
    positions:
        dtype: list[str]

",
        );
        {
            let actual_model = mr.get_model("person").unwrap();
            let expected_model: Model = Model::new(
                "person",
                &[
                    ModelField::new("id", DataType::Int64, None),
                    ModelField::new("full_name", DataType::String, None),
                ],
            );
            assert_eq!(actual_model, expected_model);
        }
        {
            let actual_model = mr.get_model("player").unwrap();
            let expected_model: Model = Model::new(
                "player",
                &[
                    ModelField::new("id", DataType::Int64, Some(&["foreign", "unique"])),
                    ModelField::new("pid", DataType::Int32, Some(&["primary", "unique"])),
                    ModelField::new("positions", DataType::List(Box::new(DataType::String)), None),
                ],
            );
            assert_eq!(actual_model, expected_model);
        }
    }

    #[test]
    fn invalid_models() {
        assert_invalid_model(
            "
person:
    id:
",
        );
        assert_invalid_model(
            "
person:
    id: int6
",
        );
        assert_invalid_model(
            "
person:
    id: 
        constraints: [primary]
",
        );
        assert_invalid_model(
            "
person:
    id: 
        dtype: [primary]
",
        );
        assert_invalid_model(
            "
person:
    id: 
        type: int64
",
        );
    }
}
