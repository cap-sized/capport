use yaml_rust2::Yaml;

use crate::config::common::Configurable;
use crate::model::common::{Model, ModelField};
use crate::util::common::{NYT, UTC};
use crate::util::error::{CpError, CpResult, SubResult};
use polars::datatypes::{DataType, TimeUnit, TimeZone};
use std::collections::HashMap;
use std::fs;

const CONSTRAINT_KEYWORD: &str = "constraints";
const DTYPE_KEYWORD: &str = "dtype";

pub struct ModelRegistry {
    registry: HashMap<String, Model>,
}

fn parse_dtype(dtype: &str) -> Option<polars::datatypes::DataType> {
    match dtype {
        "int" => Some(DataType::Int64),
        "int64" => Some(DataType::Int64),
        "int32" => Some(DataType::Int32),
        "float" => Some(DataType::Float32),
        "double" => Some(DataType::Float64),
        "str" => Some(DataType::String),
        "time" => Some(DataType::Time),
        "date" => Some(DataType::Date),
        "datetime_ny" => Some(DataType::Datetime(
            TimeUnit::Milliseconds,
            Some(TimeZone::from_str(NYT)),
        )),
        "datetime_utc" => Some(DataType::Datetime(
            TimeUnit::Milliseconds,
            Some(TimeZone::from_str(UTC)),
        )),
        "list[str]" => Some(DataType::List(Box::new(DataType::String))),
        "list[int]" => Some(DataType::List(Box::new(DataType::Int64))),
        "list[double]" => Some(DataType::List(Box::new(DataType::Float64))),
        _ => None,
    }
}

fn parse_model_field(name: &str, node: &Yaml) -> SubResult<ModelField> {
    let constraint_key = Yaml::from_str(CONSTRAINT_KEYWORD);
    let dtype_key = Yaml::from_str(DTYPE_KEYWORD);
    // TODO: figure out how to make these static
    if node.is_null() {
        return Err(format!("Field {} is null", name));
    }
    if !node.is_hash() {
        Ok(ModelField {
            label: name.to_string(),
            constraints: vec![],
            // TODO: handle invalid node type
            dtype: parse_dtype(node.as_str().unwrap()).unwrap(),
        })
    } else {
        let node_map = node.as_hash().unwrap();
        Ok(ModelField {
            label: name.to_string(),
            constraints: match node_map.get(&constraint_key) {
                Some(x) => x
                    .as_vec()
                    .unwrap()
                    .iter()
                    .map(|c| c.as_str().unwrap().to_string())
                    .collect::<Vec<_>>(),
                None => vec![],
            },
            // TODO: handle invalid node type
            dtype: match node_map.get(&dtype_key) {
                Some(x) => match parse_dtype(x.as_str().unwrap()) {
                    Some(dt) => dt,
                    None => {
                        return Err(format!("Field {}'s dtype is not recognised", name));
                    }
                },
                None => {
                    return Err(format!("Field {}'s dtype is not defined", name));
                }
            },
        })
    }
}

fn parse_model(name: &str, node: &Yaml) -> SubResult<Model> {
    let nodemap = match node.as_hash() {
        Some(x) => x.iter(),
        None => {
            return Err(format!("Model config {} is not a map: {:?}", name, node));
        }
    };
    let mut fields: Vec<ModelField> = vec![];
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
        let field = match parse_model_field(field_name, field) {
            Ok(mf) => mf,
            Err(e) => {
                return Err(e);
            }
        };
        fields.push(field);
    }
    Ok(Model {
        name: String::from(name),
        fields: fields,
    })
}

impl ModelRegistry {
    fn new() -> ModelRegistry {
        ModelRegistry {
            registry: HashMap::new(),
        }
    }
    fn get_model(&self, model_name: &str) -> Option<Model> {
        match self.registry.get(model_name) {
            Some(x) => Some(x.to_owned()),
            None => None,
        }
    }
}

impl Configurable for ModelRegistry {
    fn get_node_name() -> &'static str {
        "model"
    }
    fn extract_parse_config(&mut self, config_pack: &mut HashMap<String, HashMap<String, Yaml>>) -> CpResult<()> {
        let configs = config_pack
            .remove(ModelRegistry::get_node_name())
            .unwrap_or(HashMap::new());
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
    use yaml_rust2::YamlLoader;

    use super::*;
    fn create_model_registry(yaml_str: &str) -> ModelRegistry {
        let mut mr = ModelRegistry::new();
        let configs = YamlLoader::load_from_str(yaml_str)
            .unwrap()
            .first()
            .unwrap()
            .as_hash()
            .unwrap()
            .iter()
            .map(|(name, yamlconf)| (name.as_str().unwrap().to_string(), yamlconf.to_owned()))
            .collect::<HashMap<String, Yaml>>();
        let mut config_pack = HashMap::from([(String::from("model"), configs)]);
        mr.extract_parse_config(&mut config_pack).unwrap();
        mr
    }

    #[test]
    fn basic_model() {
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
            vec![
                ModelField::new("full_name", DataType::String, None),
                ModelField::new("first_name", DataType::String, None),
                ModelField::new("last_name", DataType::String, None),
            ],
        );
        assert_eq!(actual_model, expected_model);
    }

    #[test]
    fn basic_model_details() {
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
        let actual_model = mr.get_model("person").unwrap();
        let expected_model: Model = Model::new(
            "person",
            vec![
                ModelField::new("id", DataType::Int64, None),
                ModelField::new("pid", DataType::String, Some(vec!["primary"])),
                ModelField::new("full_name", DataType::String, None),
                ModelField::new("first_name", DataType::String, None),
                ModelField::new("last_name", DataType::String, None),
            ],
        );
        assert_eq!(actual_model, expected_model);
    }

    #[test]
    fn multiple_models() {
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
                vec![
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
                vec![
                    ModelField::new("id", DataType::Int64, Some(vec!["foreign", "unique"])),
                    ModelField::new("pid", DataType::Int32, Some(vec!["primary", "unique"])),
                    ModelField::new("positions", DataType::List(Box::new(DataType::String)), None),
                ],
            );
            assert_eq!(actual_model, expected_model);
        }
    }
}
