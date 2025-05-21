use std::collections::HashMap;

use polars::prelude::Schema;

use crate::{
    model::common::{ModelConfig, ModelFields},
    parser::keyword::{Keyword, ModelFieldKeyword, StrKeyword},
    util::error::{CpError, CpResult},
};

use super::common::Configurable;

#[derive(Debug)]
pub struct ModelRegistry {
    configs: HashMap<String, ModelConfig>,
}

impl ModelRegistry {
    pub fn new() -> ModelRegistry {
        ModelRegistry {
            configs: HashMap::new(),
        }
    }
    pub fn insert(&mut self, model: ModelConfig) -> Option<ModelConfig> {
        let prev = self.configs.remove(&model.label);
        self.configs.insert(model.label.clone(), model);
        prev
    }
    pub fn from(config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>) -> CpResult<ModelRegistry> {
        let mut reg = ModelRegistry {
            configs: HashMap::new(),
        };
        reg.extract_parse_config(config_pack)?;
        Ok(reg)
    }
    pub fn get_model(&self, model_name: &str) -> Option<ModelConfig> {
        self.configs.get(model_name).map(|x| x.to_owned())
    }
    pub fn get_substituted_model_fields(
        &self,
        model_name: &str,
        context: &serde_yaml_ng::Mapping,
    ) -> CpResult<ModelFields> {
        let result = self.get_model(model_name);
        let model = match result {
            Some(x) => x,
            None => {
                return Err(CpError::ConfigError(
                    "model not found",
                    format!("model `{}` not found", model_name),
                ));
            }
        };
        model.substitute_model_fields(context)
    }
}

impl Configurable for ModelRegistry {
    fn get_node_name() -> &'static str {
        "model"
    }
    fn extract_parse_config(
        &mut self,
        config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    ) -> CpResult<()> {
        let configs = config_pack.remove(ModelRegistry::get_node_name()).unwrap_or_default();
        let mut errors = vec![];
        for (config_name, fields) in configs {
            match serde_yaml_ng::from_value::<HashMap<StrKeyword, ModelFieldKeyword>>(fields) {
                Ok(model_fields) => {
                    self.configs.insert(
                        config_name.clone().into(),
                        ModelConfig {
                            label: config_name.into(),
                            fields: model_fields,
                        },
                    );
                }
                Err(e) => {
                    errors.push(CpError::ConfigError(
                        "Model",
                        format!("{}: {:?}", config_name, e.to_string()),
                    ));
                }
            };
        }
        if !errors.is_empty() {
            Err(CpError::ConfigError(
                "ModelRegistry: model",
                format!("Errors parsing:\n{:?}", errors),
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use polars::prelude::{DataType, TimeUnit};

    use crate::{context::common::Configurable, model::common::{ModelConfig, ModelFieldInfo}, parser::{dtype::DType, keyword::{Keyword, ModelFieldKeyword, StrKeyword}}, util::common::create_config_pack};

    use super::ModelRegistry;

    #[test]
    fn valid_unpack_model_registry() {
        let config = "
model:
    model_a: 
        $field1: int64
        field2: $value
    model_b: 
        dt: 
            datetime: America/New_York
        datetime: datetime_nyt
        utc: datetime_utc
irrelevant_node:
    for_testing:
        a: b
        ";
        let mut config_pack = create_config_pack([config]);
        let actual = ModelRegistry::from(&mut config_pack).unwrap();
        assert_eq!(actual.get_model("model_b").unwrap(), ModelConfig {
            label: "model_b".to_owned(),
            fields: HashMap::from([
                (StrKeyword::with_value("dt".to_owned()), ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(
                    DataType::Datetime(TimeUnit::Milliseconds, Some("America/New_York".into()))
                )))),
                (StrKeyword::with_value("datetime".to_owned()), ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(
                    DataType::Datetime(TimeUnit::Milliseconds, Some("America/New_York".into()))
                )))),
                (StrKeyword::with_value("utc".to_owned()), ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(
                    DataType::Datetime(TimeUnit::Milliseconds, Some("UTC".into()))
                )))),
            ])
        });
        assert_eq!(actual.get_model("model_a").unwrap(), ModelConfig {
            label: "model_a".to_owned(),
            fields: HashMap::from([
                (StrKeyword::with_symbol("field1"), ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::Int64)))),
                (StrKeyword::with_value("field2".to_owned()), ModelFieldKeyword::with_symbol("value")),
            ])
        });
        
    }

    #[test]
    fn valid_insert_substitute_model_registry() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let mut reg = ModelRegistry::new();
        reg.insert(ModelConfig {
            label: "model_a".to_owned(),
            fields: HashMap::from([
                (StrKeyword::with_symbol("field1"), ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::Int64)))),
                (StrKeyword::with_value("field2".to_owned()), ModelFieldKeyword::with_symbol("value")),
            ])
        });

        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>("
field1: sub1
value: 
    datetime: Asia/Tokyo
").unwrap();
        let fields = reg.get_substituted_model_fields("model_a", &context).unwrap();
        // TODO: 
        assert_eq!(fields, HashMap::from([
            (StrKeyword::with_value("sub1".to_owned()).and_symbol("field1"), ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::Int64)))),
            (StrKeyword::with_value("field2".to_owned()), ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(
                DataType::Datetime(TimeUnit::Milliseconds, Some("Asia/Tokyo".into()))
            ))).and_symbol("value")),
        ]));
    }
}
