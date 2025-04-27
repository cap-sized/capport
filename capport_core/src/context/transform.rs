use crate::transform::common::{RootTransform, Transform};
use crate::util::error::{CpError, CpResult};
use std::collections::HashMap;
use std::fmt;

use crate::parser::transform::parse_root_transform;

use super::common::Configurable;

pub struct TransformRegistry {
    registry: HashMap<String, RootTransform>,
}

impl Default for TransformRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for TransformRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _ = write!(f, "TransformRegistry: [ ");
        for (key, trf) in &self.registry {
            let _ = write!(f, "{} : ", key);
            let _ = trf.fmt(f);
            let _ = write!(f, ", ");
        }
        write!(f, " ]")
    }
}

impl TransformRegistry {
    pub fn new() -> TransformRegistry {
        TransformRegistry {
            registry: HashMap::new(),
        }
    }
    pub fn from(
        config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    ) -> CpResult<TransformRegistry> {
        let mut reg = TransformRegistry::default();
        reg.extract_parse_config(config_pack)?;
        Ok(reg)
    }
    pub fn insert(&mut self, transform: RootTransform) -> Option<RootTransform> {
        let prev = self.registry.remove(&transform.label);
        self.registry.insert(transform.label.clone(), transform);
        prev
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
    fn extract_parse_config(
        &mut self,
        config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    ) -> CpResult<()> {
        let configs = config_pack
            .remove(TransformRegistry::get_node_name())
            .unwrap_or_default();
        for (config_name, node) in configs {
            let model = match parse_root_transform(&config_name, node) {
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
    use std::sync::{Arc, RwLock};

    use polars::{df, prelude::LazyFrame};
    use polars_lazy::frame::IntoLazy;

    use crate::{
        pipeline::results::PipelineResults,
        transform::select::{SelectField, SelectTransform},
        util::common::create_config_pack,
    };

    use super::*;
    fn create_transform_registry(yaml_str: &str) -> TransformRegistry {
        let mut config_pack = create_config_pack(yaml_str, "transform");
        TransformRegistry::from(&mut config_pack).unwrap()
    }

    fn assert_invalid_transform(yaml_str: &str) {
        let mut reg = TransformRegistry::new();
        let mut config_pack = create_config_pack(yaml_str, "transform");
        reg.extract_parse_config(&mut config_pack).unwrap_err();
    }

    #[test]
    fn valid_empty_transform() {
        let tr = TransformRegistry::default();
        assert!(tr.registry.is_empty());
    }

    #[test]
    fn invalid_non_list_transform() {
        assert_invalid_transform(
            "
player_to_person:
    select:
        id: csid 
",
        );
    }

    #[test]
    fn valid_one_stage_mapping_transform() {
        let sample_df = df![
            "csid" => [1, 2, 3],
            "test" => [1, 2, 3],
        ]
        .unwrap()
        .lazy();
        let tr = create_transform_registry(
            "
player_to_person:
    - select:
        id: csid 
",
        );
        println!("{:?}", tr);
        let actual_transform = tr.get_transform("player_to_person").unwrap();
        let expected_transform: RootTransform = RootTransform::new(
            "player_to_person",
            vec![Box::new(SelectTransform::new(&[SelectField::new("id", "csid")]))],
        );
        let results = Arc::new(RwLock::new(PipelineResults::<LazyFrame>::default()));
        let actual_result = actual_transform
            .run_lazy(sample_df.clone(), results.clone())
            .unwrap()
            .collect()
            .unwrap();
        let expected_result = expected_transform
            .run_lazy(sample_df.clone(), results.clone())
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(actual_result, expected_result);
    }
}
