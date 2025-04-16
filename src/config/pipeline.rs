use yaml_rust2::Yaml;

use crate::config::common::Configurable;
use crate::pipeline::common::Pipeline;
use crate::pipeline::task::TaskDictionary;
use crate::util::common::CpDefault;
use crate::util::error::{CpError, CpResult};
use std::collections::HashMap;
use std::fs;

use super::parser::pipeline::parse_pipeline;

pub struct PipelineRegistry {
    registry: HashMap<String, Pipeline>,
    task_dictionary: TaskDictionary,
}

impl Default for PipelineRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineRegistry {
    pub fn new() -> PipelineRegistry {
        PipelineRegistry {
            registry: HashMap::new(),
            task_dictionary: TaskDictionary::get_default(),
        }
    }
    pub fn from(config_pack: &mut HashMap<String, HashMap<String, Yaml>>) -> PipelineRegistry {
        let mut reg = PipelineRegistry {
            registry: HashMap::new(),
            task_dictionary: TaskDictionary::get_default(),
        };
        reg.extract_parse_config(config_pack).unwrap();
        reg
    }
    pub fn get_pipeline(&self, pipeline_name: &str) -> Option<&Pipeline> {
        match self.registry.get(pipeline_name) {
            Some(x) => Some(x),
            None => None,
        }
    }
}

impl Configurable for PipelineRegistry {
    fn get_node_name() -> &'static str {
        "pipeline"
    }
    fn extract_parse_config(&mut self, config_pack: &mut HashMap<String, HashMap<String, Yaml>>) -> CpResult<()> {
        let configs = config_pack
            .remove(PipelineRegistry::get_node_name())
            .unwrap_or_default();
        for (config_name, node) in configs {
            let pipeline = match parse_pipeline(&config_name, &node) {
                Ok(x) => x,
                Err(e) => {
                    return Err(CpError::ComponentError(
                        "config.pipeline",
                        format!("Pipeline {}: {}", config_name, e),
                    ));
                }
            };
            self.registry.insert(config_name.to_string(), pipeline);
        }
        Ok(())
    }
}
