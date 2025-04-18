use yaml_rust2::Yaml;

use crate::pipeline::common::Pipeline;
use crate::util::error::{CpError, CpResult};
use std::collections::HashMap;
use std::fs;

use crate::parser::pipeline::parse_pipeline;

use super::common::Configurable;

pub struct PipelineRegistry {
    registry: HashMap<String, Pipeline>,
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
        }
    }
    pub fn from(config_pack: &mut HashMap<String, HashMap<String, Yaml>>) -> PipelineRegistry {
        let mut reg = PipelineRegistry {
            registry: HashMap::new(),
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

#[cfg(test)]
mod tests {
    use crate::{
        pipeline::common::{HasTask, PipelineStage},
        task::noop::NoopTask,
        util::common::{create_config_pack, yaml_from_str},
    };

    use super::*;
    fn create_pipeline_registry(yaml_str: &str) -> PipelineRegistry {
        let mut config_pack = create_config_pack(yaml_str, "pipeline");
        PipelineRegistry::from(&mut config_pack)
    }

    fn assert_invalid_pipeline(yaml_str: &str) {
        let mut reg = PipelineRegistry::new();
        let mut config_pack = create_config_pack(yaml_str, "pipeline");
        reg.extract_parse_config(&mut config_pack).unwrap_err();
    }

    #[test]
    fn invalid_basic_pipeline() {
        assert_invalid_pipeline(
            "
mass_load_player:
",
        );
        assert_invalid_pipeline(
            "
mass_load_player:
    - label: load_state_province
      task: __noop
",
        );
        assert_invalid_pipeline(
            "
mass_load_player:
    label: load_state_province
    task: __noop
    args:
",
        );
        assert_invalid_pipeline(
            "
mass_load_player:
    - label: load_state_province
      task: __noop
      args: 
    - label: load_state_province
      task: __noop
      args: 
",
        );
    }

    #[test]
    fn valid_empty_stage_in_pipeline() {
        let reg = create_pipeline_registry(
            "
mass_load_player:
    - label: load_state_province
      task: __noop
      args:
",
        );
        {
            let actual_pipeline = reg.get_pipeline("mass_load_player").unwrap();
            let expected_pipeline: Pipeline = Pipeline::new(
                "mass_load_player",
                &[PipelineStage::new("load_state_province", "__noop", &Yaml::Null)],
            );
            assert_eq!(actual_pipeline, &expected_pipeline);
        }
    }

    #[test]
    fn valid_basic_pipeline() {
        let reg = create_pipeline_registry(
            "
mass_load_player:
    - label: load_state_province
      task: __noop
      args:
          - filepath: \"data/csv/state_province.csv\"
            model: state_province
            save_df: STATE_PROVINCE
          - filepath: \"data/csv/state_province.csv\"
            model: state_province
            save_df: STATE_PROVINCE

player_recon:
    - label: fetch_players
      task: __noop
      args:
          database: csdb
          table: players
          cmd: \"SELECT * FROM PLAYERS;\"
          model: player
          save_df: PLAYER
    - label: testing
      task: test_ping
      args:
          endpoint: 1.1.1.1
",
        );
        {
            let actual_pipeline = reg.get_pipeline("mass_load_player").unwrap();
            let expected_pipeline: Pipeline = Pipeline::new(
                "mass_load_player",
                &[PipelineStage::new(
                    "load_state_province",
                    "__noop",
                    &yaml_from_str(
                        "
- filepath: \"data/csv/state_province.csv\"
  model: state_province
  save_df: STATE_PROVINCE
- filepath: \"data/csv/state_province.csv\"
  model: state_province
  save_df: STATE_PROVINCE
        ",
                    )
                    .unwrap(),
                )],
            );
            assert_eq!(actual_pipeline, &expected_pipeline);
        }
        {
            let actual_pipeline = reg.get_pipeline("player_recon").unwrap();
            let expected_pipeline: Pipeline = Pipeline::new(
                "player_recon",
                &[
                    PipelineStage::new(
                        "fetch_players",
                        "__noop",
                        &yaml_from_str(
                            "
database: csdb
table: players
cmd: \"SELECT * FROM PLAYERS;\"
model: player
save_df: PLAYER
        ",
                        )
                        .unwrap(),
                    ),
                    PipelineStage::new(
                        "testing",
                        "test_ping",
                        &yaml_from_str(
                            "
endpoint: 1.1.1.1
        ",
                        )
                        .unwrap(),
                    ),
                ],
            );
            assert_eq!(actual_pipeline, &expected_pipeline);
        }
    }
}
