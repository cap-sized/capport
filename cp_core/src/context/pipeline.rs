use std::collections::HashMap;

use crate::{
    pipeline::common::PipelineConfig,
    task::stage::StageConfig,
    util::error::{CpError, CpResult},
};

use super::common::Configurable;

#[derive(Debug)]
pub struct PipelineRegistry {
    configs: HashMap<String, PipelineConfig>,
}

impl Default for PipelineRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineRegistry {
    pub fn new() -> PipelineRegistry {
        PipelineRegistry {
            configs: HashMap::new(),
        }
    }
    pub fn insert(&mut self, pipeline: PipelineConfig) -> Option<PipelineConfig> {
        let prev = self.configs.remove(&pipeline.label);
        self.configs.insert(pipeline.label.clone(), pipeline);
        prev
    }
    pub fn from(
        config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    ) -> CpResult<PipelineRegistry> {
        let mut reg = PipelineRegistry {
            configs: HashMap::new(),
        };
        reg.extract_parse_config(config_pack)?;
        Ok(reg)
    }
    pub fn get_pipeline_config(&self, pipeline_name: &str) -> Option<PipelineConfig> {
        self.configs.get(pipeline_name).map(|x| x.to_owned())
    }
}

impl Configurable for PipelineRegistry {
    fn get_node_name() -> &'static str {
        "pipeline"
    }
    fn extract_parse_config(
        &mut self,
        config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    ) -> CpResult<()> {
        let configs = config_pack
            .remove(PipelineRegistry::get_node_name())
            .unwrap_or_default();
        let mut errors = vec![];
        for (label, fields) in configs {
            match serde_yaml_ng::from_value::<Vec<StageConfig>>(fields) {
                Ok(stages) => {
                    self.configs.insert(
                        label.clone(),
                        PipelineConfig {
                            label: label.clone(),
                            stages,
                        },
                    );
                }
                Err(e) => {
                    errors.push(CpError::ConfigError(
                        "Pipeline",
                        format!("{}: {:?}", label, e.to_string()),
                    ));
                }
            };
        }
        if !errors.is_empty() {
            Err(CpError::ConfigError(
                "PipelineRegistry: pipeline",
                format!("Errors parsing:\n{:?}", errors),
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        parser::task_type::TaskTypeEnum, pipeline::common::PipelineConfig, task::stage::StageConfig,
        util::common::create_config_pack,
    };

    use super::PipelineRegistry;

    #[test]
    fn valid_unpack_pipeline_registry() {
        let configs = [
            "
pipeline:
  players:
    - label: load_player_ids
      task_type: source
      task_name: load_sources
      emplace: 
        fp_player_ids: nhl_player_ids.csv
        fp_state_province: state_province.csv
        df_state_province: STATE_PROVINCE

    - label: nhl_urls
      task_type: transform
      task_name: player_ids_to_urls # user defined
      emplace:
        input: PLAYER_IDS
        output: NHL_URLS
        url_column: nhl_url
irrelevant_node:
    for_testing:
        a: b
        ",
            "
pipeline:
  player_data:
    - label: load_full_data
      task_type: source
      task_name: http_json_get_batch_request # user defined
      emplace:
        input: NHL_URLS
        url_column: nhl_url
        output: NHL_PLAYER_DATA_RAW

    - label: nhl_player_data
      task_type: transform
      task_name: transform_nhl_player_data # default
      emplace:
        input: NHL_PLAYER_DATA_RAW
        state_province_df: STATE_PROVINCE
        output: NHL_PLAYER_DATA
",
        ];
        let mut config_pack = create_config_pack(configs);
        let actual = PipelineRegistry::from(&mut config_pack).unwrap();
        assert_eq!(
            actual.get_pipeline_config("players").unwrap(),
            PipelineConfig {
                label: "players".to_owned(),
                stages: vec![
                    StageConfig {
                        label: "load_player_ids".to_owned(),
                        task_type: TaskTypeEnum::Source,
                        task_name: "load_sources".to_owned(),
                        emplace: serde_yaml_ng::from_str(
                            "
fp_player_ids: nhl_player_ids.csv
fp_state_province: state_province.csv
df_state_province: STATE_PROVINCE
"
                        )
                        .unwrap()
                    },
                    StageConfig {
                        label: "nhl_urls".to_owned(),
                        task_type: TaskTypeEnum::Transform,
                        task_name: "player_ids_to_urls".to_owned(),
                        emplace: serde_yaml_ng::from_str(
                            "
input: PLAYER_IDS
output: NHL_URLS
url_column: nhl_url
"
                        )
                        .unwrap()
                    },
                ]
            }
        );
        assert_eq!(
            actual.get_pipeline_config("player_data").unwrap(),
            PipelineConfig {
                label: "player_data".to_owned(),
                stages: vec![
                    StageConfig {
                        label: "load_full_data".to_owned(),
                        task_type: TaskTypeEnum::Source,
                        task_name: "http_json_get_batch_request".to_owned(),
                        emplace: serde_yaml_ng::from_str(
                            "
input: NHL_URLS
url_column: nhl_url
output: NHL_PLAYER_DATA_RAW
"
                        )
                        .unwrap()
                    },
                    StageConfig {
                        label: "nhl_player_data".to_owned(),
                        task_type: TaskTypeEnum::Transform,
                        task_name: "transform_nhl_player_data".to_owned(),
                        emplace: serde_yaml_ng::from_str(
                            "
input: NHL_PLAYER_DATA_RAW
state_province_df: STATE_PROVINCE
output: NHL_PLAYER_DATA
"
                        )
                        .unwrap()
                    },
                ]
            }
        );
    }
}
