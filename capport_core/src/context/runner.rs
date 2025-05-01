use std::collections::HashMap;

use crate::{
    parser::runner::parse_runner,
    pipeline::common::RunnerConfig,
    util::error::{CpError, CpResult},
};

use super::common::Configurable;

pub struct RunnerRegistry {
    registry: HashMap<String, RunnerConfig>,
}

impl Default for RunnerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl RunnerRegistry {
    pub fn new() -> RunnerRegistry {
        RunnerRegistry {
            registry: HashMap::new(),
        }
    }
    pub fn insert(&mut self, runner: RunnerConfig) -> Option<RunnerConfig> {
        let prev = self.registry.remove(&runner.label);
        self.registry.insert(runner.label.clone(), runner);
        prev
    }
    pub fn from(config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>) -> CpResult<RunnerRegistry> {
        let mut reg = RunnerRegistry::new();
        reg.extract_parse_config(config_pack)?;
        Ok(reg)
    }
    pub fn get_runner(&self, runner_name: &str) -> Option<RunnerConfig> {
        self.registry.get(runner_name).map(|x| x.to_owned())
    }
}

impl Configurable for RunnerRegistry {
    fn get_node_name() -> &'static str {
        "runner"
    }
    fn extract_parse_config(
        &mut self,
        config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    ) -> CpResult<()> {
        let configs = config_pack.remove(RunnerRegistry::get_node_name()).unwrap_or_default();
        for (config_name, node) in configs {
            let model = match parse_runner(&config_name, node) {
                Ok(x) => x,
                Err(e) => {
                    return Err(CpError::ComponentError(
                        "config.runner",
                        format!["Runner {}: {}", config_name, e],
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
    use crate::{
        context::common::Configurable,
        logger::common::DEFAULT_CONSOLE_LOGGER_NAME,
        pipeline::{common::RunnerConfig, runner::RunMethodType},
        util::common::create_config_pack,
    };

    use super::RunnerRegistry;

    fn create_runner_registry(yaml_str: &str) -> RunnerRegistry {
        let mut config_pack = create_config_pack(yaml_str, RunnerRegistry::get_node_name());
        RunnerRegistry::from(&mut config_pack).unwrap()
    }

    #[test]
    fn valid_basic_runner() {
        let reg = create_runner_registry(
            "
default:
    logger: default
    run_method: sync_lazy
",
        );
        let actual_config = reg.get_runner("default").unwrap();
        let expected_config = RunnerConfig::new("default", "default", RunMethodType::SyncLazy, None);
        assert_eq!(actual_config, expected_config);
    }

    #[test]
    fn valid_default_runner() {
        let reg = create_runner_registry(
            "
default:
",
        );
        let actual_config = reg.get_runner("default").unwrap();
        let expected_config = RunnerConfig::new("default", DEFAULT_CONSOLE_LOGGER_NAME, RunMethodType::SyncLazy, None);
        assert_eq!(actual_config, expected_config);
    }
}
