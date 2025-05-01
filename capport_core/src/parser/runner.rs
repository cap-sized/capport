use log::warn;

use crate::{
    logger::common::DEFAULT_CONSOLE_LOGGER_NAME,
    pipeline::{common::RunnerConfig, runner::RunMethodType},
    util::{common::yaml_from_str, error::CpResult},
};

use super::common::YamlRead;

pub fn parse_runner(name: &str, mut node: serde_yaml_ng::Value) -> CpResult<RunnerConfig> {
    if node.is_null() {
        let default = RunnerConfig::new(name, DEFAULT_CONSOLE_LOGGER_NAME, RunMethodType::SyncLazy, None);
        warn!("Runner {} set to defaults: {:?}", name, default);
        return Ok(default);
    }
    node.add_to_map(yaml_from_str("label")?, yaml_from_str(name)?)?;
    let logger = serde_yaml_ng::from_value::<RunnerConfig>(node)?;
    Ok(logger)
}
