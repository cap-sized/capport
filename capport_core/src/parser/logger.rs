use crate::{
    logger::common::Logger,
    util::{
        common::yaml_from_str,
        error::{CpError, CpResult},
    },
};

use super::common::YamlRead;

pub fn parse_logger(name: &str, mut node: serde_yaml_ng::Value) -> CpResult<Logger> {
    node.add_to_map(yaml_from_str("label")?, yaml_from_str(name)?)?;
    let logger = serde_yaml_ng::from_value::<Logger>(node)?;
    if logger._final_output_path.is_some() {
        return Err(CpError::ConfigError(
            "Invalid logger config",
            format!(
                "Value provided for reserved field _final_output_path in logger config {}, please remove",
                &name
            ),
        ));
    }
    Ok(logger)
}
