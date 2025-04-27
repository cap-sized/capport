use crate::{
    logger::common::Logger,
    util::{common::yaml_from_str, error::CpResult},
};

use super::common::YamlRead;

pub fn parse_logger(name: &str, mut node: serde_yaml_ng::Value) -> CpResult<Logger> {
    node.add_to_map(yaml_from_str("label")?, yaml_from_str(name)?)?;
    Ok(serde_yaml_ng::from_value::<Logger>(node)?)
}
