use yaml_rust2::Yaml;

use crate::{
    task::transform::{
        common::{RootTransform, Transform},
        select::SelectTransform,
    },
    util::error::SubResult,
};

use super::{common::YamlRead, select::parse_select_transform};

const ALLOWED_NODES: [&str; 1] = [SelectTransform::keyword()];

pub fn parse_root_transform(name: &str, node: &Yaml) -> SubResult<RootTransform> {
    let stages_configs = node.to_list(format!(
        "Child of transform node {} is not a list of select/join/drop nodes, invalid: {:?}",
        name, node
    ))?;

    let mut stages: Vec<Box<dyn Transform>> = vec![];
    for raw_config in stages_configs {
        let config = raw_config.to_map(format!(
            "Stage of transform node {} is not a map/join/drop node: {:?}",
            name, raw_config,
        ))?;
        if config.len() != 1 {
            return Err(format!(
                "Stage of transform node {} does not have exactly one key of {:?} (keys: {:?}, len={})",
                name,
                ALLOWED_NODES,
                &config.keys(),
                &config.len()
            ));
        }
        if config.contains_key(SelectTransform::keyword()) {
            match parse_select_transform(config.get(SelectTransform::keyword()).unwrap()) {
                Ok(x) => stages.push(Box::new(x)),
                Err(e) => return Err(e),
            };
        } else {
            return Err(format!(
                "Stage of transform node {} does not have exactly one key of {:?} (keys: {:?})",
                name,
                ALLOWED_NODES,
                &config.keys()
            ));
        }
    }
    Ok(RootTransform::new(name, stages))
}
