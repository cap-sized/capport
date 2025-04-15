use polars::prelude::JoinType;
use yaml_rust2::Yaml;

use crate::config::common::Configurable;
use crate::model::common::{Model, ModelField};
use crate::task::transform::join::JoinTransform;
use crate::util::common::{NYT, UTC};
use crate::util::error::{CpError, CpResult, SubResult};
use std::collections::HashMap;
use std::fs;

use super::common::{YamlMapRead, YamlRead};
const LEFT_ON_KEYWORD: &str = "left_on";

pub fn parse_jointype(jtype: &str) -> Option<JoinType> {
    match jtype {
        "left" => Some(JoinType::Left),
        "right" => Some(JoinType::Right),
        "full" => Some(JoinType::Full),
        "cross" => Some(JoinType::Cross),
        "inner" => Some(JoinType::Inner),
        _ => None,
    }
}

pub fn parse_join_transform(node: &Yaml) -> SubResult<JoinTransform> {
    // match node.over_map(parse_select_field, format!("Transform config is not a map: {:?}", node)) {
    //     Ok(selects) => Ok(SelectTransform { selects }),
    //     Err(e) => Err(e),
    // }
    let nodemap = node.to_map(format!("Transform config is not a map: {:?}", node))?;
    let left_on = nodemap.get_str(
        LEFT_ON_KEYWORD,
        format!("left_on not found or invalid: {:?}", nodemap.get(LEFT_ON_KEYWORD)),
    )?;
    Err("not done".to_owned())
}
