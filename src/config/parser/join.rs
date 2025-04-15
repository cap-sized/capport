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
use super::select::parse_select_field;
const LEFT_ON_KEYWORD: &str = "left_on";
const RIGHT_ON_KEYWORD: &str = "right_on";
const RIGHT_SELECT_KEYWORD: &str = "right_select";
const HOW_KEYWORD: &str = "how";
const JOIN_KEYWORD: &str = "join";

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
    let nodemap = node.to_map(format!("Transform config is not a map: {:?}", node))?;
    let join = nodemap.get_str(
        JOIN_KEYWORD,
        format!("`join` (name of table to join on) not found or invalid str: {:?}", nodemap.get(HOW_KEYWORD)),
    )?;
    let left_on = nodemap.get_list_str(
        LEFT_ON_KEYWORD,
        format!(
            "`left_on` not found or invalid str/list[str]: {:?}",
            nodemap.get(LEFT_ON_KEYWORD)
        ),
    )?;
    let right_on = nodemap.get_list_str(
        RIGHT_ON_KEYWORD,
        format!(
            "`right_on` not found or invalid str/list[str]: {:?}",
            nodemap.get(RIGHT_ON_KEYWORD)
        ),
    )?;
    let how = match nodemap.get_str(
        HOW_KEYWORD,
        format!("how not found or invalid str: {:?}", nodemap.get(HOW_KEYWORD)),
    ) {
        Ok(x) => match parse_jointype(&x) {
            Some(jointype) => jointype,
            None => return Err(format!("invalid jointype: {:?}", x)),
        },
        Err(e) => return Err(e),
    };
    let right_select = match nodemap.get(RIGHT_SELECT_KEYWORD) {
        Some(&x) => x.over_map(
            parse_select_field,
            format!("`right_select` is not a valid map of SelectFields: {:?}", node),
        )?,
        None => return Err(format!("`right_select` not found: {:?}", nodemap)),
    };
    Ok(JoinTransform {
        join,
        how,
        left_on,
        right_on,
        right_select,
    })
}

#[cfg(test)]
mod tests {
    use polars::prelude::JoinType;

    use crate::{
        task::transform::{join::JoinTransform, select::SelectField},
        util::common::yaml_from_str,
    };

    use super::parse_join_transform;

    #[test]
    fn valid_basic_join_transform() {
        let config = yaml_from_str(
            "
join: test
right_select:
    birth_state_province_code: code
    birth_state_province_name: name
left_on: birth_state_province_name
right_on: birth_state_province_name
how: left
",
        )
        .unwrap();
        let actual = parse_join_transform(&config).unwrap();
        let expected = JoinTransform::new(
            "test",
            "birth_state_province_name",
            "birth_state_province_name",
            vec![
                SelectField::new("birth_state_province_code", "code"),
                SelectField::new("birth_state_province_name", "name"),
            ],
            JoinType::Left,
        );
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_multiple_on_transform() {
        let config = yaml_from_str(
            "
join: player
right_select:
    first: playerFirstName.default
    last: playerLastName.default
left_on: [ firstName, lastName ]
right_on: [ first, last ]
how: right
",
        )
        .unwrap();
        let actual = parse_join_transform(&config).unwrap();
        let expected = JoinTransform::new(
            "player",
            "firstName,lastName",
            "first,last",
            vec![
                SelectField::new("first", "playerFirstName.default"),
                SelectField::new("last", "playerLastName.default"),
            ],
            JoinType::Right,
        );
        assert_eq!(actual, expected);
    }
}
