use crate::config::common::Configurable;
use crate::task::transform::common::Transform;
use crate::task::transform::select::{SelectField, SelectTransform};
use crate::util::error::{CpError, CpResult, SubResult};
use std::collections::HashMap;
use std::{fmt, fs};
use yaml_rust2::Yaml;

use super::common::{YamlMapRead, YamlRead};

const SELECT_KEYWORD: &str = "select";
const ARGS_KEYWORD: &str = "args";
const ACTION_KEYWORD: &str = "action";
const KWARGS_KEYWORD: &str = "kwargs";

pub fn parse_select_field(name: &str, node: &Yaml) -> SubResult<SelectField> {
    let kwargs_key = Yaml::from_str(KWARGS_KEYWORD);
    if node.is_null() {
        return Err(format!("Field {} is null", name));
    }
    if !node.is_hash() {
        Ok(SelectField {
            label: String::from(name),
            action: None,
            args: node.clone(),
            kwargs: None,
        })
    } else {
        let node_map = node.to_map("Unexpected non-hash".to_owned()).unwrap();
        let action = node_map.get_str(ACTION_KEYWORD, format!("no action found for SelectField {}", name))?;
        Ok(SelectField {
            label: String::from(name),
            action: Some(action),
            args: match node_map.get(ARGS_KEYWORD) {
                Some(&x) => x.to_owned(),
                None => return Err(format!("args not found in SelectField {}", name)),
            },
            kwargs: node_map.get(KWARGS_KEYWORD).cloned().cloned(),
        })
    }
}

pub fn parse_select_transform(node: &Yaml) -> SubResult<SelectTransform> {
    match node.over_map(parse_select_field, format!("Transform config is not a map: {:?}", node)) {
        Ok(selects) => Ok(SelectTransform { selects }),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use yaml_rust2::{Yaml, YamlLoader};

    use crate::{
        task::transform::select::{SelectField, SelectTransform},
        util::common::yaml_from_str,
    };

    use super::parse_select_transform;

    #[test]
    fn valid_basic_select_transform() {
        let config = yaml_from_str(
            "
id: csid 
first_name: firstName.default 
last_name: lastName.default
",
        )
        .unwrap();
        let actual = parse_select_transform(&config).unwrap();
        let expected = SelectTransform::new(vec![
            SelectField::new("id", "csid"),
            SelectField::new("first_name", "firstName.default"),
            SelectField::new("last_name", "lastName.default"),
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_detailed_select_transform() {
        let config = yaml_from_str(
            "
first_name: firstName.default 
last_name: lastName.default
full_name: 
    action: concat_str # str concat, default with space
    args: [ firstName.default, lastName.default ] 
    kwargs:
        separator: \" \"
",
        )
        .unwrap();
        let actual = parse_select_transform(&config).unwrap();
        let expected = SelectTransform::new(vec![
            SelectField::new("first_name", "firstName.default"),
            SelectField::new("last_name", "lastName.default"),
            SelectField::from(
                "full_name",
                "[ firstName.default, lastName.default ]",
                Some("concat_str"),
                Some("{separator: \" \"}"),
            ),
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_no_kwargs_select_transform() {
        let config = yaml_from_str(
            "
person_id: csid # from the previous step
player_id: playerId
shoots_catches: shootsCatches
positions: 
    action: to_list
    args: position
      
",
        )
        .unwrap();
        let actual = parse_select_transform(&config).unwrap();
        let expected = SelectTransform::new(vec![
            SelectField::new("person_id", "csid"),
            SelectField::new("player_id", "playerId"),
            SelectField::new("shoots_catches", "shootsCatches"),
            SelectField::from("positions", "position", Some("to_list"), None),
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn invalid_missing_fields_map_select_transform() {
        [
            "
positions: 
    args: position
",
            "
positions: 
    action: to_list
",
            "
positions: 
    action: 
    args: position
",
            "
positions: 
    kwargs: 
        value: bad
",
        ]
        .iter()
        .for_each(|&s| {
            let config = yaml_from_str(s).unwrap();
            let actual = parse_select_transform(&config).unwrap_err();
        });
    }
}
