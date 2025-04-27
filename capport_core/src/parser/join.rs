use std::collections::HashMap;

use crate::transform::join::{JType, JoinTransform};
use crate::util::error::{CpError, CpResult};

use super::common::YamlRead;
use super::select::parse_select_transform;

pub fn parse_join_transform(node: serde_yaml_ng::Value) -> CpResult<JoinTransform> {
    let mut join_args = node.to_str_map()?;
    let right_select = match join_args.remove("right_select") {
        Some(node) => parse_select_transform(node)?.selects,
        None => vec![],
    };
    let join = match join_args.remove("join") {
        Some(x) => x.as_str().unwrap_or("").to_owned(),
        None => {
            return Err(CpError::ConfigError(
                "Missing field in JoinTransform",
                "Missing `join` in args".to_owned(),
            ));
        }
    };
    let left_on = match join_args.remove("left_on") {
        Some(x) => x.to_val_vec_t::<String>(false)?,
        None => {
            return Err(CpError::ConfigError(
                "Missing field in JoinTransform",
                "Missing `left_on` in args".to_owned(),
            ));
        }
    };
    let right_on = match join_args.remove("right_on") {
        Some(x) => x.to_val_vec_t::<String>(false)?,
        None => {
            return Err(CpError::ConfigError(
                "Missing field in JoinTransform",
                "Missing `right_on` in args".to_owned(),
            ));
        }
    };
    let how = match join_args.remove("how") {
        Some(x) => serde_yaml_ng::from_value::<JType>(x)?,
        None => {
            return Err(CpError::ConfigError(
                "Missing field in JoinTransform",
                "Missing `how` in args".to_owned(),
            ));
        }
    };
    Ok(JoinTransform {
        join,
        right_select,
        how,
        left_on,
        right_on,
    })
}

#[cfg(test)]
mod tests {
    use polars::prelude::JoinType;

    use crate::{
        transform::{join::JoinTransform, select::SelectField},
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
        let actual = parse_join_transform(config).unwrap();
        let expected = JoinTransform::new(
            "test",
            "birth_state_province_name",
            "birth_state_province_name",
            &[
                SelectField::new("birth_state_province_code", "code"),
                SelectField::new("birth_state_province_name", "name"),
            ],
            JoinType::Left,
        );
        println!("{:?}", actual);
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
        let actual = parse_join_transform(config).unwrap();
        let expected = JoinTransform::new(
            "player",
            "firstName,lastName",
            "first,last",
            &[
                SelectField::new("first", "playerFirstName.default"),
                SelectField::new("last", "playerLastName.default"),
            ],
            JoinType::Right,
        );
        println!("{:?}", actual);
        assert_eq!(actual, expected);
    }
}
