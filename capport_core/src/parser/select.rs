use crate::transform::select::{SelectField, SelectTransform};
use crate::util::common::yaml_from_str;
use crate::util::error::{CpError, CpResult};

use super::common::YamlRead;
pub fn parse_select_transform(node: serde_yaml_ng::Value) -> CpResult<SelectTransform> {
    let select_field_map = node.to_str_val_vec()?;
    let mut select_fields = vec![];
    for (to_field, mut select_field) in select_field_map {
        if select_field.is_null() {
            select_field = yaml_from_str(&to_field)?;
        }
        if select_field.is_string() {
            select_fields.push(SelectField {
                action: None,
                args: select_field,
                label: to_field,
            });
        } else {
            select_field.add_to_map(yaml_from_str("label")?, yaml_from_str(&to_field)?)?;
            select_fields.push(match serde_yaml_ng::from_value(select_field) {
                Ok(x) => x,
                Err(e) => {
                    return Err(CpError::ConfigError(
                        "Error parsing SelectField value",
                        format!("Failed to parse {} as value: {:?}", to_field, &e),
                    ));
                }
            });
        }
    }
    Ok(SelectTransform { selects: select_fields })
}

#[cfg(test)]
mod tests {

    use crate::{
        transform::select::{SelectField, SelectTransform},
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
        let actual = parse_select_transform(config).unwrap();
        let expected = SelectTransform::new(&[
            SelectField::new("id", "csid"),
            SelectField::new("first_name", "firstName.default"),
            SelectField::new("last_name", "lastName.default"),
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_args_is_map_select_transform() {
        let config = yaml_from_str(
            "
first_name: firstName.default 
last_name: lastName.default
full_name: 
    action: concat_str # str concat, default with space
    args: 
        cols: [ firstName.default, lastName.default ] 
        separator: \" \"
",
        )
        .unwrap();
        let actual = parse_select_transform(config).unwrap();
        let expected = SelectTransform::new(&[
            SelectField::new("first_name", "firstName.default"),
            SelectField::new("last_name", "lastName.default"),
            SelectField::from(
                "full_name",
                "{ cols: [ firstName.default, lastName.default ], separator: \" \"}",
                Some("concat_str"),
            ),
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_args_not_map_select_transform() {
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
        let actual = parse_select_transform(config).unwrap();
        let expected = SelectTransform::new(&[
            SelectField::new("person_id", "csid"),
            SelectField::new("player_id", "playerId"),
            SelectField::new("shoots_catches", "shootsCatches"),
            SelectField::from("positions", "position", Some("to_list")),
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_args_no_action_select_transform() {
        [
            "
positions: 
    args: pos
",
            "
positions: 
    action: 
    args: pos
",
        ]
        .iter()
        .for_each(|&s| {
            let config = yaml_from_str(s).unwrap();
            let actual = parse_select_transform(config).unwrap();
            let expected = SelectTransform::new(&[SelectField::new("positions", "pos")]);
            assert_eq!(actual, expected);
        });
    }

    #[test]
    fn invalid_missing_fields_map_select_transform() {
        [
            "
positions: 
    action: to_list
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
            parse_select_transform(config).unwrap_err();
        });
    }

    #[test]
    fn valid_select_default_value() {
        let config = yaml_from_str(
            "
id: csid
first_name:
last_name:
",
        )
        .unwrap();
        let actual = parse_select_transform(config).unwrap();
        let expected = SelectTransform::new(&[
            SelectField::new("id", "csid"),
            SelectField::new("first_name", "first_name"),
            SelectField::new("last_name", "last_name"),
        ]);
        assert_eq!(actual, expected);
    }
}
