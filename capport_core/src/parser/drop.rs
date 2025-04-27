use crate::{
    transform::drop::{DropField, DropTransform},
    util::error::{CpError, CpResult},
};

use super::common::YamlRead;

pub fn parse_drop_transform(node: serde_yaml_ng::Value) -> CpResult<DropTransform> {
    let drop_field_map = node.to_str_val_vec()?;
    let mut drop_fields = vec![];
    for (to_field, is_drop) in drop_field_map {
        let delete: bool = match is_drop {
            serde_yaml_ng::Value::Null => true,
            serde_yaml_ng::Value::Bool(x) => x,
            serde_yaml_ng::Value::String(s) => {
                // Try to parse string into bool
                match s.to_lowercase().as_str() {
                    "true" => true,
                    "false" => false,
                    x => {
                        return Err(CpError::ConfigError(
                            "Invalid value for drop field",
                            format!("Must be true/false, received {}", x),
                        ));
                    }
                }
            }
            x => {
                return Err(CpError::ConfigError(
                    "Invalid value for drop field",
                    format!("Must be true/false, received {:?}", x),
                ));
            }
        };
        drop_fields.push(DropField {
            target: to_field,
            delete,
        });
    }
    Ok(DropTransform { deletes: drop_fields })
}

#[cfg(test)]
mod tests {
    use crate::{
        transform::drop::{DropField, DropTransform},
        util::common::yaml_from_str,
    };

    use super::parse_drop_transform;

    #[test]
    fn valid_basic_drop_transform() {
        let config = yaml_from_str(
            "
first_name: true
last_name: false
full.name: True
id: False
",
        )
        .unwrap();
        let actual = parse_drop_transform(config).unwrap();
        let expected = DropTransform::new(&[
            DropField::new("first_name"),
            DropField::new_inactive("last_name"),
            DropField::new("full.name"),
            DropField::new_inactive("id"),
        ]);
        println!("{:?}", actual);
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_basic_drop_transform_defaults() {
        let config = yaml_from_str(
            "
first_name: true
last_name: true
full.name: 
",
        )
        .unwrap();
        let actual = parse_drop_transform(config).unwrap();
        let expected = DropTransform::new(&[
            DropField::new("first_name"),
            DropField::new("last_name"),
            DropField::new("full.name"),
        ]);
        println!("{:?}", actual);
        assert_eq!(actual, expected);
    }

    #[test]
    fn invalid_basic_drop_transform() {
        let config = yaml_from_str(
            "
first_name, last_name
",
        )
        .unwrap();
        let _ = parse_drop_transform(config).unwrap_err();
    }

    #[test]
    fn invalid_value_drop_transform() {
        let config = yaml_from_str(
            "
first_name: badvalue
",
        )
        .unwrap();
        let _ = parse_drop_transform(config).unwrap_err();
    }
}
