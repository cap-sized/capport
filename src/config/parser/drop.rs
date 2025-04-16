use yaml_rust2::Yaml;

use crate::{
    task::transform::drop::{DropField, DropTransform},
    util::error::SubResult,
};

use super::common::YamlRead;

pub fn parse_drop_field(name: &str, node: &Yaml) -> SubResult<DropField> {
    if node.is_null() {
        return Err(format!("Field {} is null", name));
    }
    Ok(DropField {
        target: String::from(name),
        delete: node.as_bool().unwrap_or(false),
    })
}

pub fn parse_drop_transform(node: &Yaml) -> SubResult<DropTransform> {
    match node.over_map(
        parse_drop_field,
        format!("DropTransform config is not a map: {:?}", node),
    ) {
        Ok(deletes) => Ok(DropTransform { deletes }),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        task::transform::drop::{DropField, DropTransform},
        util::common::yaml_from_str,
    };

    use super::parse_drop_transform;

    fn valid_basic_drop_transform() {
        let config = yaml_from_str(
            "
first_name: True
last_name: True
full.name: True
",
        )
        .unwrap();
        let actual = parse_drop_transform(&config).unwrap();
        let expected = DropTransform::new(vec![
            DropField::new("first_name"),
            DropField::new("last_name"),
            DropField::new("full.name"),
        ]);
        assert_eq!(actual, expected);
    }

    fn invalid_basic_drop_transform() {
        let config = yaml_from_str(
            "
first_name: True
last_name: 
",
        )
        .unwrap();
        let actual = parse_drop_transform(&config).unwrap_err();
    }
}
