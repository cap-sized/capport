use yaml_rust2::Yaml;

use crate::{
    transform::drop::{DropField, DropTransform},
    util::error::SubResult,
};

use super::common::YamlRead;

pub fn parse_drop_field(name: &str, node: &Yaml) -> SubResult<DropField> {
    if node.is_null() {
        return Err(format!("Field {} is null", name));
    }
    Ok(DropField {
        target: String::from(name),
        delete: node.as_bool().unwrap_or(node.as_str().unwrap_or("False") == "True"),
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
        transform::drop::{DropField, DropTransform},
        util::common::yaml_from_str,
    };

    use super::parse_drop_transform;

    #[test]
    fn valid_basic_drop_transform() {
        let config = yaml_from_str(
            "
first_name: true
last_name: true
full.name: True
",
        )
        .unwrap();
        let actual = parse_drop_transform(&config).unwrap();
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
first_name: True
last_name: 
",
        )
        .unwrap();
        let actual = parse_drop_transform(&config).unwrap_err();
    }
}
