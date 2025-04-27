use crate::model::common::{DType, Model, ModelField};
use crate::util::common::yaml_from_str;
use crate::util::error::CpResult;

use super::common::YamlRead;

pub fn parse_model(name: &str, node: serde_yaml_ng::Value) -> CpResult<Model> {
    let key_val_list = node.to_str_val_vec()?;
    let mut fields = vec![];
    for (table_key, mut details) in key_val_list {
        if details.is_string() {
            // parse as a DType
            let dtype: DType = serde_yaml_ng::from_str(details.as_str().unwrap())?;
            fields.push(ModelField {
                label: table_key,
                dtype,
                constraints: None,
            });
        } else {
            details.add_to_map(yaml_from_str("label")?, yaml_from_str(&table_key)?)?;
            fields.push(serde_yaml_ng::from_value::<ModelField>(details)?);
        }
    }
    Ok(Model {
        name: name.to_owned(),
        fields,
    })
}
