use yaml_rust2::Yaml;

use crate::model::common::{Model, ModelField};
use crate::util::common::{NYT, UTC};
use crate::util::error::SubResult;
use polars::datatypes::{DataType, TimeUnit, TimeZone};

use super::common::{YamlMapRead, YamlRead};

const CONSTRAINT_KEYWORD: &str = "constraints";
const DTYPE_KEYWORD: &str = "dtype";

pub fn parse_dtype(dtype: &str) -> Option<polars::datatypes::DataType> {
    match dtype {
        "int" => Some(DataType::Int64),
        "int8" => Some(DataType::Int8),
        "int16" => Some(DataType::Int16),
        "int32" => Some(DataType::Int32),
        "int64" => Some(DataType::Int64),
        "uint8" => Some(DataType::UInt8),
        "uint16" => Some(DataType::UInt16),
        "uint32" => Some(DataType::UInt32),
        "uint64" => Some(DataType::UInt64),
        "float" => Some(DataType::Float32),
        "double" => Some(DataType::Float64),
        "str" => Some(DataType::String),
        "char" => Some(DataType::UInt8),
        "time" => Some(DataType::Time),
        "date" => Some(DataType::Date),
        "datetime_nyt" => Some(DataType::Datetime(
            TimeUnit::Milliseconds,
            Some(TimeZone::from_str(NYT)),
        )),
        "datetime_utc" => Some(DataType::Datetime(
            TimeUnit::Milliseconds,
            Some(TimeZone::from_str(UTC)),
        )),
        "list[str]" => Some(DataType::List(Box::new(DataType::String))),
        "list[int]" => Some(DataType::List(Box::new(DataType::Int64))),
        "list[double]" => Some(DataType::List(Box::new(DataType::Float64))),
        _ => None,
    }
}

pub fn parse_model_field(name: &str, node: &Yaml) -> SubResult<ModelField> {
    if node.is_null() {
        return Err(format!("Field {} is null", name));
    }
    if !node.is_hash() {
        let raw_dtype = node.to_str(format!("Field {:?} is not a str", &node))?;
        Ok(ModelField {
            label: name.to_string(),
            constraints: vec![],
            dtype: match parse_dtype(&raw_dtype) {
                Some(x) => x,
                None => return Err(format!("Field {} contains invalid dtype {:?}", name, node)),
            },
        })
    } else {
        let node_map = node.to_map(format!(
            "Expected key {} to have the fields '{}' and (optional) '{}': {:?}",
            name, DTYPE_KEYWORD, CONSTRAINT_KEYWORD, node
        ))?;
        let constraints = match node_map.get(CONSTRAINT_KEYWORD) {
            Some(x) => match x.to_list_str(format!(
                "Expected constraints of {} to be a list of keywords, received: {:?}",
                name, &x
            )) {
                Ok(xx) => xx,
                Err(e) => {
                    return Err(e);
                }
            },
            None => vec![],
        };
        Ok(ModelField {
            label: name.to_string(),
            constraints,
            dtype: match node_map.get_str(DTYPE_KEYWORD, format!("Field {}'s dtype is not defined", name)) {
                Ok(x) => match parse_dtype(&x) {
                    Some(dt) => dt,
                    None => {
                        return Err(format!("Field {}'s dtype is not recognised", name));
                    }
                },
                Err(e) => {
                    return Err(e);
                }
            },
        })
    }
}

pub fn parse_model(name: &str, node: &Yaml) -> SubResult<Model> {
    match node.over_map(
        parse_model_field,
        format!("Model config {} is not a map: {:?}", name, node),
    ) {
        Ok(fields) => Ok(Model {
            name: String::from(name),
            fields,
        }),
        Err(e) => Err(e),
    }
}
