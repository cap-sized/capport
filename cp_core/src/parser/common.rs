use std::collections::HashMap;

use serde::de::DeserializeOwned;

use crate::util::error::{CpError, CpResult};

pub trait YamlRead {
    fn to_str_map(self) -> CpResult<HashMap<String, serde_yaml_ng::Value>>;
    fn to_val_vec(self, is_strict: bool) -> CpResult<Vec<serde_yaml_ng::Value>>;
    fn to_val_vec_t<T>(self, is_strict: bool) -> CpResult<Vec<T>>
    where
        T: DeserializeOwned;
    fn to_str_val_vec(self) -> CpResult<Vec<(String, serde_yaml_ng::Value)>>;
    fn add_to_map(&mut self, key: serde_yaml_ng::Value, value: serde_yaml_ng::Value) -> CpResult<()>;
}

impl YamlRead for serde_yaml_ng::Value {
    fn to_str_map(self) -> CpResult<HashMap<String, serde_yaml_ng::Value>> {
        match self {
            serde_yaml_ng::Value::Mapping(map) => {
                let mut result = HashMap::new();
                for (k, v) in map {
                    if let serde_yaml_ng::Value::String(field_name) = k {
                        result.insert(field_name, v);
                    } else {
                        return Err(CpError::ConfigError(
                            "Invalid field key",
                            format!("Field keys must be strings, received: {:?}", v),
                        ));
                    }
                }
                Ok(result)
            }
            value => Err(CpError::ConfigError(
                "Not a mapping",
                format!("Expected a mapping inside node block, received: {:?}", &value),
            )),
        }
    }
    fn to_val_vec(self, is_strict: bool) -> CpResult<Vec<serde_yaml_ng::Value>> {
        match self {
            serde_yaml_ng::Value::Sequence(seq) => Ok(seq),
            value => {
                if is_strict {
                    Err(CpError::ConfigError(
                        "Not a sequence",
                        format!("Expected a sequence inside node block, received: {:?}", &value),
                    ))
                } else {
                    Ok(vec![value])
                }
            }
        }
    }
    fn to_val_vec_t<T>(self, is_strict: bool) -> CpResult<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let val_vec = self.to_val_vec(is_strict)?;
        let mut val_vec_t = vec![];
        for val in val_vec {
            val_vec_t.push(serde_yaml_ng::from_value::<T>(val)?);
        }
        Ok(val_vec_t)
    }
    fn to_str_val_vec(self) -> CpResult<Vec<(String, serde_yaml_ng::Value)>> {
        match self {
            serde_yaml_ng::Value::Mapping(map) => {
                let mut result = Vec::new();
                for (k, v) in map {
                    if let serde_yaml_ng::Value::String(field_name) = k {
                        result.push((field_name, v));
                    } else {
                        return Err(CpError::ConfigError(
                            "Invalid field key",
                            format!("Field keys must be strings, received: {:?}", v),
                        ));
                    }
                }
                Ok(result)
            }
            value => Err(CpError::ConfigError(
                "Not a mapping",
                format!("Expected a mapping inside node block, received: {:?}", &value),
            )),
        }
    }
    fn add_to_map(&mut self, key: serde_yaml_ng::Value, value: serde_yaml_ng::Value) -> CpResult<()> {
        match self {
            serde_yaml_ng::Value::Mapping(map) => {
                map.insert(key, value);
                Ok(())
            }
            value => Err(CpError::ConfigError(
                "Not a mapping",
                format!("Expected a mapping inside node block, received: {:?}", &value),
            )),
        }
    }
}
