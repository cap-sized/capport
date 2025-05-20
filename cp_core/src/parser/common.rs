use crate::util::error::{CpError, CpResult};

pub trait YamlRead {
    fn add_to_map(&mut self, key: serde_yaml_ng::Value, value: serde_yaml_ng::Value) -> CpResult<()>;
}

impl YamlRead for serde_yaml_ng::Value {
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
