use serde::{Deserialize, de};
use yaml_rust2::Yaml;

use crate::util::{
    common::yaml_to_str,
    error::{CpError, CpResult},
};

pub fn yaml_to_task_arg_str(yaml: &Yaml, task_name: &'static str) -> CpResult<String> {
    match yaml_to_str(yaml) {
        Ok(x) => Ok(x),
        Err(e) => Err(CpError::TaskError(task_name, e.to_string())),
    }
}

pub fn deserialize_arg_str<'de, T>(arg_str: &'de str, task_name: &'static str) -> CpResult<T>
where
    T: Deserialize<'de>,
{
    match serde_yaml_ng::from_str(arg_str) {
        Ok(x) => Ok(x),
        Err(e) => Err(CpError::TaskError(task_name, e.to_string())),
    }
}
