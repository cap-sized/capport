use std::collections::HashMap;

use yaml_rust2::{Yaml, YamlEmitter, YamlLoader};

use super::error::SubResult;

pub const NYT: &str = "America/New_York";
pub const UTC: &str = "UTC";

pub fn create_config_pack(yaml_str: &str, configurable: &str) -> HashMap<String, HashMap<String, Yaml>> {
    let configs = YamlLoader::load_from_str(yaml_str)
        .unwrap()
        .first()
        .unwrap()
        .as_hash()
        .unwrap()
        .iter()
        .map(|(name, yamlconf)| (name.as_str().unwrap().to_string(), yamlconf.to_owned()))
        .collect::<HashMap<String, Yaml>>();
    HashMap::from([(configurable.to_owned(), configs)])
}

pub fn yaml_from_str(s: &str) -> Option<Yaml> {
    YamlLoader::load_from_str(s).unwrap().first().cloned()
}

pub fn yaml_to_str(doc: &Yaml) -> SubResult<String> {
    let mut out_str = String::new();
    let mut emitter = YamlEmitter::new(&mut out_str);
    // dump the YAML object to a String
    match emitter.dump(doc) {
        Ok(_) => Ok(out_str),
        Err(e) => Err(format!("{:?}", e)),
    }
}

pub trait CpDefault {
    fn get_default() -> Self;
}
