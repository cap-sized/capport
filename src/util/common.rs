use std::collections::HashMap;

use yaml_rust2::{Yaml, YamlLoader};

pub const NYT: &str = "America/New_York";
pub const UTC: &str = "UTC";

pub fn create_config_pack(yaml_str: &str) -> HashMap<String, HashMap<String, Yaml>> {
    let configs = YamlLoader::load_from_str(yaml_str)
        .unwrap()
        .first()
        .unwrap()
        .as_hash()
        .unwrap()
        .iter()
        .map(|(name, yamlconf)| (name.as_str().unwrap().to_string(), yamlconf.to_owned()))
        .collect::<HashMap<String, Yaml>>();
    return HashMap::from([(String::from("model"), configs)]);
}
