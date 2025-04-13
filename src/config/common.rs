use yaml_rust2::{Yaml, YamlLoader};

use crate::util::error::{CpError, CpResult};
use std::{collections::HashMap, iter::Map, path::PathBuf};

pub trait Configurable {
    fn get_node_name() -> &'static str;
}

pub struct RawConfigPack {}

pub fn read_configs(dir: &str, file_exts: Vec<&str>) -> CpResult<Vec<PathBuf>> {
    let paths = std::fs::read_dir(dir)?
        .filter_map(|res| res.ok())
        .map(|dir_entry| dir_entry.path())
        .filter_map(|path| {
            if path
                .extension()
                .map_or(false, |ext| file_exts.contains(&ext.to_str().unwrap()))
            {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    Ok(paths)
}

fn unpack_yaml_map(
    config: &Yaml,
    config_pack: &mut HashMap<String, HashMap<String, Yaml>>,
    config_type: &str,
) -> CpResult<()> {
    let config_map = match config.as_hash() {
        Some(x) => x.iter(),
        None => {
            return Err(CpError::ComponentError(
                "config.common",
                format!("Found a {} node that is not a map", config_type),
            ));
        }
    };
    for (key_node, c) in config_map {
        let pack = config_pack.get_mut(config_type).unwrap();
        let key = match key_node.as_str() {
            Some(val) => val,
            None => {
                return Err(CpError::ComponentError(
                    "config.common",
                    format!(
                        "key {:?} of config_type {} is not a string",
                        key_node, config_type
                    ),
                ));
            }
        };
        if pack.contains_key(key) {
            return Err(CpError::ComponentError(
                "config.common",
                format!(
                    "Invalid config: duplicate key for {} ({})",
                    config_type, key,
                ),
            ));
        }
        if !c.is_null() {
            pack.insert(String::from(key), Yaml::clone(c));
        }
    }
    Ok(())
}

fn pack_configs(
    configs: &Vec<Yaml>,
) -> CpResult<HashMap<String, HashMap<String, Yaml>>> {
    let mut config_pack: HashMap<String, HashMap<String, Yaml>> =
        HashMap::new();
    for config in configs {
        let config_map = match config.as_hash() {
            Some(x) => x.iter(),
            None => {
                return Err(CpError::ComponentError(
                    "config.common",
                    String::from("All config files must be maps"),
                ));
            }
        };
        for (config_type_node, c) in config_map {
            let ctype = config_type_node.as_str().unwrap();
            if !config_pack.contains_key(ctype) {
                config_pack.insert(String::from(ctype), HashMap::new());
            }
            match unpack_yaml_map(c, &mut config_pack, ctype) {
                Ok(()) => (),
                Err(e) => return Err(e),
            }
        }
    }
    Ok(config_pack)
}

pub fn pack_configs_from_files(
    files: &Vec<PathBuf>,
) -> CpResult<HashMap<String, HashMap<String, Yaml>>> {
    let configs: Vec<Yaml> = files
        .iter()
        .flat_map(|path| {
            let config_str = std::fs::read_to_string(path).unwrap();
            YamlLoader::load_from_str(&config_str).unwrap()
        })
        .collect();
    pack_configs(&configs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_yaml_sample(s: &str) -> Vec<Yaml> {
        YamlLoader::load_from_str(s).unwrap()
    }

    #[test]
    fn invalid_config_nodes_list() {
        let configs = get_yaml_sample(
            "
foo:
    - list1
    - list2
bar:
    - bar1
    - bar2.0
",
        );
        pack_configs(&configs).unwrap_err();
    }

    #[test]
    fn invalid_config_list_nodes() {
        let configs = get_yaml_sample(
            "
- foo:
    list1: 
    list2: 
- bar:
    bar1: 
    bar2.0:
",
        );
        pack_configs(&configs).unwrap_err();
    }

    #[test]
    fn invalid_config_null_nodes() {
        let configs = get_yaml_sample(
            "
foo:
    list1: 
    list2: 
bar:
",
        );
        pack_configs(&configs).unwrap_err();
    }

    #[test]
    fn invalid_config_invalid_keys() {
        let configs = get_yaml_sample(
            "
foo:
    list1: 
    list2: 
bar:
    1: 
    2.0:
",
        );
        pack_configs(&configs).unwrap_err();
    }

    #[test]
    fn valid_config_list_nodes() {
        let configs = get_yaml_sample(
            "
foo:
    list1: a
    list2: b
bar:
    BarA: x
    BarB2.0: b
    x: 
",
        );
        let result = pack_configs(&configs).unwrap();
        let mut expected = HashMap::new();
        expected.insert(
            String::from("foo"),
            HashMap::from([
                (String::from("list1"), Yaml::from_str("a")),
                (String::from("list2"), Yaml::from_str("b")),
            ]),
        );
        expected.insert(
            String::from("bar"),
            HashMap::from([
                (String::from("BarA"), Yaml::from_str("x")),
                (String::from("BarB2.0"), Yaml::from_str("b")),
            ]),
        );
        assert_eq!(result, expected);
    }
}
