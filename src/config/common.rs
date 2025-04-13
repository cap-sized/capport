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
) {
    config.as_hash().unwrap().iter().for_each(|(key_node, c)| {
        let pack = config_pack.get_mut(config_type).unwrap();
        let key = key_node.as_str().unwrap();
        if pack.contains_key(key) {
            CpError::ComponentError(
                "config.common",
                format!(
                    "Invalid config: duplicate key for {} ({})",
                    config_type, key,
                ),
            );
        }
        if !c.is_null() {
            pack.insert(String::from(key), Yaml::clone(c));
        }
    });
}

pub fn sort_configs(
    files: &Vec<PathBuf>,
) -> CpResult<HashMap<String, HashMap<String, Yaml>>> {
    let configs: Vec<Yaml> = files
        .iter()
        .flat_map(|path| {
            let config_str = std::fs::read_to_string(path).unwrap();
            YamlLoader::load_from_str(&config_str).unwrap()
        })
        .collect();
    let mut config_pack: HashMap<String, HashMap<String, Yaml>> =
        HashMap::new();
    configs.iter().for_each(|config| {
        config
            .as_hash()
            .unwrap()
            .iter()
            .for_each(|(config_type_node, c)| {
                let ctype = config_type_node.as_str().unwrap();
                if !config_pack.contains_key(ctype) {
                    config_pack.insert(String::from(ctype), HashMap::new());
                }
                unpack_yaml_map(c, &mut config_pack, ctype);
            });
    });

    Ok(config_pack)
}
