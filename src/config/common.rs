use yaml_rust2::{Yaml, YamlLoader};

use crate::util::error::{CpError, CpResult, SubResult};
use std::{collections::HashMap, iter::Map, path::PathBuf};

use super::parser::common::YamlRead;

pub trait Configurable {
    fn get_node_name() -> &'static str;
    fn extract_parse_config(&mut self, config_pack: &mut HashMap<String, HashMap<String, Yaml>>) -> CpResult<()>;
}

pub fn read_configs(dir: &str, file_exts: &[&str]) -> CpResult<Vec<PathBuf>> {
    let paths = std::fs::read_dir(dir)?
        .filter_map(|res| res.ok())
        .map(|dir_entry| dir_entry.path())
        .filter_map(|path| {
            if path
                .extension()
                .is_some_and(|ext| file_exts.contains(&ext.to_str().unwrap()))
            {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    Ok(paths)
}

pub fn pack_configs_from_files(files: &[PathBuf]) -> CpResult<HashMap<String, HashMap<String, Yaml>>> {
    let configs: Vec<Yaml> = files
        .iter()
        .flat_map(|path| {
            let config_str = std::fs::read_to_string(path).unwrap();
            YamlLoader::load_from_str(&config_str).unwrap()
        })
        .collect();
    pack_configurables(&configs)
}

fn pack_configurables(configs: &Vec<Yaml>) -> CpResult<HashMap<String, HashMap<String, Yaml>>> {
    let mut configurables_map: HashMap<String, HashMap<String, Yaml>> = HashMap::new();
    for config in configs {
        match config.to_map(format!("The following top-level config is not a map: {:?}", config)) {
            Ok(x) => {
                for (configurable, named_configs) in x {
                    match unpack_named_configs(named_configs, &mut configurables_map, &configurable) {
                        Ok(()) => (),
                        Err(e) => return Err(CpError::ComponentError("config.common", e)),
                    }
                }
            }
            Err(e) => {
                return Err(CpError::ComponentError("config.common", e));
            }
        }
    }
    Ok(configurables_map)
}

fn unpack_named_configs(
    named_configs: &Yaml,
    configurables_map: &mut HashMap<String, HashMap<String, Yaml>>,
    config_type: &str,
) -> SubResult<()> {
    if !configurables_map.contains_key(config_type) {
        configurables_map.insert(config_type.to_owned(), HashMap::new());
    }
    let config_map = named_configs.to_map(format!(
        "The following named {} config is not a map: {:?}",
        config_type, named_configs
    ))?;
    for (key, c) in config_map {
        let pack = configurables_map
            .get_mut(config_type)
            .unwrap_or_else(|| panic!("Configurable not initialized: {}", config_type));
        if !c.is_null() {
            pack.insert(key, Yaml::clone(c));
        } else {
            return Err(format!("Key {} has null value", &key));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use core::time;
    use std::{
        fs::{self, DirBuilder},
        io::Write,
        str::FromStr,
    };

    use crate::{
        task::transform::select::SelectTransform,
        util::{common::yaml_from_str, tmp::TempFile},
    };

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
        pack_configurables(&configs).unwrap_err();
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
        pack_configurables(&configs).unwrap_err();
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
        pack_configurables(&configs).unwrap_err();
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
        pack_configurables(&configs).unwrap_err();
    }

    #[test]
    fn invalid_null_arg_named_configs() {
        let configs = get_yaml_sample(
            "
main: 
    foo:
    bar:
        1: 
        2.0:
",
        );
        pack_configurables(&configs).unwrap_err();
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
",
        );
        let result = pack_configurables(&configs).unwrap();
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

    fn generate_tmp_config_files(dir_path: &str) -> Vec<TempFile> {
        fs::create_dir_all(dir_path).unwrap();

        let tmp_a = TempFile::default_in_dir(dir_path, "yml").unwrap();
        let tmp_b = TempFile::default_in_dir(dir_path, "yaml").unwrap();
        let tmp_c = TempFile::default_in_dir(dir_path, "log").unwrap();
        let mut file_a = tmp_a.get_mut().unwrap();
        let mut file_b = tmp_b.get_mut().unwrap();
        let mut file_c = tmp_c.get_mut().unwrap();
        file_a
            .write_all(
                b"
foo:
    list1: a
    list2: b
bar:
    BarA: x
    BarB2.0: b
",
            )
            .unwrap();
        file_b
            .write_all(
                b"
bar:
    oi: x
",
            )
            .unwrap();
        file_c
            .write_all(
                b"
foo:
    io: x
",
            )
            .unwrap();
        println!("wrote to tmps: [{:?}, {:?}]", tmp_a, tmp_b);
        vec![tmp_a, tmp_b, tmp_c]
    }

    #[test]
    fn valid_pack_configs_from_files() {
        let dir_path = "/tmp/capport_testing/valid_pack_configs_from_files/";
        {
            let tmp_paths = generate_tmp_config_files(dir_path);
            let paths = tmp_paths
                .iter()
                .filter(|&x| x.filepath.ends_with("ml"))
                .map(|x| PathBuf::from_str(&x.filepath).unwrap())
                .collect::<Vec<_>>();
            let actual = pack_configs_from_files(&paths).unwrap();
            let expected = HashMap::from([
                (
                    "foo".to_owned(),
                    HashMap::from([
                        ("list1".to_owned(), yaml_from_str("a").unwrap()),
                        ("list2".to_owned(), yaml_from_str("b").unwrap()),
                    ]),
                ),
                (
                    "bar".to_owned(),
                    HashMap::from([
                        ("BarA".to_owned(), yaml_from_str("x").unwrap()),
                        ("BarB2.0".to_owned(), yaml_from_str("b").unwrap()),
                        ("oi".to_owned(), yaml_from_str("x").unwrap()),
                    ]),
                ),
            ]);
            assert_eq!(actual, expected);
        }
        fs::remove_dir(dir_path).unwrap();
    }

    #[test]
    fn valid_read_configs() {
        let dir_path = "/tmp/capport_testing/valid_read_configs/";
        {
            let tmp_paths = generate_tmp_config_files(dir_path);
            let mut actual = read_configs(dir_path, &["yml", "yaml"]).unwrap();
            let mut expected = tmp_paths
                .iter()
                .filter(|&x| x.filepath.ends_with("ml"))
                .map(|x| PathBuf::from_str(&x.filepath).unwrap())
                .collect::<Vec<_>>();
            actual.sort();
            expected.sort();
            assert_eq!(actual, expected);
        }
        fs::remove_dir(dir_path).unwrap();
    }
}
