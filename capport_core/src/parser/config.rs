use crate::util::error::CpResult;
use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
};

use super::common::YamlRead;

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

pub fn pack_configs_from_files<P: AsRef<Path>>(
    paths: &[P],
) -> CpResult<HashMap<String, HashMap<String, serde_yaml_ng::Value>>> {
    let mut config_pack: HashMap<String, HashMap<String, serde_yaml_ng::Value>> = HashMap::new();

    for path in paths {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let yaml_root: serde_yaml_ng::Value = serde_yaml_ng::from_reader(reader)?;
        pack_configurables(&mut config_pack, yaml_root)?;
    }

    Ok(config_pack)
}

pub fn pack_configurables(
    config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    yaml_root: serde_yaml_ng::Value,
) -> CpResult<()> {
    let top_level_map = yaml_root.to_str_map()?;
    for (configurable_name, value) in top_level_map {
        let node_fields = value.to_str_map()?;
        match config_pack.entry(configurable_name.clone()) {
            std::collections::hash_map::Entry::Occupied(mut oe) => {
                oe.get_mut().extend(node_fields);
            }
            std::collections::hash_map::Entry::Vacant(ve) => {
                ve.insert(node_fields);
            }
        };
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use std::{
        fs::{self},
        io::Write,
        str::FromStr,
    };

    use crate::util::{common::yaml_from_str, tmp::TempFile};

    use super::*;

    #[test]
    fn invalid_config_nodes_list() {
        let configs = yaml_from_str(
            "
foo:
    - list1
    - list2
bar:
    - bar1
    - bar2.0
",
        )
        .unwrap();
        let mut config_pack = HashMap::new();
        pack_configurables(&mut config_pack, configs).unwrap_err();
    }

    #[test]
    fn invalid_config_list_nodes() {
        let configs = yaml_from_str(
            "
- foo:
    list1: 
    list2: 
- bar:
    bar1: 
    bar2.0:
",
        )
        .unwrap();
        let mut config_pack = HashMap::new();
        pack_configurables(&mut config_pack, configs).unwrap_err();
    }

    #[test]
    fn invalid_config_null_nodes() {
        let configs = yaml_from_str(
            "
foo:
    list1: 
    list2: 
bar:
",
        )
        .unwrap();
        let mut config_pack = HashMap::new();
        pack_configurables(&mut config_pack, configs).unwrap_err();
    }

    #[test]
    fn invalid_config_invalid_keys() {
        let configs = yaml_from_str(
            "
foo:
    list1: 
    list2: 
bar:
    1: 
    2.0:
",
        )
        .unwrap();
        let mut config_pack = HashMap::new();
        pack_configurables(&mut config_pack, configs).unwrap_err();
    }

    #[test]
    fn invalid_null_arg_named_configs() {
        let configs = yaml_from_str(
            "
main: 
    foo:
    bar:
        1: 
        2.0:
",
        )
        .unwrap();
        let mut config_pack = HashMap::new();
        pack_configurables(&mut config_pack, configs).unwrap();
        let mut expected = HashMap::new();
        expected.insert(
            String::from("main"),
            HashMap::from([
                (String::from("foo"), serde_yaml_ng::Value::Null),
                (String::from("bar"), yaml_from_str("{ 1: null, 2.0: null }").unwrap()),
            ]),
        );
        assert_eq!(config_pack, expected);
    }

    #[test]
    fn valid_config_list_nodes() {
        let configs = yaml_from_str(
            "
foo:
    list1: a
    list2: b
bar:
    BarA: x
    BarB2.0: b
",
        )
        .unwrap();
        let mut config_pack = HashMap::new();
        pack_configurables(&mut config_pack, configs).unwrap();
        let mut expected = HashMap::new();
        expected.insert(
            String::from("foo"),
            HashMap::from([
                (String::from("list1"), yaml_from_str("a").unwrap()),
                (String::from("list2"), yaml_from_str("b").unwrap()),
            ]),
        );
        expected.insert(
            String::from("bar"),
            HashMap::from([
                (String::from("BarA"), yaml_from_str("x").unwrap()),
                (String::from("BarB2.0"), yaml_from_str("b").unwrap()),
            ]),
        );
        assert_eq!(config_pack, expected);
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
