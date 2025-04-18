use polars::{df, prelude::PlSmallStr};
use polars_lazy::frame::{IntoLazy, LazyFrame};
use std::collections::HashMap;

use rand::{Rng, distr::Alphanumeric};
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
        Err(e) => Err(e.to_string()),
    }
}

pub fn rng_str(len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

pub struct DummyData;
impl DummyData {
    pub fn state_code() -> LazyFrame {
        // https://ddvat.gov.in/docs/List%20of%20State%20Code.pdf
        df! [
            "state" => ["Karnataka", "Goa", "Tamil Nadu", "Delhi"],
            "state_code" => ["KA", "GA", "TN", "DL"],
            "tin" => [29, 30, 33, 7],
        ]
        .unwrap()
        .lazy()
    }

    pub fn player_scores() -> LazyFrame {
        df![
            "csid" => [82938842, 82938842, 86543102, 82938842, 86543102, 86543102, 8872631],
            "game" => [1, 2, 1, 3, 2, 3, 1],
            "scores" => [20, 3, 43, -7, 50, 12, 19],
        ]
        .unwrap()
        .lazy()
    }

    pub fn id_name_map() -> LazyFrame {
        df![
            "first_name" => ["Darren", "Hunter", "Varya"],
            "last_name" => ["Hutnaby", "O'Connor", "Zeb"],
            "id" => [8872631, 82938842, 86543102],
        ]
        .unwrap()
        .lazy()
    }

    pub fn player_data() -> LazyFrame {
        df![
            "csid" => [8872631, 82938842, 86543102],
            "playerId" => ["abcd", "88ef", "1988"],
            "shootsCatches" => ["L", "R", "L"],
            "state" => ["TN", "DL", "GA"],
            "name" => df![
                "first" => ["Darren", "Hunter", "Varya"],
                "last" => ["Hutnaby", "O'Connor", "Zeb"],
            ].unwrap().into_struct(PlSmallStr::from_str("name")),
        ]
        .unwrap()
        .lazy()
    }
}

#[cfg(test)]
mod tests {
    use yaml_rust2::Yaml;

    use super::{yaml_from_str, yaml_to_str};

    fn invalid_yaml_to_str_bad_yaml() {
        let badval = Yaml::BadValue;
        yaml_to_str(&badval).unwrap_err();
    }
}
