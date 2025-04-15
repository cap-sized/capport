use std::collections::HashMap;

use yaml_rust2::{Yaml, YamlEmitter};

use crate::util::error::SubResult;

pub trait YamlRead {
    fn _to_str(&self, msg_if_fail: &str) -> SubResult<String>;
    fn to_str(&self, msg_if_fail: String) -> SubResult<String>;
    fn to_map(&self, msg_if_fail: String) -> SubResult<HashMap<String, &Yaml>>;
    fn over_map<Out, F>(&self, func: F, msg_if_fail: String) -> SubResult<Vec<Out>>
    where
        F: Fn(&str, &Yaml) -> SubResult<Out>;
    fn to_list(&self, msg_if_fail: String) -> SubResult<&Vec<Yaml>>;
    fn to_list_str(&self, msg_if_fail: String) -> SubResult<Vec<String>>;
}

pub trait YamlMapRead {
    fn get_str(&self, key: &str, msg_if_fail: String) -> SubResult<String>;
    fn get_list_str(&self, key: &str, msg_if_fail: String) -> SubResult<Vec<String>>;
}

impl YamlRead for Yaml {
    fn _to_str(&self, msg_if_fail: &str) -> SubResult<String> {
        match self.as_str() {
            Some(x) => Ok(x.to_string()),
            None => Err(msg_if_fail.to_owned()),
        }
    }
    fn to_str(&self, msg_if_fail: String) -> SubResult<String> {
        match self.as_str() {
            Some(x) => Ok(x.to_string()),
            None => Err(msg_if_fail),
        }
    }
    fn to_map(&self, msg_if_fail: String) -> SubResult<HashMap<String, &Yaml>> {
        let itermap = match self.as_hash() {
            Some(x) => x.iter(),
            None => return Err(msg_if_fail),
        };
        let mut map = HashMap::new();
        for (k, v) in itermap {
            match k._to_str(&msg_if_fail) {
                Ok(x) => map.insert(x, v),
                Err(e) => return Err(e),
            };
        }
        Ok(map)
    }
    fn to_list(&self, msg_if_fail: String) -> SubResult<&Vec<Yaml>> {
        match self.as_vec() {
            Some(x) => Ok(x),
            None => Err(msg_if_fail),
        }
    }
    fn to_list_str(&self, msg_if_fail: String) -> SubResult<Vec<String>> {
        let vecstr = match self.to_list(msg_if_fail.clone()) {
            Ok(x) => x
                .iter()
                .map(|x| x._to_str(&msg_if_fail))
                .collect::<Vec<SubResult<String>>>(),
            Err(e) => {
                return Err(e);
            }
        };
        let mut list_str: Vec<String> = vec![];
        for s in vecstr {
            match s {
                Ok(x) => list_str.push(x),
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(list_str)
    }
    fn over_map<Out, F>(&self, func: F, msg_if_fail: String) -> SubResult<Vec<Out>>
    where
        F: Fn(&str, &Yaml) -> SubResult<Out>,
    {
        let mut list_out: Vec<Out> = vec![];
        let itermap = match self.as_hash() {
            Some(x) => x,
            None => return Err(msg_if_fail),
        };
        for (key, val) in itermap {
            let keystr = key._to_str(&msg_if_fail)?;
            match func(&keystr, val) {
                Ok(x) => list_out.push(x),
                Err(e) => return Err(e),
            }
        }
        Ok(list_out)
    }
}

impl YamlMapRead for HashMap<String, &Yaml> {
    fn get_str(&self, key: &str, msg_if_fail: String) -> SubResult<String> {
        match &self.get(key).is_some_and(|x| x.as_str().is_some()) {
            true => Ok(self.get(key).unwrap().as_str().unwrap().to_owned()),
            false => Err(msg_if_fail),
        }
    }
    fn get_list_str(&self, key: &str, msg_if_fail: String) -> SubResult<Vec<String>> {
        match &self.get(key).is_some_and(|x| x.as_vec().is_some()) {
            true => {
                let vec = self.get(key).unwrap().as_vec().unwrap();
                let mut vecstr: Vec<String> = vec![];
                for v in vec {
                    match v._to_str(&msg_if_fail) {
                        Ok(x) => vecstr.push(x),
                        Err(e) => return Err(e),
                    };
                }
                Ok(vecstr)
            }
            false => match &self.get_str(key, msg_if_fail.clone()) {
                Ok(x) => Ok(vec![x.to_string()]),
                Err(e) => Err(e.clone()),
            },
        }
    }
}
