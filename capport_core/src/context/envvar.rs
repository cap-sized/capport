use std::collections::HashSet;

use chrono::{DateTime, TimeZone};
use serde::{Deserialize, Serialize};

use crate::{
    logger::common::{DEFAULT_KEYWORD_CONFIG_DIR, DEFAULT_KEYWORD_OUTPUT_DIR, DEFAULT_KEYWORD_REFDATE_DIR},
    util::{
        args::RunPipelineArgs,
        common::{parse_date_str, parse_datetime_str},
        error::{CpError, CpResult},
    },
};

#[derive(Debug)]
pub struct EnvironmentVariableRegistry {
    keys: HashSet<String>,
}

impl Default for EnvironmentVariableRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvironmentVariableRegistry {
    pub fn new() -> EnvironmentVariableRegistry {
        EnvironmentVariableRegistry { keys: HashSet::new() }
    }

    pub fn init<T>(
        default_config_dir: String,
        default_output_dir: String,
        default_ref_date: &T,
    ) -> CpResult<EnvironmentVariableRegistry>
    where
        T: Serialize,
    {
        let mut ev = EnvironmentVariableRegistry::new();
        ev.set_str(DEFAULT_KEYWORD_CONFIG_DIR, default_config_dir)?;
        ev.set_str(DEFAULT_KEYWORD_OUTPUT_DIR, default_output_dir)?;

        ev.set::<T>(DEFAULT_KEYWORD_REFDATE_DIR, default_ref_date)?;
        Ok(ev)
    }

    pub fn from_args(args: &RunPipelineArgs) -> CpResult<EnvironmentVariableRegistry> {
        let dt = parse_datetime_str(args.date.as_ref().unwrap())?;
        EnvironmentVariableRegistry::init(args.config_dir.to_owned(), args.output.to_owned(), &dt)
    }

    pub fn set_str(&mut self, key: &str, value: String) -> CpResult<()> {
        unsafe {
            std::env::set_var(key, value);
        }
        self.keys.insert(key.to_owned());
        Ok(())
    }

    pub fn get_str(&self, key: &str) -> CpResult<String> {
        match std::env::var(key) {
            Ok(x) => Ok(x),
            Err(e) => Err(CpError::ComponentError(
                "Environment variable error: ",
                format!("[variable: {}] {:?}", key, e),
            )),
        }
    }

    pub fn get<T>(&self, key: &str) -> CpResult<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let value = self.get_str(key)?;
        Ok(serde_yaml_ng::from_str::<T>(&value)?)
    }

    pub fn set<T>(&mut self, key: &str, value: &T) -> CpResult<()>
    where
        T: Serialize,
    {
        let valstr = serde_yaml_ng::to_string(value)?;
        unsafe {
            std::env::set_var(key, valstr);
        }
        self.keys.insert(key.to_owned());
        Ok(())
    }

    pub fn pop(&mut self, key: &str) -> CpResult<()> {
        unsafe {
            self.keys.remove(key);
            std::env::remove_var(key);
            Ok(())
        }
    }

    pub fn has_key(&self, key: &str) -> bool {
        self.keys.contains(key)
    }

    pub fn drop_all(&mut self) -> CpResult<()> {
        let keys = self.keys.clone();
        for key in keys {
            self.pop(&key)?;
        }
        self.keys.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        logger::common::{DEFAULT_KEYWORD_CONFIG_DIR, DEFAULT_KEYWORD_OUTPUT_DIR, DEFAULT_KEYWORD_REFDATE_DIR},
        util::args::RunPipelineArgs,
    };

    use super::EnvironmentVariableRegistry;
    use chrono::prelude::*;

    const KEYA: &str = "KEYA";
    #[test]
    fn valid_set_get_variable() {
        let mut ev = EnvironmentVariableRegistry::new();
        let s = String::from("12345");
        ev.set_str(KEYA, s.clone()).unwrap();
        assert_eq!(ev.get_str(KEYA).unwrap(), s);
        assert!(ev.has_key(KEYA));
        ev.pop(KEYA).unwrap();
        assert!(!ev.has_key(KEYA));
    }

    #[test]
    fn valid_set_get_yml_variable_number() {
        let mut ev = EnvironmentVariableRegistry::new();
        let s = 12345;
        ev.set::<i32>(KEYA, &s).unwrap();
        assert_eq!(ev.get::<i32>(KEYA).unwrap(), s);
        assert!(ev.has_key(KEYA));
        assert!(ev.has_key(KEYA));
        ev.pop(KEYA).unwrap();
        assert!(!ev.has_key(KEYA));
    }

    #[test]
    fn valid_set_get_yml_variable_date_invalid_parse() {
        let mut ev = EnvironmentVariableRegistry::new();
        let dt = Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap();
        ev.set::<DateTime<Utc>>(KEYA, &dt).unwrap();
        assert!(ev.get::<i32>(KEYA).is_err());
        assert_eq!(ev.get::<DateTime<Utc>>(KEYA).unwrap(), dt);
        assert!(ev.has_key(KEYA));
        ev.pop(KEYA).unwrap();
        assert!(!ev.has_key(KEYA));
    }

    #[test]
    fn valid_set_drop_all() {
        let dt = Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap();
        let mut ev =
            EnvironmentVariableRegistry::init("/tmp/config".to_owned(), "/tmp/output".to_owned(), &dt).unwrap();
        assert_eq!(ev.get::<DateTime<Utc>>(DEFAULT_KEYWORD_REFDATE_DIR).unwrap(), dt);
        assert_eq!(
            ev.get::<String>(DEFAULT_KEYWORD_CONFIG_DIR).unwrap(),
            "/tmp/config".to_owned()
        );
        assert_eq!(
            ev.get_str(DEFAULT_KEYWORD_OUTPUT_DIR).unwrap(),
            "/tmp/output".to_owned()
        );
        ev.drop_all().unwrap();
        assert!(!ev.has_key(DEFAULT_KEYWORD_CONFIG_DIR));
        assert!(!ev.has_key(DEFAULT_KEYWORD_OUTPUT_DIR));
        assert!(!ev.has_key(DEFAULT_KEYWORD_REFDATE_DIR));
    }

    #[test]
    fn valid_set_drop_all_from_args() {
        let str_dt = [
            (
                Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
                "2014-11-28T12:00:09+00:00",
            ),
            (Utc.with_ymd_and_hms(2014, 11, 28, 0, 0, 0).unwrap(), "2014-11-28"),
        ];
        for (dt, dt_str) in str_dt {
            let args = RunPipelineArgs {
                config_dir: "/tmp/config".to_owned(),
                output: "/tmp/output".to_owned(),
                date: None,
                datetime: Some(dt_str.to_string()),
                pipeline: "ignore".to_owned(),
                print_to_console: true,
            };
            let mut ev = EnvironmentVariableRegistry::from_args(&args).unwrap();
            assert_eq!(ev.get::<DateTime<Utc>>(DEFAULT_KEYWORD_REFDATE_DIR).unwrap(), dt);
            assert_eq!(
                ev.get::<String>(DEFAULT_KEYWORD_CONFIG_DIR).unwrap(),
                args.config_dir.to_owned()
            );
            assert_eq!(ev.get_str(DEFAULT_KEYWORD_OUTPUT_DIR).unwrap(), args.output.to_owned());
            ev.drop_all().unwrap();
        }
    }
}
