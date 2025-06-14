use std::collections::HashSet;

use chrono::{DateTime, FixedOffset, NaiveDate};
use serde::{Deserialize, Serialize};

use crate::{
    logger::common::{
        DEFAULT_KEYWORD_CONFIG_DIR, DEFAULT_KEYWORD_IS_CONSOLE, DEFAULT_KEYWORD_IS_EXECUTING,
        DEFAULT_KEYWORD_OUTPUT_DIR, DEFAULT_KEYWORD_PIPELINE, DEFAULT_KEYWORD_REF_DATE, DEFAULT_KEYWORD_REF_DATETIME,
        DEFAULT_KEYWORD_RUNNER,
    },
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

pub fn get_env_var_str(key: &str) -> CpResult<String> {
    match std::env::var(key) {
        Ok(x) => Ok(x.trim().to_owned()),
        Err(e) => Err(CpError::ComponentError(
            "Environment variable error: ",
            format!("[variable: {}] {:?}", key, e),
        )),
    }
}

pub fn get_env_var<T>(key: &str) -> CpResult<T>
where
    T: for<'a> Deserialize<'a>,
{
    let value = get_env_var_str(key)?;
    Ok(serde_yaml_ng::from_str::<T>(&value)?)
}

impl EnvironmentVariableRegistry {
    pub fn new() -> EnvironmentVariableRegistry {
        EnvironmentVariableRegistry { keys: HashSet::new() }
    }

    pub fn get_keys(&self) -> HashSet<String> {
        self.keys.clone()
    }

    pub fn init(default_config_dir: String, default_output_dir: String) -> CpResult<EnvironmentVariableRegistry> {
        let mut ev = EnvironmentVariableRegistry::new();
        ev.set_str(DEFAULT_KEYWORD_CONFIG_DIR, default_config_dir)?;
        ev.set_str(DEFAULT_KEYWORD_OUTPUT_DIR, default_output_dir)?;

        Ok(ev)
    }

    pub fn from_args(args: &RunPipelineArgs) -> CpResult<EnvironmentVariableRegistry> {
        let mut ev = EnvironmentVariableRegistry::init(args.config.to_owned(), args.output.to_owned())?;
        if args.datetime.is_some() {
            let dt = parse_datetime_str(args.datetime.as_ref().unwrap())?;
            ev.set::<DateTime<FixedOffset>>(DEFAULT_KEYWORD_REF_DATETIME, &dt)?;
        } else if args.date.is_some() {
            let dt = parse_date_str(args.date.as_ref().unwrap())?;
            ev.set::<NaiveDate>(DEFAULT_KEYWORD_REF_DATE, &dt)?;
        }
        ev.set_str(DEFAULT_KEYWORD_PIPELINE, args.pipeline.clone())?;
        ev.set_str(DEFAULT_KEYWORD_RUNNER, args.runner.clone())?;
        ev.set::<bool>(DEFAULT_KEYWORD_IS_EXECUTING, &args.execute)?;
        ev.set::<bool>(DEFAULT_KEYWORD_IS_CONSOLE, &args.console)?;
        Ok(ev)
    }

    pub fn set_str(&mut self, key: &str, value: String) -> CpResult<()> {
        unsafe {
            std::env::set_var(key, value);
        }
        self.keys.insert(key.to_owned());
        Ok(())
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
        let mut failed_keys = vec![];
        for key in &keys {
            match self.pop(key) {
                Ok(_) => {}
                Err(x) => failed_keys.push(format!("{}: {:?}\n", key, x)),
            }
        }
        if !failed_keys.is_empty() {
            return Err(CpError::ComponentError(
                "Keys not found or deregistered correctly",
                format!("{:?}", failed_keys),
            ));
        }
        Ok(())
    }
}

impl Drop for EnvironmentVariableRegistry {
    fn drop(&mut self) {
        match self.drop_all() {
            Ok(_) => {}
            Err(e) => log::error!("{:?}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        context::envvar::{get_env_var, get_env_var_str},
        logger::common::{
            DEFAULT_KEYWORD_CONFIG_DIR, DEFAULT_KEYWORD_IS_CONSOLE, DEFAULT_KEYWORD_IS_EXECUTING,
            DEFAULT_KEYWORD_OUTPUT_DIR, DEFAULT_KEYWORD_PIPELINE, DEFAULT_KEYWORD_REF_DATE,
            DEFAULT_KEYWORD_REF_DATETIME, DEFAULT_KEYWORD_RUNNER,
        },
        util::args::RunPipelineArgs,
    };

    use super::EnvironmentVariableRegistry;
    use chrono::prelude::*;

    const KEYA: &str = "KEYA";
    const KEYB: &str = "KEYB";
    const KEYC: &str = "KEYC";
    const KEYD: &str = "KEYD";
    #[test]
    fn valid_set_get_variable() {
        let mut ev = EnvironmentVariableRegistry::new();
        let s = String::from("12345");
        ev.set_str(KEYA, s.clone()).unwrap();
        assert_eq!(get_env_var_str(KEYA).unwrap(), s);
        assert!(ev.has_key(KEYA));
        ev.pop(KEYA).unwrap();
        assert!(!ev.has_key(KEYA));
    }

    #[test]
    fn valid_set_get_yml_variable_date_invalid_parse_and_drop() {
        {
            let mut ev = EnvironmentVariableRegistry::new();
            let dt = Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap();
            ev.set::<DateTime<Utc>>(KEYB, &dt).unwrap();
            assert!(get_env_var::<i32>(KEYB).is_err());
            assert_eq!(get_env_var::<DateTime<Utc>>(KEYB).unwrap(), dt);
            assert!(ev.has_key(KEYB));
        }
        assert!(get_env_var_str(KEYB).is_err());
    }

    #[test]
    fn handle_externally_removed_envvar() {
        let mut ev = EnvironmentVariableRegistry::new();
        let dt = Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap();
        ev.set::<DateTime<Utc>>(KEYC, &dt).unwrap();
        unsafe {
            // Removing this here will not lead to an error while dropping later
            std::env::remove_var(KEYC);
        }
        ev.drop_all().unwrap();
    }

    #[test]
    fn valid_set_get_yml_variable_number() {
        let mut ev = EnvironmentVariableRegistry::new();
        let s = 12345;
        ev.set::<i32>(KEYD, &s).unwrap();
        assert_eq!(get_env_var::<i32>(KEYD).unwrap(), s);
        assert!(ev.has_key(KEYD));
        assert!(ev.has_key(KEYD));
        ev.pop(KEYD).unwrap();
        assert!(!ev.has_key(KEYD));
    }

    #[test]
    fn valid_set_drop_all_from_args_datetime() {
        {
            let str_dt = [(
                Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
                "2014-11-28T12:00:09+00:00",
            )];
            for (dt, dt_str) in str_dt {
                let args = RunPipelineArgs {
                    config: "/tmp/config".to_owned(),
                    output: "/tmp/output".to_owned(),
                    date: None,
                    runner: "".to_string(),
                    datetime: Some(dt_str.to_string()),
                    pipeline: "ignore".to_owned(),
                    execute: false,
                    console: false,
                };
                let mut ev = EnvironmentVariableRegistry::from_args(&args).unwrap();
                assert_eq!(get_env_var::<DateTime<Utc>>(DEFAULT_KEYWORD_REF_DATETIME).unwrap(), dt);
                assert_eq!(
                    get_env_var::<String>(DEFAULT_KEYWORD_CONFIG_DIR).unwrap(),
                    args.config.to_owned()
                );
                assert_eq!(
                    get_env_var_str(DEFAULT_KEYWORD_OUTPUT_DIR).unwrap(),
                    args.output.to_owned()
                );
                assert_eq!(
                    get_env_var_str(DEFAULT_KEYWORD_PIPELINE).unwrap(),
                    args.pipeline.to_owned()
                );
                assert_eq!(get_env_var_str(DEFAULT_KEYWORD_RUNNER).unwrap(), args.runner.to_owned());
                assert_eq!(
                    get_env_var::<bool>(DEFAULT_KEYWORD_IS_EXECUTING).unwrap(),
                    args.execute.to_owned()
                );
                assert_eq!(
                    get_env_var::<bool>(DEFAULT_KEYWORD_IS_CONSOLE).unwrap(),
                    args.console.to_owned()
                );
                ev.drop_all().unwrap();
            }
        }
        {
            let str_dt = [(NaiveDate::from_ymd_opt(1988, 9, 8).unwrap(), "1988-09-08")];
            for (dt, dt_str) in str_dt {
                let args = RunPipelineArgs {
                    config: "/tmp/config".to_owned(),
                    output: "/tmp/output".to_owned(),
                    date: Some(dt_str.to_string()),
                    datetime: None,
                    runner: "runner".to_string(),
                    pipeline: "ignore".to_owned(),
                    execute: false,
                    console: true,
                };
                let mut ev = EnvironmentVariableRegistry::from_args(&args).unwrap();
                assert_eq!(get_env_var::<NaiveDate>(DEFAULT_KEYWORD_REF_DATE).unwrap(), dt);
                assert_eq!(
                    get_env_var::<String>(DEFAULT_KEYWORD_CONFIG_DIR).unwrap(),
                    args.config.to_owned()
                );
                assert_eq!(
                    get_env_var_str(DEFAULT_KEYWORD_OUTPUT_DIR).unwrap(),
                    args.output.to_owned()
                );
                assert_eq!(
                    get_env_var_str(DEFAULT_KEYWORD_PIPELINE).unwrap(),
                    args.pipeline.to_owned()
                );
                assert_eq!(get_env_var_str(DEFAULT_KEYWORD_RUNNER).unwrap(), args.runner.to_owned());
                assert_eq!(
                    get_env_var::<bool>(DEFAULT_KEYWORD_IS_EXECUTING).unwrap(),
                    args.execute.to_owned()
                );
                assert_eq!(
                    get_env_var::<bool>(DEFAULT_KEYWORD_IS_CONSOLE).unwrap(),
                    args.console.to_owned()
                );
                ev.drop_all().unwrap();
            }
        }
    }
}
