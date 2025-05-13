use chrono::{DateTime, FixedOffset, NaiveDate, Utc};
use log::{debug, trace};
use std::collections::HashMap;

use rand::{Rng, distr::Alphanumeric};

use crate::{
    context::envvar::get_env_var_str,
    logger::common::{DEFAULT_KEYWORD_CONFIG_DIR, DEFAULT_KEYWORD_OUTPUT_DIR},
    parser::common::YamlRead,
};

use super::error::{CpError, CpResult};

pub const NYT: &str = "America/New_York";
pub const UTC: &str = "UTC";

pub type YamlValue = serde_yaml_ng::Value;

pub const RECOGNIZED_DATE_PATTERNS: [&str; 2] = ["%Y-%m-%d", "%Y.%m.%d"];
pub const RECOGNIZED_TIME_PATTERNS: [&str; 2] = ["%H:%M:%S", "%H.%M.%S"];

pub fn parse_date_str(datetime_str: &str) -> CpResult<NaiveDate> {
    for &pattern in &RECOGNIZED_DATE_PATTERNS {
        match NaiveDate::parse_from_str(datetime_str, pattern) {
            Ok(x) => return Ok(x),
            Err(e) => {
                debug!("Date string `{}` cannot be parsed with pattern `{}`", pattern, e);
                println!("Date string `{}` cannot be parsed with pattern `{}`", pattern, e);
            }
        }
    }
    Err(CpError::RawError(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "Invalid date `{}` parsed, only the following patterns are recognized: {:?}",
            datetime_str, &RECOGNIZED_DATE_PATTERNS
        ),
    )))
}

pub fn parse_datetime_str(datetime_str: &str) -> CpResult<DateTime<FixedOffset>> {
    match datetime_str.parse() {
        Ok(x) => Ok(x),
        Err(e) => Err(CpError::RawError(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid datetime `{}` parsed, {:?}", datetime_str, e),
        ))),
    }
}

pub fn get_fmt_time_str_now(fmt: &str) -> String {
    Utc::now().format(fmt).to_string()
}

pub fn get_utc_time_str_now() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, false)
}

pub fn get_full_path(abs_or_rel_path_str: &str, is_config: bool) -> CpResult<std::path::PathBuf> {
    let path = std::path::Path::new(abs_or_rel_path_str);
    if path.is_absolute() {
        trace!("Loading from absolute path: {:?}", path);
        return Ok(path.to_owned());
    }

    trace!("Received relative path: {:?}; appending root...", path);
    let base = get_env_var_str(if is_config {
        DEFAULT_KEYWORD_CONFIG_DIR
    } else {
        DEFAULT_KEYWORD_OUTPUT_DIR
    })?;
    trace!("Default root dir: {}", base.as_str());
    let full_path = std::path::Path::new(base.as_str()).join(path);
    trace!("Derived path: {:?} (from {:?})", full_path, path);
    Ok(full_path)
}

pub fn create_config_pack(
    yaml_str: &str,
    configurable: &str,
) -> HashMap<String, HashMap<String, serde_yaml_ng::Value>> {
    let configs: serde_yaml_ng::Value = serde_yaml_ng::from_str(yaml_str).unwrap();
    HashMap::from([(configurable.to_owned(), configs.to_str_map().unwrap())])
}

pub fn yaml_from_str(s: &str) -> CpResult<serde_yaml_ng::Value> {
    Ok(serde_yaml_ng::from_str(s)?)
}

pub fn yaml_to_str(doc: &serde_yaml_ng::Value) -> CpResult<String> {
    Ok(serde_yaml_ng::to_string(doc)?)
}

pub fn rng_str(len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

#[macro_export]
macro_rules! ctx_run_n_threads {
    ($num_threads:expr, $slice:expr, $ctx:expr, $action:expr) => {
        let slice = ($slice);
        let len = slice.len();
        let n = std::cmp::min(($num_threads) as usize, len);
        log::trace!("Started {} threads", n);
        let (quo, rem) = (len / n, len % n);
        let split = (quo + 1) * rem;
        match thread::scope(|scope| {
            let ctx = ($ctx).clone();
            let chunks = slice[..split].chunks(quo + 1).chain(slice[split..].chunks(quo));
            for chunk in chunks {
                let ictx = ctx.clone();
                scope.spawn(move |_| ($action)(chunk, ictx));
            }
        }) {
            Ok(_) => {
                log::trace!("Joined {} threads", n);
            }
            Err(e) => {
                log::error!("Thread err:\n{:?}", e);
            }
        };
    };
}
