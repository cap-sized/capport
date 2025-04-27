use serde::{Deserialize, Serialize};

use crate::util::error::CpResult;

const DEFAULT_LOG_LEVEL: log::LevelFilter = log::LevelFilter::Info;
const DEFAULT_LOG_PREFIX: &str = "programlog_";
const DEFAULT_TIMESTAMP_SUFFIX: &str = "%Y-%m-%dT%H%M%S.log";

#[derive(Serialize, Deserialize)]
pub struct LogWriter {
    pub label: String,
    pub level: Option<String>,
    pub output: Option<String>,
    pub file_prefix: Option<String>,
    pub file_timestamp: Option<String>,
}

fn get_level(level: &Option<String>) -> Option<log::LevelFilter> {
    match level {
        Some(x) => match x.as_ref() {
            "debug" => Some(log::LevelFilter::Debug),
            "info" => Some(log::LevelFilter::Info),
            "warn" => Some(log::LevelFilter::Warn),
            "error" => Some(log::LevelFilter::Error),
            "trace" => Some(log::LevelFilter::Trace),
            "off" => Some(log::LevelFilter::Off),
            _ => None,
        },
        None => None,
    }
}

impl LogWriter {
    pub fn start(&self) -> CpResult<()> {
        match fern::Dispatch::new()
            .level(get_level(&self.level).unwrap_or(DEFAULT_LOG_LEVEL))
            .chain(fern::DateBased::new(DEFAULT_LOG_PREFIX, DEFAULT_TIMESTAMP_SUFFIX))
            .apply()
        {
            Ok(_) => Ok(()),
            // allowed panic here, because failed logging would be considered catastrophic
            Err(e) => panic!("Failed to setup logger, quiting: {:?}", e),
        }
    }
}
