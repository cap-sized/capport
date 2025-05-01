use fern::colors::{Color, ColoredLevelConfig};
use log::info;
use serde::{Deserialize, Deserializer, de};

use crate::util::{
    common::{get_fmt_time_str_now, get_full_path, get_utc_time_str_now},
    error::{CpError, CpResult},
};

pub const DEFAULT_CONSOLE_LOGGER_NAME: &str = "__stdout__";
const DEFAULT_LOG_LEVEL: log::LevelFilter = log::LevelFilter::Info;
const DEFAULT_TIMESTAMP_SUFFIX: &str = "%Y%m%d-%H%M%S";

pub const DEFAULT_KEYWORD_REF_DATETIME: &str = "REF_DATETIME";
pub const DEFAULT_KEYWORD_REF_DATE: &str = "REF_DATE";
pub const DEFAULT_KEYWORD_OUTPUT_DIR: &str = "OUTPUT_DIR";
pub const DEFAULT_KEYWORD_CONFIG_DIR: &str = "CONFIG_DIR";

const COLOR_DEBUG: Color = Color::Magenta;
const COLOR_INFO: Color = Color::BrightGreen;
const COLOR_WARN: Color = Color::BrightYellow;
const COLOR_ERROR: Color = Color::Red;
const COLOR_TRACE: Color = Color::Blue;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogLevelFilter(pub log::LevelFilter);

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Logger {
    pub label: String,
    pub level: Option<LogLevelFilter>,
    pub output_path_prefix: Option<String>,

    pub _final_output_path: Option<String>,
}

impl<'de> Deserialize<'de> for LogLevelFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.to_lowercase().as_ref() {
            "debug" => Ok(LogLevelFilter(log::LevelFilter::Debug)),
            "info" => Ok(LogLevelFilter(log::LevelFilter::Info)),
            "warn" => Ok(LogLevelFilter(log::LevelFilter::Warn)),
            "error" => Ok(LogLevelFilter(log::LevelFilter::Error)),
            "trace" => Ok(LogLevelFilter(log::LevelFilter::Trace)),
            "off" => Ok(LogLevelFilter(log::LevelFilter::Off)),
            s => Err(de::Error::custom(format!("Unknown LogLevelFilter in logger: {}", s))),
        }
    }
}

impl Logger {
    pub fn new(label: &str, level: Option<log::LevelFilter>, output: Option<&str>) -> Self {
        Logger {
            label: label.to_owned(),
            level: level.map(LogLevelFilter),
            output_path_prefix: output.map(|x| x.to_owned()),
            _final_output_path: None,
        }
    }
    pub fn get_full_path(&self, pipeline_name: &str) -> Option<String> {
        self.output_path_prefix.as_ref()?;
        let output_dir = match get_full_path(self.output_path_prefix.as_ref().unwrap(), false) {
            Ok(x) => x,
            Err(e) => {
                println!("No output log will be produced: {:?}", e);
                return None;
            }
        };
        let datetime_str = get_fmt_time_str_now(DEFAULT_TIMESTAMP_SUFFIX);
        Some(format!(
            "{}{}_{}.log",
            output_dir.to_str().unwrap(),
            pipeline_name,
            datetime_str
        ))
    }

    // TODO: use pipeline_name and env var for logging directory
    pub fn start(&mut self, pipeline_name: &str, to_stdout: bool) -> CpResult<()> {
        let colors = ColoredLevelConfig::new()
            .debug(COLOR_DEBUG)
            .info(COLOR_INFO)
            .warn(COLOR_WARN)
            .error(COLOR_ERROR)
            .trace(COLOR_TRACE);
        let base_dispatch = fern::Dispatch::new()
            .level(self.level.clone().map(|x| x.0).unwrap_or(DEFAULT_LOG_LEVEL))
            .format(move |out, message, record| {
                out.finish(format_args!(
                    "[{} {} {}] {}",
                    get_utc_time_str_now(),
                    colors.color(record.level()),
                    record.target(),
                    message
                ))
            });
        let dispatch = match &self.get_full_path(pipeline_name) {
            Some(full_file_path) => {
                let d = if to_stdout {
                    base_dispatch.chain(std::io::stdout())
                } else {
                    base_dispatch
                };
                let outfile = fern::log_file(full_file_path)?;
                self._final_output_path = Some(format!("{:?}", outfile));
                d.chain(outfile)
            }
            None => base_dispatch.chain(std::io::stdout()),
        };

        match dispatch.apply() {
            Ok(_) => {
                info!("Log recording into {}", self._final_output_path.as_ref().unwrap());
                Ok(())
            }
            Err(e) => Err(CpError::ComponentError(
                "Logger",
                format!("Failed to setup logger, quiting: {:?}", e),
            )),
        }
    }
}

#[cfg(test)]
mod tests {

    use chrono::Utc;
    use regex::Regex;

    use crate::{
        context::envvar::EnvironmentVariableRegistry,
        logger::common::{LogLevelFilter, Logger},
    };

    use super::DEFAULT_KEYWORD_OUTPUT_DIR;

    #[test]
    fn deserializing_log_level() {
        [
            (log::LevelFilter::Debug, "Debug"),
            (log::LevelFilter::Info, "Info"),
            (log::LevelFilter::Trace, "TraCE"),
            (log::LevelFilter::Error, "error"),
            (log::LevelFilter::Off, "Off"),
            (log::LevelFilter::Warn, "wArN"),
        ]
        .iter()
        .for_each(|(level, lvlstr)| {
            assert_eq!(
                LogLevelFilter(level.to_owned()),
                serde_yaml_ng::from_str::<LogLevelFilter>(lvlstr).unwrap()
            );
        });
    }

    #[test]
    fn invalid_log_level() {
        assert!(serde_yaml_ng::from_str::<LogLevelFilter>("test").is_err());
    }

    #[test]
    fn valid_prefixing_full_paths() {
        let pipeline_name = "mypipe";
        {
            let writer = Logger::new("test", None, Some("/tmp/"));
            let date_utc = Utc::now().format("%Y%m%d").to_string();
            // may spuriously fail near midnight, just rerun in that case
            let fmt_date = format!("/tmp/mypipe_{}-([0-9]*).log", date_utc);

            let prefix_re = Regex::new(&fmt_date).unwrap();
            assert!(
                prefix_re
                    .find(writer.get_full_path(pipeline_name).unwrap().as_str())
                    .is_some()
            );
        }

        {
            let mut ev = EnvironmentVariableRegistry::new();
            ev.set_str(DEFAULT_KEYWORD_OUTPUT_DIR, "/tmp/capport_testing/logger/".to_owned())
                .unwrap();

            let writer = Logger::new("test", None, Some("test/test_"));
            let date_utc = Utc::now().format("%Y%m%d").to_string();
            // may spuriously fail near midnight, just rerun in that case
            let fmt_date = format!("/tmp/capport_testing/logger/test/test_mypipe_{}-([0-9]*).log", date_utc);
            let prefix_re = Regex::new(&fmt_date).unwrap();
            let path = writer.get_full_path(pipeline_name).unwrap();
            assert!(prefix_re.find(path.as_str()).is_some());
        }

        {
            let writer = Logger::new("test", None, None);
            assert_eq!(writer.get_full_path(pipeline_name), None);
        }
    }
}
