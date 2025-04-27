use fern::colors::{Color, ColoredLevelConfig};
use serde::{Deserialize, Deserializer, de};

use crate::util::{
    common::get_utc_time_str_now,
    error::{CpError, CpResult},
};

pub const DEFAULT_CONSOLE_LOGGER_NAME: &str = "";
const DEFAULT_LOG_LEVEL: log::LevelFilter = log::LevelFilter::Info;
const DEFAULT_LOG_PREFIX: &str = "programlog_";
const DEFAULT_TIMESTAMP_SUFFIX: &str = "%Y-%m-%dT%H%M%S.log";

const COLOR_DEBUG: Color = Color::Magenta;
const COLOR_INFO: Color = Color::BrightGreen;
const COLOR_WARN: Color = Color::BrightYellow;
const COLOR_ERROR: Color = Color::Red;
const COLOR_TRACE: Color = Color::Blue;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogLevelFilter(pub log::LevelFilter);

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LogWriter {
    pub label: String,
    pub level: Option<LogLevelFilter>,
    pub output: Option<String>, // absence => for
    pub file_prefix: Option<String>,
    pub file_timestamp: Option<String>,
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

impl LogWriter {
    pub fn new(
        label: &str,
        level: Option<log::LevelFilter>,
        output: Option<&str>,
        file_prefix: Option<&str>,
        file_timestamp: Option<&str>,
    ) -> Self {
        LogWriter {
            label: label.to_owned(),
            level: level.map(LogLevelFilter),
            output: output.map(|x| x.to_owned()),
            file_prefix: file_prefix.map(|x| x.to_owned()),
            file_timestamp: file_timestamp.map(|x| x.to_owned()),
        }
    }
    pub fn get_full_prefix(&self) -> Option<String> {
        self.output.as_ref().map(|filepath| {
            format!(
                "{}/{}",
                filepath,
                self.file_prefix.clone().unwrap_or(DEFAULT_LOG_PREFIX.to_owned())
            )
        })
    }
    pub fn start(&self, to_stdout: bool) -> CpResult<()> {
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
        let dispatch = match &self.get_full_prefix() {
            Some(full_file_prefix) => {
                let d = if to_stdout {
                    base_dispatch.chain(std::io::stdout())
                } else {
                    base_dispatch
                };
                d.chain(fern::DateBased::new(
                    full_file_prefix,
                    self.file_timestamp
                        .clone()
                        .unwrap_or(DEFAULT_TIMESTAMP_SUFFIX.to_owned()),
                ))
            }
            None => base_dispatch.chain(std::io::stdout()),
        };

        match dispatch.apply() {
            Ok(_) => Ok(()),
            Err(e) => Err(CpError::ComponentError(
                "Logger",
                format!("Failed to setup logger, quiting: {:?}", e),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use log::{error, warn};

    use crate::logger::writer::{DEFAULT_LOG_PREFIX, LogLevelFilter, LogWriter};

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
        {
            let writer = LogWriter::new("test", None, Some("/tmp/"), None, None);
            assert_eq!(
                std::path::Path::new(writer.get_full_prefix().unwrap().as_str()),
                std::path::Path::new(format!("/tmp/{}", DEFAULT_LOG_PREFIX).as_str())
            );
        }
        {
            let writer = LogWriter::new("test", None, Some("/tmp/"), Some("custom"), None);
            assert_eq!(
                std::path::Path::new(writer.get_full_prefix().unwrap().as_str()),
                std::path::Path::new(format!("/tmp/{}", "custom".to_string()).as_str())
            );
        }
        {
            let writer = LogWriter::new("test", None, None, Some("custom"), None);
            assert_eq!(writer.get_full_prefix(), None);
        }
    }

    #[test]
    fn start_console_log() {
        let writer = LogWriter::new("test", None, None, None, None);
        writer.start(true).unwrap();
        warn!("WARN WARN WARN");
        error!("test test test");
    }
}
