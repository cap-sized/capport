use std::collections::HashMap;

use crate::{
    logger::writer::{DEFAULT_CONSOLE_LOGGER_NAME, LogWriter},
    parser::logger::parse_logger,
    util::error::{CpError, CpResult},
};

use super::common::Configurable;

#[derive(Debug)]
pub struct LoggerRegistry {
    registry: HashMap<String, LogWriter>,
}

impl Default for LoggerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl LoggerRegistry {
    pub fn new() -> LoggerRegistry {
        LoggerRegistry {
            registry: HashMap::from([(
                DEFAULT_CONSOLE_LOGGER_NAME.to_owned(),
                LogWriter::new(DEFAULT_CONSOLE_LOGGER_NAME, None, None, None, None),
            )]),
        }
    }
    pub fn from(config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>) -> CpResult<LoggerRegistry> {
        let mut reg = LoggerRegistry::new();
        reg.extract_parse_config(config_pack)?;
        Ok(reg)
    }
    pub fn get_logger(&self, logger_name: &str) -> Option<LogWriter> {
        self.registry.get(logger_name).map(|x| x.to_owned())
    }

    pub fn start_logger(&self, logger_name: &str, to_console: bool) -> CpResult<()> {
        let logger = match self.registry.get(logger_name) {
            Some(x) => x,
            None => {
                return Err(CpError::ComponentError(
                    "config.logger",
                    format!("Logger {} not found", logger_name),
                ));
            }
        };
        logger.start(to_console)?;
        Ok(())
    }
}

impl Configurable for LoggerRegistry {
    fn get_node_name() -> &'static str {
        "logger"
    }
    fn extract_parse_config(
        &mut self,
        config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    ) -> CpResult<()> {
        let configs = config_pack.remove(LoggerRegistry::get_node_name()).unwrap_or_default();
        for (config_name, node) in configs {
            let pipeline = match parse_logger(&config_name, node) {
                Ok(x) => {
                    if x.output.is_none() {
                        return Err(CpError::ComponentError(
                            "config.logger",
                            format!(
                                "Invalid logger config {}: Configured logger output cannot be empty",
                                &config_name
                            ),
                        ));
                    }
                    x
                }
                Err(e) => {
                    return Err(CpError::ComponentError(
                        "config.logger",
                        format!("Logger {}: {}", config_name, e),
                    ));
                }
            };
            self.registry.insert(config_name.to_string(), pipeline);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{logger::writer::LogWriter, util::common::create_config_pack};

    use super::LoggerRegistry;

    fn create_logger_registry(yaml_str: &str) -> LoggerRegistry {
        let mut config_pack = create_config_pack(yaml_str, "logger");
        LoggerRegistry::from(&mut config_pack).unwrap()
    }

    fn assert_invalid_model(yaml_str: &str) {
        let mut config_pack = create_config_pack(yaml_str, "logger");
        LoggerRegistry::from(&mut config_pack).unwrap_err();
    }

    #[test]
    fn valid_one_full_log_writer() {
        let config = "
base_log:
    level: debug
    output: /tmp/
    file_prefix: myprogram_
    file_timestamp: \"%Y-%m-%d\"
        ";
        let reg = create_logger_registry(config);
        println!("{:?}", reg);
        let actual = reg.get_logger("base_log").unwrap();
        let expected = LogWriter::new(
            "base_log",
            Some(log::LevelFilter::Debug),
            Some("/tmp/"),
            Some("myprogram_"),
            Some("%Y-%m-%d"),
        );
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_missing_output_prefix_timestamp_fields() {
        let config = "
base_log:
    output: /tmp/
        ";
        let reg = create_logger_registry(config);
        println!("{:?}", reg);
        let actual = reg.get_logger("base_log").unwrap();
        let expected = LogWriter::new("base_log", None, Some("/tmp/"), None, None);
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_missing_prefix_timestamp_fields() {
        let config = "
base_log:
    level: warn
    output: /tmp/
        ";
        let reg = create_logger_registry(config);
        println!("{:?}", reg);
        let actual = reg.get_logger("base_log").unwrap();
        let expected = LogWriter::new("base_log", Some(log::LevelFilter::Warn), Some("/tmp/"), None, None);
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_missing_timestamp_fields() {
        let config = "
base_log:
    level: error
    output: /tmp/
    file_prefix: myprogram_
        ";
        let reg = create_logger_registry(config);
        println!("{:?}", reg);
        let actual = reg.get_logger("base_log").unwrap();
        let expected = LogWriter::new(
            "base_log",
            Some(log::LevelFilter::Error),
            Some("/tmp/"),
            Some("myprogram_"),
            None,
        );
        assert_eq!(actual, expected);
    }

    #[test]
    fn invalid_level() {
        let config = "
base_log:
    level: bad
    output: /tmp/
    file_prefix: myprogram_
        ";
        assert_invalid_model(config);
    }

    #[test]
    fn invalid_missing_output() {
        [
            "
base_log:
        ",
            "
base_log:
    level: error
        ",
            "
base_log:
    file_prefix: myprogram_
        ",
        ]
        .iter()
        .for_each(|config| {
            assert_invalid_model(config);
        });
    }
}
