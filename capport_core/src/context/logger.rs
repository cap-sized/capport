use std::collections::HashMap;

use log::info;

use crate::{
    logger::common::{DEFAULT_CONSOLE_LOGGER_NAME, Logger},
    parser::logger::parse_logger,
    util::error::{CpError, CpResult},
};

use super::common::Configurable;

#[derive(Debug)]
pub struct LoggerRegistry {
    registry: HashMap<String, Logger>,
    running_logger: Option<String>,
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
                Logger::new(DEFAULT_CONSOLE_LOGGER_NAME, None, None, None, None),
            )]),
            running_logger: None,
        }
    }
    pub fn from(config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>) -> CpResult<LoggerRegistry> {
        let mut reg = LoggerRegistry::new();
        reg.extract_parse_config(config_pack)?;
        Ok(reg)
    }
    pub fn get_logger(&self, logger_name: &str) -> Option<Logger> {
        self.registry.get(logger_name).map(|x| x.to_owned())
    }

    pub fn start_logger(&mut self, logger_name: &str, pipeline_name: &str, to_console: bool) -> CpResult<()> {
        if self.running_logger.is_some() {
            return Err(CpError::ComponentError(
                "config.logger",
                format!("Logger {} already running", self.running_logger.clone().unwrap()),
            ));
        }
        let logger = match self.registry.get_mut(logger_name) {
            Some(x) => x,
            None => {
                return Err(CpError::ComponentError(
                    "config.logger",
                    format!("Logger {} not found", logger_name),
                ));
            }
        };
        logger.start(pipeline_name, to_console)?;
        let _ = self.running_logger.insert(logger_name.to_owned());
        Ok(())
    }

    pub fn show_output(&self) {
        if self.running_logger.is_none() {
            return;
        }
        let logger_name = self.running_logger.as_ref().unwrap();
        if let Some(x) = &self.get_logger(logger_name) {
            info!(
                "Logger {}: Printed output to {}",
                logger_name,
                x._final_output_path.clone().unwrap_or("console".to_owned())
            )
        }
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
    use std::{fs, io::ErrorKind};

    use crate::{logger::common::Logger, util::common::create_config_pack};

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
        let expected = Logger::new(
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
        let expected = Logger::new("base_log", None, Some("/tmp/"), None, None);
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
        let expected = Logger::new("base_log", Some(log::LevelFilter::Warn), Some("/tmp/"), None, None);
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
        let expected = Logger::new(
            "base_log",
            Some(log::LevelFilter::Error),
            Some("/tmp/"),
            Some("myprogram_"),
            None,
        );
        assert_eq!(actual, expected);
    }

    #[test]
    fn invalid_field() {
        let config = "
base_log:
    level: bad
    output: /tmp/
    file_prefix: myprogram_
    _final_output_path: something
        ";
        assert_invalid_model(config);
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

    #[test]
    fn invalid_starting_two_loggers() {
        let dir_path = "/tmp/capport_testing/invalid_starting_two_loggers/";
        match fs::create_dir_all(dir_path) {
            Ok(_) => {}
            Err(e) => {
                if e.kind() != ErrorKind::DirectoryNotEmpty {
                    panic!("failed to create directory: {:?}", e)
                }
            }
        };

        let config = format!(
            "
base_log:
    level: error
    output: {}
    file_prefix: myprogram_
second_log:
    level: error
    output: {}
    file_prefix: anothermyprogram_
        ",
            dir_path, dir_path
        );
        let mut reg = create_logger_registry(&config);
        reg.start_logger("base_log", "test", true).unwrap();
        assert!(reg.start_logger("second_log", "test", true).is_err());
        fs::remove_dir_all(dir_path).unwrap();
    }
}
