use std::collections::HashMap;

use yaml_rust2::Yaml;

use crate::{
    logger::writer::LogWriter,
    util::error::{CpError, CpResult},
};

use super::common::Configurable;

pub struct LoggerRegistry {
    registry: HashMap<String, LogWriter>,
}
impl LoggerRegistry {
    pub fn new() -> LoggerRegistry {
        LoggerRegistry {
            registry: HashMap::new(),
        }
    }
    pub fn from(config_pack: &mut HashMap<String, HashMap<String, Yaml>>) -> CpResult<LoggerRegistry> {
        let mut reg = LoggerRegistry {
            registry: HashMap::new(),
        };
        reg.extract_parse_config(config_pack)?;
        Ok(reg)
    }
}

impl Configurable for LoggerRegistry {
    fn get_node_name() -> &'static str {
        "logger"
    }
    fn extract_parse_config(&mut self, config_pack: &mut HashMap<String, HashMap<String, Yaml>>) -> CpResult<()> {
        let configs = config_pack.remove(LoggerRegistry::get_node_name()).unwrap_or_default();
        for (config_name, node) in configs {
            let pipeline = match parse_logger(&config_name, &node) {
                Ok(x) => x,
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
