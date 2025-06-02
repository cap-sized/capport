use std::collections::HashMap;

use crate::{
    parser::{common::YamlRead, connection::ConnectionConfig},
    util::error::{CpError, CpResult},
};

use super::common::Configurable;

#[derive(Debug)]
pub struct ConnectionRegistry {
    configs: HashMap<String, ConnectionConfig>,
}

impl Default for ConnectionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionRegistry {
    pub fn new() -> ConnectionRegistry {
        ConnectionRegistry {
            configs: HashMap::new(),
        }
    }
    pub fn insert(&mut self, conn: ConnectionConfig) -> Option<ConnectionConfig> {
        let prev = self.configs.remove(&conn.label);
        self.configs.insert(conn.label.clone(), conn);
        prev
    }
    pub fn from(
        config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    ) -> CpResult<ConnectionRegistry> {
        let mut reg = ConnectionRegistry {
            configs: HashMap::new(),
        };
        reg.extract_parse_config(config_pack)?;
        Ok(reg)
    }
    pub fn get_connection_config(&self, conn_name: &str) -> Option<ConnectionConfig> {
        self.configs.get(conn_name).map(|x| x.to_owned())
    }
}

impl Configurable for ConnectionRegistry {
    fn get_node_name() -> &'static str {
        "connection"
    }
    fn extract_parse_config(
        &mut self,
        config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    ) -> CpResult<()> {
        let configs = config_pack
            .remove(ConnectionRegistry::get_node_name())
            .unwrap_or_default();
        let mut errors = vec![];
        for (label, mut fields) in configs {
            fields.add_to_map(
                serde_yaml_ng::Value::String("label".to_owned()),
                serde_yaml_ng::Value::String(label.clone()),
            )?;
            match serde_yaml_ng::from_value::<ConnectionConfig>(fields) {
                Ok(connection) => {
                    self.configs.insert(label.clone(), connection);
                }
                Err(e) => {
                    errors.push(CpError::ConfigError(
                        "Connection",
                        format!("{}: {:?}", label, e.to_string()),
                    ));
                }
            };
        }
        if !errors.is_empty() {
            Err(CpError::ConfigError(
                "ConnectionRegistry: connection",
                format!("Errors parsing:\n{:?}", errors),
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        context::{common::Configurable, envvar::EnvironmentVariableRegistry}, parser::connection::ConnectionConfig,
        util::common::create_config_pack,
    };

    use super::ConnectionRegistry;

    #[test]
    fn valid_unpack_connection_registry() {
        let configs = [
            "
connection:
    test:
        host_env: POSTGRES_URL_ENV
        user_env: MY_USER_ENV
        port: 5432
        db_env: DB_ENV
irrelevant_node:
    for_testing:
        a: b
        ",
            "
connection:
    pwonly: 
        password_env: MYPASS_ENV
        port: 3306
    nothing: {}
",
        ];
        let mut env_var = EnvironmentVariableRegistry::new();
        env_var.set_str("POSTGRES_URL_ENV", "postgres:5432".to_owned()).unwrap();
        env_var.set_str("MY_USER_ENV", "myuser".to_owned()).unwrap();
        env_var.set_str("MYPASS_ENV", "mypass".to_owned()).unwrap();
        let mut config_pack = create_config_pack(configs);
        let actual = ConnectionRegistry::from(&mut config_pack).unwrap();
        assert_eq!(
            actual.get_connection_config("test").unwrap(),
            ConnectionConfig {
                label: "test".to_owned(),
                port: Some(5432),
                host_env: Some("POSTGRES_URL_ENV".to_owned()),
                user_env: Some("MY_USER_ENV".to_owned()),
                db_env: Some("DB_ENV".to_owned()),
                password_env: None
            }
        );
        assert_eq!(
            actual.get_connection_config("pwonly").unwrap(),
            ConnectionConfig {
                label: "pwonly".to_owned(),
                port: Some(3306),
                password_env: Some("MYPASS_ENV".to_owned()),
                host_env: None,
                user_env: None,
                db_env: None,
            }
        );
        assert_eq!(
            actual.get_connection_config("nothing").unwrap(),
            ConnectionConfig {
                label: "nothing".to_owned(),
                port: None,
                password_env: None,
                host_env: None,
                user_env: None,
                db_env: None,
            }
        );
    }

    #[test]
    fn invalid_unpack_connection_registry() {
        let configs = [
            "
connection:
    test:
        host_env: POSTGRES_URL_ENV
        user_env: MY_USER_ENV
        port: invalid
        db_env: DB_ENV
",
            "
connection:
    bad_nothing:
",
        ];
        for config in configs {
            let mut config_pack = create_config_pack([config]);
            let mut actual = ConnectionRegistry::default();
            assert!(actual.extract_parse_config(&mut config_pack).is_err());
        }
    }
}
