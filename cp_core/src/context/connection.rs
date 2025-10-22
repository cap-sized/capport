use std::collections::HashMap;

use crate::{
    parser::{common::YamlRead, connection::{ConnectionConfig, NetworkConnection}},
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
    pub fn get_connection(&self, conn_name: &str, user: &str) -> Option<NetworkConnection> {
        self.configs.get(conn_name).map(|x| x.get_connection(user))
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
    use std::collections::HashMap;

    use crate::{
        context::{common::Configurable, envvar::EnvironmentVariableRegistry},
        parser::connection::{ConnectionConfig, NetworkConnection},
        util::common::create_config_pack,
    };

    use super::ConnectionRegistry;

    #[test]
    fn valid_unpack_connection_registry() {
        let configs = [
            "
connection:
    test:
        host: localhost
        port: 5432
        db_name: default
        users:
            admin: MYPASS_ENV
            public:
irrelevant_node:
    for_testing:
        a: b
        ",
            "
connection:
    pwonly:
        port: 1988
        db_name: nineteen
        users:
            admin: DUMMY
",
        ];
        let mut env_var = EnvironmentVariableRegistry::new();
        env_var.set_str("MYPASS_ENV", "mypass".to_owned()).unwrap();
        env_var.set_str("DUMMY", "dummy".to_owned()).unwrap();
        let mut config_pack = create_config_pack(configs);
        let actual = ConnectionRegistry::from(&mut config_pack).unwrap();
        assert_eq!(
            actual.get_connection_config("test").unwrap(),
            ConnectionConfig {
                label: "test".to_owned(),
                port: 5432,
                host: Some("localhost".to_owned()),
                db_name: "default".to_owned(),
                users: HashMap::from([
                    ("admin".to_owned(), Some("MYPASS_ENV".to_string())),
                    ("public".to_owned(), None)
                ])
            }
        );
        assert_eq!(
            actual.get_connection_config("pwonly").unwrap(),
            ConnectionConfig {
                label: "pwonly".to_owned(),
                host: None,
                port: 1988,
                db_name: "nineteen".to_owned(),
                users: HashMap::from([
                    ("admin".to_owned(), Some("DUMMY".to_string()))
                ])
            }
        );
        assert_eq!(
            actual.get_connection("pwonly", "admin").unwrap(),
            NetworkConnection::new("localhost", 1988, "nineteen", "admin", Some("dummy".to_owned()))
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

