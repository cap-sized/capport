use std::collections::HashMap;

use crate::{
    parser::{common::YamlRead, connection::ConnectionConfig},
    util::{
        common::EnvKeyType,
        error::{CpError, CpResult},
    },
};

use super::{common::Configurable, envvar::get_env_var_str};

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
    pub fn get_key_from_env(&self, conn_name: &str, env_key_type: EnvKeyType) -> Option<String> {
        self.get_connection_config(conn_name)
            .map(|config| {
                let env_key = match env_key_type {
                    EnvKeyType::DbName => config.db_env.as_ref(),
                    EnvKeyType::Password => config.password_env.as_ref(),
                    EnvKeyType::Url => config.url_env.as_ref(),
                    EnvKeyType::User => config.user_env.as_ref(),
                };
                env_key?;
                get_env_var_str(env_key.unwrap()).ok()
            })
            .unwrap_or(None)
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
        context::envvar::EnvironmentVariableRegistry,
        parser::connection::ConnectionConfig,
        util::common::{EnvKeyType, create_config_pack},
    };

    use super::ConnectionRegistry;

    #[test]
    fn valid_unpack_request_registry() {
        let configs = [
            "
connection:
    test:
        url_env: POSTGRES_URL_ENV
        user_env: MY_USER_ENV
        db_env: DB_ENV
irrelevant_node:
    for_testing:
        a: b
        ",
            "
connection:
    pwonly: 
        password_env: MYPASS_ENV
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
                url_env: Some("POSTGRES_URL_ENV".to_owned()),
                user_env: Some("MY_USER_ENV".to_owned()),
                db_env: Some("DB_ENV".to_owned()),
                password_env: None
            }
        );
        assert_eq!(
            actual.get_connection_config("pwonly").unwrap(),
            ConnectionConfig {
                label: "pwonly".to_owned(),
                password_env: Some("MYPASS_ENV".to_owned()),
                url_env: None,
                user_env: None,
                db_env: None,
            }
        );
        assert_eq!(
            actual.get_key_from_env("test", EnvKeyType::Url).unwrap(),
            "postgres:5432".to_owned()
        );
        assert_eq!(
            actual.get_key_from_env("test", EnvKeyType::User).unwrap(),
            "myuser".to_owned()
        );
        assert_eq!(
            actual.get_key_from_env("pwonly", EnvKeyType::Password).unwrap(),
            "mypass".to_owned()
        );
        assert!(actual.get_key_from_env("test", EnvKeyType::DbName).is_none());
    }
}
