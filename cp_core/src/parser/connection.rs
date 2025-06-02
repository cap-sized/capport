use serde::{Deserialize, Serialize};

use crate::{context::envvar::get_env_var_str, util::common::EnvKeyType};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub label: String,
    pub host_env: Option<String>,
    pub port: Option<u32>,
    pub user_env: Option<String>,
    pub password_env: Option<String>,
    pub db_env: Option<String>,
}

impl ConnectionConfig {
    pub fn get_key_from_env(&self, env_key_type: EnvKeyType) -> Option<String> {
        let env_key = match env_key_type {
            EnvKeyType::DbName => self.db_env.as_ref(),
            EnvKeyType::Password => self.password_env.as_ref(),
            EnvKeyType::Host => self.host_env.as_ref(),
            EnvKeyType::User => self.user_env.as_ref(),
        };
        if let Some(ek) = env_key {
            get_env_var_str(ek).ok()
        } else {
            None
        }
    }

    pub fn to_url(&self) -> String {
        let dbname = self.get_key_from_env(EnvKeyType::DbName).unwrap_or("".to_string());
        let port = if let Some(p) = &self.port {
            format!(":{}", p)
        } else {
            "".to_string()
        };
        let host = format!("{}{}", self.get_key_from_env(EnvKeyType::Host).unwrap_or("localhost".to_owned()), port);
        let user = self.get_key_from_env(EnvKeyType::User);
        let pw = self.get_key_from_env(EnvKeyType::Password);
        let userpw = if let Some(u) = user {
            format!("{}{}{}@", u, 
                if pw.is_some() { ":" } else { "" },
                pw.unwrap_or("".to_string())
            )
        } else {
            String::new()
        };

        format!("{}{}/{}", userpw, host, dbname)
    }

}

#[cfg(test)]
mod tests {
    use crate::{
        context::envvar::EnvironmentVariableRegistry, parser::connection::ConnectionConfig, util::common::EnvKeyType,
    };

    #[test]
    fn valid_parse_connection_config_get_url() {
        let mut env_var = EnvironmentVariableRegistry::new();
        env_var.set_str("POSTGRES_URL_ENV", "postgres:5432".to_owned()).unwrap();
        env_var.set_str("MYSQL_HOST_ENV", "mysql".to_owned()).unwrap();
        env_var.set_str("MY_USER_ENV", "myuser".to_owned()).unwrap();
        env_var.set_str("MYPASS_ENV", "mypass".to_owned()).unwrap();
        env_var.set_str("ALTDB_ENV", "altdb".to_owned()).unwrap();
        let test = ConnectionConfig {
            label: "test".to_owned(),
            host_env: Some("POSTGRES_URL_ENV".to_owned()),
            port: None,
            user_env: Some("MY_USER_ENV".to_owned()),
            db_env: Some("DB_ENV".to_owned()),
            password_env: Some("MYPASS_ENV".to_owned()),
        };
        let hostport = ConnectionConfig {
            label: "hp".to_owned(),
            password_env: None,
            host_env: Some("MYSQL_HOST_ENV".to_owned()),
            port: Some(3306),
            user_env: None,
            db_env: Some("ALTDB_ENV".to_owned()),
        };
        let all_defaults = ConnectionConfig {
            label: "def".to_owned(),
            password_env: None,
            host_env: None,
            port: None,
            user_env: None,
            db_env: None,
        };
        assert_eq!(
            test.get_key_from_env(EnvKeyType::Host).unwrap(),
            "postgres:5432".to_owned()
        );
        assert_eq!(test.get_key_from_env(EnvKeyType::User).unwrap(), "myuser".to_owned());
        assert_eq!(
            test.get_key_from_env(EnvKeyType::Password).unwrap(),
            "mypass".to_owned()
        );
        assert!(test.get_key_from_env(EnvKeyType::DbName).is_none());
        assert_eq!(test.to_url(), "myuser:mypass@postgres:5432/".to_owned());
        assert_eq!(
            hostport.get_key_from_env(EnvKeyType::Host).unwrap(),
            "mysql".to_owned()
        );
        assert!(hostport.get_key_from_env(EnvKeyType::User).is_none());
        assert!(hostport.get_key_from_env(EnvKeyType::Password).is_none());
        assert_eq!(hostport.get_key_from_env(EnvKeyType::DbName).unwrap(), "altdb".to_owned());
        assert_eq!(test.to_url(), "myuser:mypass@postgres:5432/".to_owned());
        assert_eq!(hostport.to_url(), "mysql:3306/altdb".to_owned());
        assert_eq!(all_defaults.to_url(), "localhost/".to_owned());
    }
}
