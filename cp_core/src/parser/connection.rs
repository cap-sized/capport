use serde::{Deserialize, Serialize};

use crate::{context::envvar::get_env_var_str, util::common::EnvKeyType};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub label: String,
    pub url_env: Option<String>,
    pub user_env: Option<String>,
    pub password_env: Option<String>,
    pub db_env: Option<String>,
}

impl ConnectionConfig {
    pub fn get_key_from_env(&self, env_key_type: EnvKeyType) -> Option<String> {
        let env_key = match env_key_type {
            EnvKeyType::DbName => self.db_env.as_ref(),
            EnvKeyType::Password => self.password_env.as_ref(),
            EnvKeyType::Url => self.url_env.as_ref(),
            EnvKeyType::User => self.user_env.as_ref(),
        };
        get_env_var_str(env_key.unwrap()).ok()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        context::envvar::EnvironmentVariableRegistry,
        parser::connection::ConnectionConfig,
        util::common::EnvKeyType,
    };

    #[test]
    fn valid_unpack_request_registry() {
        let mut env_var = EnvironmentVariableRegistry::new();
        env_var.set_str("POSTGRES_URL_ENV", "postgres:5432".to_owned()).unwrap();
        env_var.set_str("MY_USER_ENV", "myuser".to_owned()).unwrap();
        env_var.set_str("MYPASS_ENV", "mypass".to_owned()).unwrap();
        let test = ConnectionConfig {
            label: "test".to_owned(),
            url_env: Some("POSTGRES_URL_ENV".to_owned()),
            user_env: Some("MY_USER_ENV".to_owned()),
            db_env: Some("DB_ENV".to_owned()),
            password_env: None
        };
        let pwonly = ConnectionConfig {
            label: "pwonly".to_owned(),
            password_env: Some("MYPASS_ENV".to_owned()),
            url_env: None,
            user_env: None,
            db_env: None,
        };
        assert_eq!(
            test.get_key_from_env(EnvKeyType::Url).unwrap(),
            "postgres:5432".to_owned()
        );
        assert_eq!(
            test.get_key_from_env(EnvKeyType::User).unwrap(),
            "myuser".to_owned()
        );
        assert_eq!(
            pwonly.get_key_from_env(EnvKeyType::Password).unwrap(),
            "mypass".to_owned()
        );
        assert!(test.get_key_from_env(EnvKeyType::DbName).is_none());
    }
}
