use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{context::envvar::get_env_var_str};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkConnection {
    pub host: String,
    pub port: u32,
    pub db_name: String,
    pub username: String,
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub label: String,
    pub host: Option<String>,
    pub port: u32,
    pub db_name: String,
    pub users: HashMap<String, Option<String>>,
}

impl ConnectionConfig {
    pub fn get_user_password(&self, user: &str) -> Option<String> {
        match self.users.get(user) {
            Some(ek) => get_env_var_str(ek.clone().unwrap_or(String::new()).as_ref()).ok(),
            None => None
        }
    }
    pub fn get_connection(&self, user: &str) -> NetworkConnection {
        NetworkConnection { host: self.host.clone().unwrap_or("localhost".to_owned()), port: self.port, db_name: self.db_name.clone(), username: user.to_owned(), password: self.get_user_password(user) }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        context::envvar::EnvironmentVariableRegistry, parser::connection::{ConnectionConfig, NetworkConnection},
    };

    #[test]
    fn valid_parse_connection_config_get_url() {
        let configs = [
            "
label: test
port: 80
db_name: web
users:
    default: MYPASS_ENV
                ",
            "
label: hp
host: 8.8.8.8
port: 3306
db_name: main
users:
    user: ALTDB_ENV
                ",
        ];
        let mut env_var = EnvironmentVariableRegistry::new();
        env_var.set_str("MYPASS_ENV", "mypass".to_owned()).unwrap();
        env_var.set_str("ALTDB_ENV", "altdb".to_owned()).unwrap();
        let test = ConnectionConfig {
            label: "test".to_owned(),
            host: None,
            port: 80,
            db_name: "web".to_owned(),
            users: HashMap::from([
                ("default".to_string(), Some("MYPASS_ENV".to_owned()))
            ])
        };
        let hostport = ConnectionConfig {
            label: "hp".to_owned(),
            host: Some("8.8.8.8".to_owned()),
            db_name: "main".to_owned(),
            port: 3306,
            users: HashMap::from([
                ("user".to_string(), Some("ALTDB_ENV".to_owned()))
            ])
        };
        assert_eq!(test, serde_yaml_ng::from_str(configs[0]).unwrap());
        assert_eq!(hostport, serde_yaml_ng::from_str(configs[1]).unwrap());
        assert_eq!(test.get_user_password("default").unwrap(), "mypass");
        assert_eq!(test.get_connection("default"), NetworkConnection {
            host: "localhost".to_owned(),
            port: 80,
            db_name: "web".to_owned(),
            username: "default".to_owned(),
            password: Some("mypass".to_owned())
        });
        assert_eq!(test.get_connection("not_defined"), NetworkConnection {
            host: "localhost".to_owned(),
            port: 80,
            db_name: "web".to_owned(),
            username: "not_defined".to_owned(),
            password: None
        });
        assert_eq!(hostport.get_user_password("user").unwrap(), "altdb");
    }
}

