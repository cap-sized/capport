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
    pub readonly: Option<u8>,
    pub connection_timeout_ms: Option<u16>,
    pub send_retries: Option<u8>,
    pub retry_timeout_ms: Option<u16>,
}

impl NetworkConnection {
    pub fn new(host: &str, port: u32, db_name: &str, username: &str, password: Option<String>) -> NetworkConnection {
        NetworkConnection { 
            host: host.to_owned(), 
            port, 
            db_name: db_name.to_owned(), 
            username: username.to_owned(), 
            password,
            readonly: None,
            connection_timeout_ms: None, 
            send_retries: None, 
            retry_timeout_ms: None 
        }
    }

    pub fn to_uri(&self) -> String {
        let password = self.password.as_deref().map(|x| format!(":{}", x)).unwrap_or_default();
        let readonly = self.readonly.map(|x| format!("&readonly={}", x)).unwrap_or_default();
        let connection_timeout = self.connection_timeout_ms.map(|x| format!("&connection_timeout={}ms", x)).unwrap_or_default();
        let retry_timeout = self.retry_timeout_ms.map(|x| format!("&retry_timeout={}ms", x)).unwrap_or_default();
        let send_retries = self.send_retries.map(|x| format!("&send_retries={}", x)).unwrap_or_default();
        format!("tcp://{}{}@{}:{}/{}?compression=lz4{}{}{}{}", &self.username, &password, 
            &self.host, &self.port, &self.db_name, &readonly, &connection_timeout, &retry_timeout, &send_retries
        )
    }
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
        let password = self.get_user_password(user);
        NetworkConnection::new(self.host.as_deref().unwrap_or("localhost"), self.port, &self.db_name, user, password)
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
        assert_eq!(test.get_connection("default"), NetworkConnection::new("localhost", 80, "web", "default", Some("mypass".to_owned())));
        assert_eq!(test.get_connection("not_defined"), NetworkConnection::new("localhost", 80, "web", "not_defined", None));
        assert_eq!(hostport.get_user_password("user").unwrap(), "altdb");
    }
}

