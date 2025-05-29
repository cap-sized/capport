use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub label: String,
    pub url_env: Option<String>,
    pub user_env: Option<String>,
    pub password_env: Option<String>,
    pub db_env: Option<String>,
}
