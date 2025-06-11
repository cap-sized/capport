use serde::{Deserialize, Serialize, de};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum HttpMethod {
    Get,
}

impl<'de> Deserialize<'de> for HttpMethod {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?.to_lowercase();
        match s.as_str() {
            "get" => Ok(HttpMethod::Get),
            // TODO: Handle parsing of list[xxx] types, more datetime regions
            s => Err(de::Error::custom(format!("Unknown http_method: {}", s))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct HttpOptionsConfig {
    pub max_retry: Option<u8>,
    pub init_retry_interval_ms: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::HttpMethod;

    fn example_http_method() -> Vec<HttpMethod> {
        [HttpMethod::Get, HttpMethod::Get].into_iter().collect::<Vec<_>>()
    }

    fn example_str() -> Vec<&'static str> {
        ["get", "GET"].into_iter().collect::<Vec<_>>()
    }

    #[test]
    fn valid_http_method_ser() {
        let actual_str = example_http_method()
            .iter()
            .map(|x| serde_yaml_ng::to_string(x).unwrap().trim().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            actual_str,
            example_str()
                .into_iter()
                .map(|x| format!("{}{}", x.chars().next().unwrap().to_uppercase(), x[1..].to_lowercase()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn valid_http_method_de() {
        let actual_contraint = example_str()
            .iter()
            .map(|x| serde_yaml_ng::from_str::<HttpMethod>(x).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual_contraint, example_http_method());
    }
}
