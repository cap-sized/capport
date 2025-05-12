use polars::prelude::JoinType;
use serde::{Deserialize, Deserializer, Serialize, de};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JType(pub polars::prelude::JoinType);

impl From<JType> for JoinType {
    fn from(w: JType) -> Self {
        w.0
    }
}

impl Serialize for JType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let enum_type = &self.0;
        let repr = format!("{:?}", enum_type).trim().to_lowercase().to_owned();
        serializer.serialize_str(&repr)
    }
}

impl<'de> Deserialize<'de> for JType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "left" => Ok(JType(JoinType::Left)),
            "right" => Ok(JType(JoinType::Right)),
            "full" => Ok(JType(JoinType::Full)),
            "cross" => Ok(JType(JoinType::Cross)),
            "inner" => Ok(JType(JoinType::Inner)),
            s => Err(de::Error::custom(format!("Unknown jointype in model: {}", s))),
        }
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::JoinType;

    use super::JType;

    fn example_jtype() -> Vec<JType> {
        [
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::Cross,
            JoinType::Inner,
        ]
        .map(JType)
        .into_iter()
        .collect::<Vec<_>>()
    }

    fn example_str() -> Vec<String> {
        ["left", "right", "full", "cross", "inner"]
            .map(|x| x.to_owned())
            .into_iter()
            .collect::<Vec<_>>()
    }

    #[test]
    fn valid_dtype_ser() {
        let actual_str = example_jtype()
            .iter()
            .map(|x| serde_yaml_ng::to_string(x).unwrap().trim().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(actual_str, example_str());
    }

    #[test]
    fn valid_jtype_de() {
        let actual_jtype = example_str()
            .iter()
            .map(|x| serde_yaml_ng::from_str::<JType>(x).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual_jtype, example_jtype());
    }

    #[test]
    fn invalid_jtype_de() {
        assert!(serde_yaml_ng::from_str::<JType>("bad").is_err());
    }

    #[test]
    fn valid_jtype_from() {
        let dtype: JType = JType(JoinType::Inner);
        assert_eq!(JoinType::from(dtype), JoinType::Inner)
    }
}
