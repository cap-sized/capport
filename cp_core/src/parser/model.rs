use serde::{Deserialize, Serialize, de};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ModelConstraint {
    Unique,
    Primary,
    NotNull,
}

impl<'de> Deserialize<'de> for ModelConstraint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?.to_lowercase();
        match s.as_str() {
            "unique" => Ok(ModelConstraint::Unique),
            "primary" => Ok(ModelConstraint::Primary),
            "notnull" => Ok(ModelConstraint::NotNull),
            "not_null" => Ok(ModelConstraint::NotNull),
            "not null" => Ok(ModelConstraint::NotNull),
            // TODO: Handle parsing of list[xxx] types, more datetime regions
            s => Err(de::Error::custom(format!("Unknown constraint in model: {}", s))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ModelConstraint;

    fn example_constraint() -> Vec<ModelConstraint> {
        [
            ModelConstraint::Unique,
            ModelConstraint::Primary,
            ModelConstraint::Unique,
            ModelConstraint::Primary,
            ModelConstraint::NotNull,
            ModelConstraint::NotNull,
            ModelConstraint::NotNull,
        ]
        .into_iter()
        .collect::<Vec<_>>()
    }

    fn example_str() -> Vec<&'static str> {
        [
            "unique", "PrimarY", "Unique", "PRIMARY", "not null", "NotNull", "not_null",
        ]
        .into_iter()
        .collect::<Vec<_>>()
    }

    #[test]
    fn valid_model_constraints_ser() {
        let actual_str = [
            ModelConstraint::Unique,
            ModelConstraint::Primary,
            ModelConstraint::NotNull,
        ]
        .iter()
        .map(|f| serde_yaml_ng::to_string(f).map(|x| x.trim().to_owned()).unwrap())
        .collect::<Vec<_>>();
        assert_eq!(actual_str, vec!["Unique", "Primary", "NotNull"]);
    }

    #[test]
    fn valid_model_constraints_de() {
        let actual_contraint = example_str()
            .iter()
            .map(|x| serde_yaml_ng::from_str::<ModelConstraint>(x).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual_contraint, example_constraint());
    }
}
