use serde::{Deserialize, Serialize, de};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ModelConstraint {
    Unique,
    Foreign,
}

impl<'de> Deserialize<'de> for ModelConstraint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?.to_lowercase();
        match s.as_str() {
            "unique" => Ok(ModelConstraint::Unique),
            "foreign" => Ok(ModelConstraint::Foreign),
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
            ModelConstraint::Foreign,
            ModelConstraint::Unique,
            ModelConstraint::Foreign,
        ]
        .into_iter()
        .collect::<Vec<_>>()
    }

    fn example_str() -> Vec<&'static str> {
        ["unique", "foreign", "Unique", "FOREIGN"]
            .into_iter()
            .collect::<Vec<_>>()
    }

    #[test]
    fn valid_model_constraints_ser() {
        let actual_str = example_constraint()
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
    fn valid_model_constraints_de() {
        let actual_contraint = example_str()
            .iter()
            .map(|x| serde_yaml_ng::from_str::<ModelConstraint>(x).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual_contraint, example_constraint());
    }
}
