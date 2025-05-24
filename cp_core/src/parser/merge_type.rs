use serde::{Deserialize, Serialize, de};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum MergeTypeEnum {
    Insert,
    Replace,
}

impl<'de> Deserialize<'de> for MergeTypeEnum {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?.to_lowercase();
        match s.as_str() {
            "insert" => Ok(MergeTypeEnum::Insert),
            "replace" => Ok(MergeTypeEnum::Replace),
            // TODO: Handle parsing of list[xxx] types, more datetime regions
            s => Err(de::Error::custom(format!("Unknown sink merge_type: {}", s))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MergeTypeEnum;

    fn example_merge() -> Vec<MergeTypeEnum> {
        [
            MergeTypeEnum::Insert,
            MergeTypeEnum::Replace,
            MergeTypeEnum::Insert,
            MergeTypeEnum::Replace,
        ]
        .into_iter()
        .collect::<Vec<_>>()
    }

    fn example_str() -> Vec<&'static str> {
        ["insert", "replace", "Insert", "REPLACE"]
            .into_iter()
            .collect::<Vec<_>>()
    }

    #[test]
    fn valid_merge_type_ser() {
        let actual_str = example_merge()
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
    fn valid_merge_type_de() {
        let actual_contraint = example_str()
            .iter()
            .map(|x| serde_yaml_ng::from_str::<MergeTypeEnum>(x).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual_contraint, example_merge());
    }
}
