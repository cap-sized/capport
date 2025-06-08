use serde::{Deserialize, Serialize, de};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum MergeTypeEnum {
    Insert,
    Replace,
    MakeNext,
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
            "next" => Ok(MergeTypeEnum::MakeNext),
            "make next" => Ok(MergeTypeEnum::MakeNext),
            "make_next" => Ok(MergeTypeEnum::MakeNext),
            "makenext" => Ok(MergeTypeEnum::MakeNext),
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
            MergeTypeEnum::MakeNext,
            MergeTypeEnum::MakeNext,
            MergeTypeEnum::MakeNext,
            MergeTypeEnum::MakeNext,
        ]
        .into_iter()
        .collect::<Vec<_>>()
    }

    fn example_str() -> Vec<&'static str> {
        [
            "insert",
            "replace",
            "Insert",
            "REPLACE",
            "next",
            "make_next",
            "Make Next",
            "MakeNext",
        ]
        .into_iter()
        .collect::<Vec<_>>()
    }

    #[test]
    fn valid_merge_type_ser() {
        let actual_str = [MergeTypeEnum::Insert, MergeTypeEnum::Replace, MergeTypeEnum::MakeNext]
            .iter()
            .map(|f| serde_yaml_ng::to_string(f).map(|x| x.trim().to_owned()).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual_str, vec!["Insert", "Replace", "MakeNext"]);
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
