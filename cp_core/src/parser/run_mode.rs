use serde::{Deserialize, Serialize, de};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum RunModeEnum {
    Debug,
    Once,
    Loop,
}

impl<'de> Deserialize<'de> for RunModeEnum {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?.to_lowercase();
        match s.as_str() {
            "debug" => Ok(RunModeEnum::Debug),
            "once" => Ok(RunModeEnum::Once),
            "loop" => Ok(RunModeEnum::Loop),
            // TODO: Handle parsing of list[xxx] types, more datetime regions
            s => Err(de::Error::custom(format!("Unknown run_mode: {}", s))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RunModeEnum;

    fn example_run_mode() -> Vec<RunModeEnum> {
        [
            RunModeEnum::Debug,
            RunModeEnum::Once,
            RunModeEnum::Loop,
            RunModeEnum::Debug,
            RunModeEnum::Once,
            RunModeEnum::Loop,
        ]
        .into_iter()
        .collect::<Vec<_>>()
    }

    fn example_str() -> Vec<&'static str> {
        ["debug", "once", "loop", "dEbug", "Once", "LOOP"]
            .into_iter()
            .collect::<Vec<_>>()
    }

    #[test]
    fn valid_run_mode_ser() {
        let actual_str = example_run_mode()
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
    fn valid_run_mode_de() {
        let actual_contraint = example_str()
            .iter()
            .map(|x| serde_yaml_ng::from_str::<RunModeEnum>(x).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual_contraint, example_run_mode());
    }
}
