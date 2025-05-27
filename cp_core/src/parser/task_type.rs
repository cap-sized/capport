use serde::{Deserialize, Serialize, de};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TaskTypeEnum {
    Transform,
    Source,
    Sink,
    Request,
}

impl<'de> Deserialize<'de> for TaskTypeEnum {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?.to_lowercase();
        match s.as_str() {
            "transform" => Ok(TaskTypeEnum::Transform),
            "source" => Ok(TaskTypeEnum::Source),
            "sink" => Ok(TaskTypeEnum::Sink),
            "req" => Ok(TaskTypeEnum::Request),
            "request" => Ok(TaskTypeEnum::Request),
            // TODO: Handle parsing of list[xxx] types, more datetime regions
            s => Err(de::Error::custom(format!("Unknown task_type: {}", s))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TaskTypeEnum;

    fn example_task_type() -> Vec<TaskTypeEnum> {
        [
            TaskTypeEnum::Transform,
            TaskTypeEnum::Source,
            TaskTypeEnum::Sink,
            TaskTypeEnum::Request,
            TaskTypeEnum::Transform,
            TaskTypeEnum::Source,
            TaskTypeEnum::Sink,
            TaskTypeEnum::Request,
        ]
        .into_iter()
        .collect::<Vec<_>>()
    }

    fn example_str() -> Vec<&'static str> {
        [
            "transforM",
            "source",
            "Sink",
            "request",
            "transform",
            "SOURCE",
            "sink",
            "Request",
        ]
        .into_iter()
        .collect::<Vec<_>>()
    }

    #[test]
    fn valid_task_type_ser() {
        let actual_str = example_task_type()
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
    fn valid_task_type_de() {
        let actual_contraint = example_str()
            .iter()
            .map(|x| serde_yaml_ng::from_str::<TaskTypeEnum>(x).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual_contraint, example_task_type());
    }
}
