use std::{collections::HashMap, fmt};

use yaml_rust2::Yaml;

use crate::{
    pipeline::common::{HasTask, PipelineOnceTask},
    task::noop::NoopTask,
    util::error::{CpError, CpResult},
};

pub type TaskGenerator = fn(&Yaml) -> CpResult<PipelineOnceTask>;

pub struct TaskDictionary {
    pub tasks: HashMap<String, TaskGenerator>,
}

pub fn generate_task<T: HasTask>() -> TaskGenerator {
    |yaml| T::task(&yaml)
}

impl Default for TaskDictionary {
    fn default() -> Self {
        Self {
            tasks: HashMap::from([("noop".to_string(), generate_task::<NoopTask>())]),
        }
    }
}

impl TaskDictionary {
    pub fn new(tasks: Vec<(&str, TaskGenerator)>) -> Self {
        Self {
            tasks: tasks
                .iter()
                .cloned()
                .map(|(x, tg)| (x.to_owned(), tg))
                .collect::<HashMap<String, TaskGenerator>>(),
        }
    }
}

impl fmt::Display for TaskDictionary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{:?}]",
            self.tasks
                .iter()
                .map(|(x, _)| x.to_owned())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}
