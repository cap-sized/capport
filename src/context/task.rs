use std::{collections::HashMap, fmt};

use yaml_rust2::Yaml;

use crate::{
    pipeline::common::{HasTask, PipelineOnceTask},
    task::{
        loadstore::csv::{CsvModelLoadTask, CsvModelSaveTask},
        noop::NoopTask,
    },
    util::error::{CpError, CpResult},
};

pub type TaskGenerator = fn(&Yaml) -> CpResult<PipelineOnceTask>;

pub struct TaskDictionary {
    pub tasks: HashMap<String, TaskGenerator>,
}

pub fn generate_task<T: HasTask>() -> TaskGenerator {
    |yaml| T::task(yaml)
}

impl Default for TaskDictionary {
    fn default() -> Self {
        Self {
            tasks: HashMap::from([
                ("noop".to_string(), generate_task::<NoopTask>()),
                ("load_csv".to_string(), generate_task::<CsvModelLoadTask>()),
                ("save_csv".to_string(), generate_task::<CsvModelSaveTask>()),
            ]),
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
        let mut vecstr = self.tasks.keys().map(|x| x.to_owned()).collect::<Vec<String>>();
        vecstr.sort();
        write!(f, "{:?}", vecstr)
    }
}

#[cfg(test)]
mod tests {
    use crate::task::noop::NoopTask;

    use super::{TaskDictionary, generate_task};

    #[test]
    fn print_task_dict() {
        {
            let tdict = TaskDictionary::new(vec![("noop", generate_task::<NoopTask>())]);
            let printed_tdict = format!("{}", tdict);
            assert_eq!(printed_tdict, "[\"noop\"]".to_owned());
        }
        {
            let tdict = TaskDictionary::new(vec![
                ("noop", generate_task::<NoopTask>()),
                ("abcd", generate_task::<NoopTask>()),
                ("defg", generate_task::<NoopTask>()),
                ("____", generate_task::<NoopTask>()),
                ("_89_", generate_task::<NoopTask>()),
            ]);
            let printed_tdict = format!("{}", tdict);
            assert_eq!(
                printed_tdict,
                "[\"_89_\", \"____\", \"abcd\", \"defg\", \"noop\"]".to_owned()
            );
        }
    }
}
