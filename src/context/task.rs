use std::{collections::HashMap, fmt};

use polars::prelude::LazyFrame;
use yaml_rust2::Yaml;

use crate::{
    pipeline::common::{HasTask, PipelineTask},
    task::{
        loadstore::csv::{CsvModelLoadTask, CsvModelSaveTask},
        noop::NoopTask,
        transform::TransformTask,
    },
    util::error::CpResult,
};

pub type TaskGenerator<ResultType, SvcDistributor> = fn(&Yaml) -> CpResult<PipelineTask<ResultType, SvcDistributor>>;

pub struct TaskDictionary<ResultType, SvcDistributor> {
    pub tasks: HashMap<String, TaskGenerator<ResultType, SvcDistributor>>,
}

pub fn generate_lazy_task<T: HasTask, S>() -> TaskGenerator<LazyFrame, S> {
    |yaml| T::lazy_task::<S>(yaml)
}

impl Default for TaskDictionary<LazyFrame, ()> {
    fn default() -> Self {
        Self {
            tasks: HashMap::from([
                ("noop".to_string(), generate_lazy_task::<NoopTask, ()>()),
                ("load_csv".to_string(), generate_lazy_task::<CsvModelLoadTask, ()>()),
                ("save_csv".to_string(), generate_lazy_task::<CsvModelSaveTask, ()>()),
                ("transform".to_string(), generate_lazy_task::<TransformTask, ()>()),
            ]),
        }
    }
}

impl<R, S> TaskDictionary<R, S> {
    pub fn new(tasks: Vec<(&str, TaskGenerator<R, S>)>) -> Self {
        Self {
            tasks: tasks
                .iter()
                .cloned()
                .map(|(x, tg)| (x.to_owned(), tg))
                .collect::<HashMap<String, TaskGenerator<R, S>>>(),
        }
    }
}

impl<R, S> fmt::Display for TaskDictionary<R, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut vecstr = self.tasks.keys().map(|x| x.to_owned()).collect::<Vec<String>>();
        vecstr.sort();
        write!(f, "{:?}", vecstr)
    }
}

#[cfg(test)]
mod tests {
    use crate::task::noop::NoopTask;

    use super::{TaskDictionary, generate_lazy_task};

    #[test]
    fn print_task_dict() {
        {
            let tdict = TaskDictionary::new(vec![("noop", generate_lazy_task::<NoopTask, ()>())]);
            let printed_tdict = format!("{}", tdict);
            assert_eq!(printed_tdict, "[\"noop\"]".to_owned());
        }
        {
            let tdict = TaskDictionary::new(vec![
                ("noop", generate_lazy_task::<NoopTask, ()>()),
                ("abcd", generate_lazy_task::<NoopTask, ()>()),
                ("defg", generate_lazy_task::<NoopTask, ()>()),
                ("____", generate_lazy_task::<NoopTask, ()>()),
                ("_89_", generate_lazy_task::<NoopTask, ()>()),
            ]);
            let printed_tdict = format!("{}", tdict);
            assert_eq!(
                printed_tdict,
                "[\"_89_\", \"____\", \"abcd\", \"defg\", \"noop\"]".to_owned()
            );
        }
    }
}
