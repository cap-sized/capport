use yaml_rust2::Yaml;

use crate::{
    context::{model::ModelRegistry, pipeline::PipelineRegistry, task::TaskDictionary, transform::TransformRegistry},
    util::error::{CpError, CpResult},
};

use super::{
    common::{Pipeline, PipelineOnceTask},
    results::PipelineResults,
};

pub struct Context {
    model_registry: ModelRegistry,
    transform_registry: TransformRegistry,
    task_dictionary: TaskDictionary,
    results: PipelineResults,
}

impl Context {
    pub fn new(
        model_registry: ModelRegistry,
        transform_registry: TransformRegistry,
        task_dictionary: TaskDictionary,
    ) -> Self {
        Context {
            model_registry,
            transform_registry,
            task_dictionary,
            results: PipelineResults::new(),
        }
    }
    pub fn clone_results(&self) -> PipelineResults {
        self.results.clone()
    }
    pub fn get_task(&self, key: &str, args: &Yaml) -> CpResult<PipelineOnceTask> {
        let taskgen = match self.task_dictionary.tasks.get(key) {
            Some(x) => x,
            None => {
                return Err(CpError::PipelineError(
                    "No Task Found",
                    format!(
                        "No task found, task must be one of the labels: [{}]",
                        &self.task_dictionary
                    ),
                ));
            }
        };
        taskgen(args)
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new(
            ModelRegistry::new(),
            TransformRegistry::new(),
            TaskDictionary::default(),
        )
    }
}
