use polars::prelude::LazyFrame;
use yaml_rust2::Yaml;

use crate::{
    context::{model::ModelRegistry, pipeline::PipelineRegistry, task::TaskDictionary, transform::TransformRegistry},
    model::common::Model,
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
    pub fn get_model(&self, key: &str) -> CpResult<Model> {
        match self.model_registry.get_model(key) {
            Some(x) => Ok(x),
            None => Err(CpError::ComponentError(
                "No Model Found",
                format!(
                    "No model found, model must be one of the following: {}",
                    &self.model_registry
                ),
            )),
        }
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
    pub fn set_result(&mut self, key: &str, lf: LazyFrame) -> Option<LazyFrame> {
        self.results.insert(key, lf)
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
