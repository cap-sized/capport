use polars::prelude::LazyFrame;
use yaml_rust2::Yaml;

use crate::{
    context::{model::ModelRegistry, pipeline::PipelineRegistry, task::TaskDictionary, transform::TransformRegistry},
    model::common::Model,
    transform::common::RootTransform,
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
    pub fn clone_result(&self, key: &str) -> CpResult<LazyFrame> {
        match self.results.get_unchecked(key) {
            Some(x) => Ok(x),
            None => Err(CpError::ComponentError(
                "No Result Found",
                format!(
                    "No result `{}` found, model must be one of the following: {}",
                    key, &self.model_registry
                ),
            )),
        }
    }
    pub fn get_ro_results(&self) -> &PipelineResults {
        &self.results
    }
    pub fn insert_result(&mut self, key: &str, result: LazyFrame) -> CpResult<Option<LazyFrame>> {
        Ok(self.results.insert(key, result))
    }
    pub fn get_model(&self, key: &str) -> CpResult<Model> {
        match self.model_registry.get_model(key) {
            Some(x) => Ok(x),
            None => Err(CpError::ComponentError(
                "No Model Found",
                format!(
                    "No model `{}` found, model must be one of the following: {}",
                    key, &self.model_registry
                ),
            )),
        }
    }
    pub fn get_task(&self, key: &str, args: &Yaml) -> CpResult<PipelineOnceTask> {
        let taskgen = match self.task_dictionary.tasks.get(key) {
            Some(x) => x,
            None => {
                return Err(CpError::ComponentError(
                    "No Task Found",
                    format!(
                        "No task `{}` found, task must be one of the labels: [{}]",
                        key, &self.task_dictionary
                    ),
                ));
            }
        };
        taskgen(args)
    }
    pub fn get_transform(&self, key: &str) -> CpResult<&RootTransform> {
        match self.transform_registry.get_transform(key) {
            Some(x) => Ok(x),
            None => {
                Err(CpError::ComponentError(
                    "No Transform Found",
                    format!(
                        "No transform `{}` found, transform must be one of the following: {:?}",
                        key, &self.transform_registry
                    ),
                ))
            }
        }
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
