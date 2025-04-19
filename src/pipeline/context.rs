use std::{
    ops::DerefMut,
    process::exit,
    sync::{Arc, Mutex},
};

use polars::prelude::LazyFrame;
use yaml_rust2::Yaml;

use crate::{
    context::{model::ModelRegistry, pipeline::PipelineRegistry, task::TaskDictionary, transform::TransformRegistry},
    model::common::Model,
    transform::common::RootTransform,
    util::error::{CpError, CpResult},
};

use super::{
    common::{Pipeline, PipelineTask},
    results::PipelineResults,
};

pub trait PipelineContext<ResultType, ServiceDistributor> {
    // Results
    fn clone_results(&self) -> PipelineResults<ResultType>;

    fn clone_result(&self, key: &str) -> CpResult<LazyFrame>;

    fn get_results(&self) -> Arc<Mutex<PipelineResults<ResultType>>>;

    fn insert_result(&mut self, key: &str, result: ResultType) -> Option<ResultType>;

    // Immutables
    fn get_model(&self, key: &str) -> CpResult<Model>;

    fn get_task(&self, key: &str, args: &Yaml) -> CpResult<PipelineTask<ResultType, ServiceDistributor>>;

    fn get_transform(&self, key: &str) -> CpResult<&RootTransform>;

    fn svc(&self) -> Option<ServiceDistributor>;
}

pub struct DefaultContext<ResultType> {
    model_registry: ModelRegistry,
    transform_registry: TransformRegistry,
    task_dictionary: TaskDictionary<ResultType, ()>,
    results: Arc<Mutex<PipelineResults<ResultType>>>,
}

impl DefaultContext<LazyFrame> {
    pub fn new(
        model_registry: ModelRegistry,
        transform_registry: TransformRegistry,
        task_dictionary: TaskDictionary<LazyFrame, ()>,
    ) -> Self {
        DefaultContext {
            model_registry,
            transform_registry,
            task_dictionary,
            results: Arc::new(Mutex::new(PipelineResults::<LazyFrame>::new())),
        }
    }
}

impl PipelineContext<LazyFrame, ()> for DefaultContext<LazyFrame> {
    fn clone_results(&self) -> PipelineResults<LazyFrame> {
        self.results.as_ref().lock().unwrap().clone()
    }
    fn clone_result(&self, key: &str) -> CpResult<LazyFrame> {
        let results = self.results.as_ref().lock().unwrap();
        match results.get_unchecked(key) {
            Some(x) => Ok(x),
            None => Err(CpError::ComponentError(
                "No Result Found",
                format!(
                    "No result `{}` found, result must be one of the following: {:?}",
                    key, &self.results
                ),
            )),
        }
    }
    fn get_results(&self) -> Arc<Mutex<PipelineResults<LazyFrame>>> {
        self.results.clone()
    }
    fn insert_result(&mut self, key: &str, result: LazyFrame) -> Option<LazyFrame> {
        let mut binding = self.results.as_ref().lock();
        let results = binding.as_deref_mut().unwrap();
        results.insert(key, result)
    }
    fn get_model(&self, key: &str) -> CpResult<Model> {
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
    fn get_task(&self, key: &str, args: &Yaml) -> CpResult<PipelineTask<LazyFrame, ()>> {
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
    fn get_transform(&self, key: &str) -> CpResult<&RootTransform> {
        match self.transform_registry.get_transform(key) {
            Some(x) => Ok(x),
            None => Err(CpError::ComponentError(
                "No Transform Found",
                format!(
                    "No transform `{}` found, transform must be one of the following: {:?}",
                    key, &self.transform_registry
                ),
            )),
        }
    }

    fn svc(&self) -> Option<()> {
        None
    }
}

impl Default for DefaultContext<LazyFrame> {
    fn default() -> Self {
        Self::new(
            ModelRegistry::new(),
            TransformRegistry::new(),
            TaskDictionary::default(),
        )
    }
}
