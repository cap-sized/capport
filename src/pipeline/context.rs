use std::sync::{Arc, RwLock};

use polars::prelude::LazyFrame;
use yaml_rust2::Yaml;

use crate::{
    context::{model::ModelRegistry, task::TaskDictionary, transform::TransformRegistry},
    model::common::Model,
    transform::common::RootTransform,
    util::error::{CpError, CpResult},
};

use super::{common::PipelineTask, results::PipelineResults};

pub trait PipelineContext<ResultType, ServiceDistributor> {
    // Results
    fn clone_results(&self) -> CpResult<PipelineResults<ResultType>>;

    fn clone_result(&self, key: &str) -> CpResult<ResultType>;

    fn get_results(&self) -> Arc<RwLock<PipelineResults<ResultType>>>;

    fn insert_result(&self, key: &str, result: ResultType) -> CpResult<Option<ResultType>>;

    // Immutables
    fn get_model(&self, key: &str) -> CpResult<Model>;

    fn get_task(&self, key: &str, args: &Yaml) -> CpResult<PipelineTask<ResultType, ServiceDistributor>>;

    fn get_transform(&self, key: &str) -> CpResult<&RootTransform>;

    fn svc(&self) -> Option<ServiceDistributor>;
}

pub struct DefaultContext<ResultType, S> {
    model_registry: ModelRegistry,
    transform_registry: TransformRegistry,
    task_dictionary: TaskDictionary<ResultType, S>,
    results: Arc<RwLock<PipelineResults<ResultType>>>,
}

unsafe impl<ResultType, ServiceDistributor> Send for DefaultContext<ResultType, ServiceDistributor> {}
unsafe impl<ResultType, ServiceDistributor> Sync for DefaultContext<ResultType, ServiceDistributor> {}

impl<R, S> DefaultContext<R, S> {
    pub fn new(
        model_registry: ModelRegistry,
        transform_registry: TransformRegistry,
        task_dictionary: TaskDictionary<R, S>,
    ) -> Self {
        DefaultContext {
            model_registry,
            transform_registry,
            task_dictionary,
            results: Arc::new(RwLock::new(PipelineResults::<R>::default())),
        }
    }
}

impl<ResultType: Clone, S> PipelineContext<ResultType, S> for DefaultContext<ResultType, S> {
    fn clone_results(&self) -> CpResult<PipelineResults<ResultType>> {
        Ok(self.results.as_ref().read()?.clone())
    }
    fn clone_result(&self, key: &str) -> CpResult<ResultType> {
        let binding = self.results.clone();
        let results = binding.read()?;
        match results.get_unchecked(key) {
            Some(x) => Ok(x),
            None => Err(CpError::ComponentError(
                "No Result Found",
                format!(
                    "No result `{}` found, result must be one of the following: {:?}",
                    key,
                    self.results.read().unwrap().keys()
                ),
            )),
        }
    }
    fn get_results(&self) -> Arc<RwLock<PipelineResults<ResultType>>> {
        self.results.clone()
    }
    fn insert_result(&self, key: &str, result: ResultType) -> CpResult<Option<ResultType>> {
        let binding = self.results.clone();
        let mut res = binding.write()?;
        Ok(res.insert(key, result))
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
    fn get_task(&self, key: &str, args: &Yaml) -> CpResult<PipelineTask<ResultType, S>> {
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

    fn svc(&self) -> Option<S> {
        None
    }
}

impl Default for DefaultContext<LazyFrame, ()> {
    fn default() -> Self {
        Self::new(
            ModelRegistry::new(),
            TransformRegistry::new(),
            TaskDictionary::default(),
        )
    }
}
