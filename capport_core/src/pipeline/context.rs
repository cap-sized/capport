use std::sync::{Arc, RwLock};

use polars::prelude::LazyFrame;

use crate::{
    context::{logger::LoggerRegistry, model::ModelRegistry, task::TaskDictionary, transform::TransformRegistry},
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
    fn clone_results(&self) -> CpResult<PipelineResults<ResultType>>;

    fn clone_result(&self, key: &str) -> CpResult<ResultType>;

    fn get_results(&self) -> Arc<RwLock<PipelineResults<ResultType>>>;

    fn insert_result(&self, key: &str, result: ResultType) -> CpResult<Option<ResultType>>;

    // Pipeline
    fn set_pipeline(&mut self, pipeline: Pipeline) -> CpResult<()>;

    fn get_pipeline(&self) -> Option<&Pipeline>;

    fn pop_pipeline(&mut self) -> Option<Pipeline>;

    // Immutables
    fn get_model(&self, key: &str) -> CpResult<Model>;

    fn get_task(
        &self,
        key: &str,
        args: &serde_yaml_ng::Value,
    ) -> CpResult<PipelineTask<ResultType, ServiceDistributor>>;

    fn get_transform(&self, key: &str) -> CpResult<&RootTransform>;

    fn svc(&self) -> &ServiceDistributor;

    fn mut_svc(&mut self) -> &mut ServiceDistributor;

    // Logger handlers
    fn init_log(&mut self, logger_name: &str, to_console: bool) -> CpResult<()>;

    fn close_log(&self);
}

pub struct DefaultContext<ResultType, ServiceDistributor> {
    model_registry: ModelRegistry,
    transform_registry: TransformRegistry,
    task_dictionary: TaskDictionary<ResultType, ServiceDistributor>,
    results: Arc<RwLock<PipelineResults<ResultType>>>,
    logger_registry: LoggerRegistry,
    service_distributor: ServiceDistributor,
    pipeline: Option<Pipeline>,
}

unsafe impl<ResultType, ServiceDistributor> Send for DefaultContext<ResultType, ServiceDistributor> {}
unsafe impl<ResultType, ServiceDistributor> Sync for DefaultContext<ResultType, ServiceDistributor> {}

impl<R, S> DefaultContext<R, S> {
    pub fn new(
        model_registry: ModelRegistry,
        transform_registry: TransformRegistry,
        task_dictionary: TaskDictionary<R, S>,
        service_distributor: S,
        logger_registry: LoggerRegistry,
    ) -> Self {
        DefaultContext {
            model_registry,
            transform_registry,
            task_dictionary,
            results: Arc::new(RwLock::new(PipelineResults::<R>::default())),
            service_distributor,
            logger_registry,
            pipeline: None,
        }
    }
}

impl<ResultType: Clone, ServiceDistributor> PipelineContext<ResultType, ServiceDistributor>
    for DefaultContext<ResultType, ServiceDistributor>
{
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
    fn get_task(
        &self,
        key: &str,
        args: &serde_yaml_ng::Value,
    ) -> CpResult<PipelineTask<ResultType, ServiceDistributor>> {
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

    fn svc(&self) -> &ServiceDistributor {
        &self.service_distributor
    }

    fn mut_svc(&mut self) -> &mut ServiceDistributor {
        &mut self.service_distributor
    }

    fn init_log(&mut self, logger_name: &str, to_console: bool) -> CpResult<()> {
        let pipeline_name = self.get_pipeline().map(|x| &x.label).cloned();
        let lr = &mut self.logger_registry;
        match pipeline_name {
            Some(pipeline) => lr.start_logger(logger_name, pipeline.as_str(), to_console),
            None => Err(CpError::PipelineError(
                "Pipeline not set yet",
                "No pipeline set yet before `init_log` called".to_owned(),
            )),
        }
    }

    fn close_log(&self) {
        self.logger_registry.show_output();
    }

    fn set_pipeline(&mut self, pipeline: Pipeline) -> CpResult<()> {
        if self.pipeline.is_some() {
            return Err(CpError::PipelineError(
                "Pipeline already running with context",
                format!(
                    "Remove pipeline {} before setting new pipeline",
                    self.pipeline.as_ref().unwrap().label
                ),
            ));
        }
        let _ = self.pipeline.insert(pipeline.to_owned());
        Ok(())
    }

    fn get_pipeline(&self) -> Option<&Pipeline> {
        self.pipeline.as_ref().to_owned()
    }

    fn pop_pipeline(&mut self) -> Option<Pipeline> {
        let last_pipeline = self.pipeline.clone();
        self.pipeline = None;
        last_pipeline
    }
}

impl<R, S> Drop for DefaultContext<R, S> {
    fn drop(&mut self) {
        self.logger_registry.show_output();
    }
}

impl Default for DefaultContext<LazyFrame, ()> {
    fn default() -> Self {
        Self::new(
            ModelRegistry::new(),
            TransformRegistry::new(),
            TaskDictionary::default(),
            (),
            LoggerRegistry::new(),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{logger::common::DEFAULT_CONSOLE_LOGGER_NAME, pipeline::common::Pipeline};

    use super::{DefaultContext, PipelineContext};

    #[test]
    fn invalid_init_log_before_pipeline() {
        let mut ctx = DefaultContext::default();
        assert!(ctx.init_log(DEFAULT_CONSOLE_LOGGER_NAME, true).is_err());
    }

    #[test]
    fn invalid_double_set_pipeline() {
        let mut ctx = DefaultContext::default();
        let p1 = Pipeline::new("my_first_pipeline", &[]);
        let p2 = Pipeline::new("my_second_pipeline", &[]);
        ctx.set_pipeline(p1.clone()).unwrap();
        ctx.set_pipeline(p2.clone()).unwrap_err();
    }

    #[test]
    fn valid_set_unset_pipeline() {
        let mut ctx = DefaultContext::default();
        let p1 = Pipeline::new("my_first_pipeline", &[]);
        ctx.set_pipeline(p1.clone()).unwrap();
        assert_eq!(ctx.get_pipeline().unwrap().label, "my_first_pipeline");
        let removed = ctx.pop_pipeline().unwrap();
        assert_eq!(&removed.label, "my_first_pipeline");
        assert_eq!(ctx.get_pipeline(), None);
    }
}
