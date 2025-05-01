use std::sync::Arc;

use log::info;
use polars::prelude::LazyFrame;
use serde::{Deserialize, de};

use crate::util::error::{CpError, CpResult};

use super::{context::PipelineContext, results::PipelineResults};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMethodType {
    SyncLazy,
    SyncEager,
    AsyncLazy,
    AsyncEager,
}

impl<'de> Deserialize<'de> for RunMethodType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "sync_lazy" => Ok(RunMethodType::SyncLazy),
            "sync_eager" => Ok(RunMethodType::SyncEager),
            "async_lazy" => Ok(RunMethodType::AsyncLazy),
            "async_eager" => Ok(RunMethodType::AsyncEager),
            s => Err(de::Error::custom(format!("Unknown run_method: {}", s))),
        }
    }
}

pub struct PipelineRunner;

impl PipelineRunner {
    pub fn run_lazy<S>(ctx: Arc<dyn PipelineContext<LazyFrame, S>>) -> CpResult<PipelineResults<LazyFrame>> {
        let pipeline = match ctx.get_pipeline() {
            Some(p) => p,
            None => {
                return Err(CpError::PipelineError(
                    "No pipeline found in context",
                    "PipelineContext requires a pipeline before runner can execute".to_string(),
                ));
            }
        };
        info!("Initating pipeline: {}", &pipeline.label);
        for stage in &pipeline.stages {
            let task = ctx.get_task(&stage.task, &stage.args)?;
            task(ctx.clone())?;
        }
        ctx.clone().clone_results()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use polars::prelude::LazyFrame;

    use crate::{
        context::{
            logger::LoggerRegistry,
            model::ModelRegistry,
            task::{TaskDictionary, generate_lazy_task},
            transform::TransformRegistry,
        },
        pipeline::{
            common::{Pipeline, PipelineStage},
            context::{DefaultContext, PipelineContext},
            results::PipelineResults,
        },
        task::noop::NoopTask,
    };

    use super::PipelineRunner;

    fn noop_stage(name: &str) -> PipelineStage {
        PipelineStage::new(name, "noop", &serde_yaml_ng::Value::Null)
    }

    fn raw_context() -> DefaultContext<LazyFrame, ()> {
        DefaultContext::new(
            ModelRegistry::new(),
            TransformRegistry::new(),
            TaskDictionary::new(vec![("noop", generate_lazy_task::<NoopTask, ()>())]),
            (),
            LoggerRegistry::new(),
        )
    }

    fn create_context(pipeline: Pipeline) -> Arc<DefaultContext<LazyFrame, ()>> {
        let mut ctx = raw_context();
        ctx.set_pipeline(pipeline).unwrap();
        Arc::new(ctx)
    }

    #[test]
    fn valid_noop_n_stages() {
        let n_pipelines = (0..4)
            .map(|n| {
                (1..n)
                    .map(|i| noop_stage(format!("_noop_{}", i).as_str()))
                    .collect::<Vec<PipelineStage>>()
            })
            .map(|x| Pipeline::new("noop", &x))
            .collect::<Vec<_>>();
        n_pipelines.iter().for_each(|pipeline| {
            let ctx = create_context(pipeline.clone());
            let actual = PipelineRunner::run_lazy(ctx.clone()).unwrap();
            assert_eq!(actual, PipelineResults::<LazyFrame>::default());
        });
    }

    #[test]
    fn invalid_task_not_found() {
        let pipeline = Pipeline::new(
            "invalid",
            &[PipelineStage::new("not_found", "nooop", &serde_yaml_ng::Value::Null)],
        );
        let ctx = create_context(pipeline.clone());
        assert!(PipelineRunner::run_lazy(ctx.clone()).is_err());
    }

    #[test]
    fn invalid_no_pipeline() {
        let ctx = Arc::new(raw_context());
        assert!(PipelineRunner::run_lazy(ctx.clone()).is_err());
    }
}
