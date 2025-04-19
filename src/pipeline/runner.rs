use std::sync::Arc;

use polars::prelude::LazyFrame;

use crate::util::error::{CpError, CpResult};

use super::{
    common::Pipeline,
    context::{DefaultContext, PipelineContext},
    results::PipelineResults,
};

pub struct PipelineRunner;
impl PipelineRunner {
    pub fn run_lazy<S>(
        ctx: Arc<dyn PipelineContext<LazyFrame, S>>,
        pipeline: &Pipeline,
    ) -> CpResult<PipelineResults<LazyFrame>> {
        for stage in &pipeline.stages {
            let task = ctx.get_task(&stage.task_name, &stage.args_node)?;
            task(ctx.clone())?;
        }
        ctx.clone().clone_results()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use polars::prelude::LazyFrame;
    use yaml_rust2::Yaml;

    use crate::{
        context::{
            model::ModelRegistry,
            task::{TaskDictionary, generate_lazy_task},
            transform::TransformRegistry,
        },
        pipeline::{
            common::{Pipeline, PipelineStage},
            context::DefaultContext,
            results::PipelineResults,
        },
        task::noop::NoopTask,
    };

    use super::PipelineRunner;

    fn noop_stage(name: &str) -> PipelineStage {
        PipelineStage::new(name, "noop", &yaml_rust2::Yaml::Null)
    }

    fn create_context() -> Arc<DefaultContext<LazyFrame>> {
        Arc::new(DefaultContext::new(
            ModelRegistry::new(),
            TransformRegistry::new(),
            TaskDictionary::new(vec![("noop", generate_lazy_task::<NoopTask, ()>())]),
        ))
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
            let ctx = create_context();
            let actual = PipelineRunner::run_lazy(ctx.clone(), pipeline).unwrap();
            assert_eq!(actual, PipelineResults::<LazyFrame>::default());
        });
    }

    #[test]
    fn invalid_task_not_found() {
        let pipeline = Pipeline::new("invalid", &[PipelineStage::new("not_found", "nooop", &Yaml::Null)]);
        let ctx = create_context();
        assert!(PipelineRunner::run_lazy(ctx.clone(), &pipeline).is_err());
    }
}
