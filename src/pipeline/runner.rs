use crate::util::error::{CpError, CpResult};

use super::{common::Pipeline, context::Context, results::PipelineResults};

pub struct PipelineRunner;
impl PipelineRunner {
    pub fn run_once(ctx: Context, pipeline: &Pipeline) -> CpResult<PipelineResults> {
        for stage in &pipeline.stages {
            (stage.task)(&ctx, &stage.args_yaml_str)?;
        }
        Ok(ctx.clone_results())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        context::{model::ModelRegistry, transform::TransformRegistry},
        pipeline::{
            common::{Pipeline, PipelineStage},
            context::Context,
            results::PipelineResults,
        },
    };

    use super::PipelineRunner;

    fn noop_stage(name: &str) -> PipelineStage {
        PipelineStage::new(
            name,
            |x, y| {
                println!("{}", y);
                Ok(())
            },
            "test",
        )
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
            let ctx = Context::default();
            let actual = PipelineRunner::run_once(ctx, pipeline).unwrap();
            assert_eq!(actual, PipelineResults::new());
        });
    }
}
