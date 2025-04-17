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
    fn valid_noop_0to2_stages() {
        let pipeline = Pipeline::new(
            "noop",
            &[noop_stage("_noop_1"), noop_stage("_noop_2"), noop_stage("_noop_3")],
        );
        let ctx = Context::new(ModelRegistry::new(), TransformRegistry::new());
        let actual = PipelineRunner::run_once(ctx, &pipeline).unwrap();
        assert_eq!(actual, PipelineResults::new());
    }
}
