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
