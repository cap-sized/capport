use serde::{Deserialize, Serialize};

use crate::{
    pipeline::{
        common::{PipelineTask, RunTask},
        context::Context,
        results::PipelineResults,
    },
    util::error::{CpResult, SubResult},
};
pub struct NoopTask;

fn run(ctx: Box<Context>) -> CpResult<PipelineResults> {
    Ok(ctx.clone_results())
}

impl Default for NoopTask {
    fn default() -> Self {
        NoopTask
    }
}

impl RunTask for NoopTask {
    fn task(&self) -> SubResult<PipelineTask> {
        Ok(run)
    }
}
