use serde::{Deserialize, Serialize};

use crate::{
    pipeline::{
        common::{HasTask, PipelineOnceTask},
        context::Context,
        results::PipelineResults,
    },
    util::error::{CpResult, SubResult},
};
pub struct NoopTask;

fn run(ctx: &Context, args_yaml_str: &str) -> CpResult<()> {
    println!("args: {}", args_yaml_str);
    Ok(())
}

impl Default for NoopTask {
    fn default() -> Self {
        NoopTask
    }
}

impl HasTask for NoopTask {
    fn task(&self) -> SubResult<PipelineOnceTask> {
        Ok(run)
    }
}
