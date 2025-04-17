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

impl HasTask for NoopTask {
    fn task(&self) -> SubResult<PipelineOnceTask> {
        Ok(run)
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{common::HasTask, context::Context, results::PipelineResults};

    use super::NoopTask;

    #[test]
    fn valid_noop_behaviour() {
        let ctx = Context::default();
        let t = NoopTask.task().unwrap();
        t(&ctx, "test").unwrap();
        assert_eq!(ctx.clone_results(), PipelineResults::new());
    }
}
