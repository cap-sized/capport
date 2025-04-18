use serde::{Deserialize, Serialize};

use crate::{
    pipeline::{
        common::{HasTask, PipelineOnceTask},
        context::Context,
        results::PipelineResults,
    },
    util::{
        common::yaml_to_str,
        error::{CpError, CpResult, SubResult},
    },
};

use super::common::{deserialize_arg_str, yaml_to_task_arg_str};

#[derive(Serialize, Deserialize)]
pub struct NoopTask;

pub fn run(ctx: &Context, task: &NoopTask) -> CpResult<()> {
    Ok(())
}

impl HasTask for NoopTask {
    fn task(args: &yaml_rust2::Yaml) -> CpResult<PipelineOnceTask> {
        let arg_str = yaml_to_task_arg_str(args, "NoopTask")?;
        let noop_task: NoopTask = deserialize_arg_str::<NoopTask>(&arg_str, "NoopTask")?;
        Ok(Box::new(move |ctx| run(ctx, &noop_task)))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        pipeline::{common::HasTask, context::Context, results::PipelineResults},
        util::common::yaml_from_str,
    };

    use super::NoopTask;

    #[test]
    fn valid_noop_behaviour() {
        let ctx = Context::default();
        let args = yaml_from_str("---").unwrap();
        let t = NoopTask::task(&args).unwrap();
        t(&ctx).unwrap();
        assert_eq!(ctx.clone_results(), PipelineResults::new());
    }

    #[test]
    fn invalid_noop_args() {
        let ctx = Context::default();
        let args = yaml_from_str(
            "
---
a: b
",
        )
        .unwrap();
        assert!(NoopTask::task(&args).is_err());
    }
}
