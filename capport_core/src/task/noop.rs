use polars::prelude::*;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    pipeline::{
        common::{HasTask, PipelineTask},
        context::PipelineContext,
    },
    util::error::CpResult,
};

use super::common::{deserialize_arg_str, yaml_to_task_arg_str};

#[derive(Serialize, Deserialize)]
pub struct NoopTask;

pub fn run<R, S>(_ctx: Arc<dyn PipelineContext<R, S>>, _task: &NoopTask) -> CpResult<()> {
    Ok(())
}

impl<S> HasTask<S> for NoopTask {
    fn lazy_task(args: &yaml_rust2::Yaml) -> CpResult<PipelineTask<LazyFrame, S>> {
        let arg_str = yaml_to_task_arg_str(args, "NoopTask")?;
        let noop_task: NoopTask = deserialize_arg_str::<NoopTask>(&arg_str, "NoopTask")?;
        Ok(Box::new(move |ctx| run(ctx, &noop_task)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use polars::prelude::LazyFrame;

    use crate::{
        pipeline::{
            common::{HasTask, PipelineTask},
            context::{DefaultContext, PipelineContext},
            results::PipelineResults,
        },
        util::{common::yaml_from_str, error::CpResult},
    };

    use super::NoopTask;

    #[test]
    fn valid_noop_behaviour() {
        let ctx = Arc::new(DefaultContext::default());
        let args = yaml_from_str("---").unwrap();
        let t = NoopTask::lazy_task(&args).unwrap();
        t(ctx.clone()).unwrap();
        assert_eq!(ctx.clone_results().unwrap(), PipelineResults::<LazyFrame>::default());
    }

    #[test]
    fn invalid_noop_args() {
        let args = yaml_from_str(
            "
---
a: b
",
        )
        .unwrap();
        let task: CpResult<PipelineTask<LazyFrame, ()>> = NoopTask::lazy_task(&args);
        assert!(task.is_err());
    }
}
