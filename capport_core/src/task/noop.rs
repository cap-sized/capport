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

#[derive(Serialize, Deserialize)]
pub struct NoopTask;

pub fn run<R, S>(_ctx: Arc<dyn PipelineContext<R, S>>, _task: &NoopTask) -> CpResult<()> {
    Ok(())
}

impl HasTask for NoopTask {
    fn lazy_task<S>(args: &serde_yaml_ng::Value) -> CpResult<PipelineTask<LazyFrame, S>> {
        let noop_task: NoopTask = serde_yaml_ng::from_value::<NoopTask>(args.to_owned())?;
        Ok(Box::new(move |ctx| run(ctx, &noop_task)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use polars::prelude::LazyFrame;

    use crate::{
        pipeline::{
            common::HasTask,
            context::{DefaultContext, PipelineContext},
            results::PipelineResults,
        },
        util::common::yaml_from_str,
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
        assert!(NoopTask::lazy_task::<()>(&args).is_err());
    }
}
