use std::sync::Arc;

use crate::{pipeline::context::DefaultPipelineContext, util::error::CpResult};

pub trait Stage {
    /// Executes the default pipeline context.
    /// This isn't a trait, so reimplementing DefaultPipelineContext and all the tasks
    /// will be necessary if there is a change in impl of lazyframe etc.
    fn exec(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()>;

    /// Runs a polling loop while not killed. Since there are only 3 types of stages
    /// we will allow the async_fn_in_trait. Returns the number of iterations made.
    #[allow(async_fn_in_trait)]
    async fn aloop(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<u64>;
}
