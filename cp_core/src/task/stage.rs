use std::sync::Arc;

use crate::{pipeline::context::DefaultPipelineContext, util::error::CpResult};

pub trait Stage {
    /// Executes the default pipeline context.
    /// This isn't a trait, so reimplementing DefaultPipelineContext and all the tasks
    /// will be necessary if there is a change in impl of lazyframe etc.
    fn exec(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()>;
}
