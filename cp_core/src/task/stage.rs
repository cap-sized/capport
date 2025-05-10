use std::sync::Arc;

use crate::{pipeline::context::PipelineContext, util::error::CpResult};

pub trait Stage {
    fn exec(&self, ctx: Arc<dyn PipelineContext>) -> CpResult<()>;
}
