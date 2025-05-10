use polars::prelude::LazyFrame;

use crate::{frame::{common::PipelineFrame, polars::{PolarsBroadcastHandle, PolarsListenHandle, PolarsPipelineFrame}}, util::error::{CpError, CpResult}};

use super::results::PipelineResults;

pub trait PipelineContext {
    fn get_listener(&self, label: &str, handler: &str) -> CpResult<PolarsListenHandle>;
    fn get_broadcast(&self, label: &str, handler: &str) -> CpResult<PolarsBroadcastHandle>;
    fn extract_clone_result(&self, label: &str) -> CpResult<LazyFrame>;
}

/// The pipeline context contains the universe of results.
pub struct DefaultPipelineContext {
    results: PipelineResults<PolarsPipelineFrame>
}

unsafe impl Send for DefaultPipelineContext {}
unsafe impl Sync for DefaultPipelineContext {}

impl DefaultPipelineContext {
    pub fn from(results: PipelineResults<PolarsPipelineFrame>) -> Self {
        Self { results }
    }
    pub fn new() -> Self {
        Self::from(PipelineResults::<PolarsPipelineFrame>::new())
    }

}

impl PipelineContext for DefaultPipelineContext {
    fn get_listener(&self, label: &str, handler: &str) -> CpResult<PolarsListenHandle> {
        match self.results.get(label) {
            Some(x) => Ok(x.get_listen_handle(handler)),
            None => Err(CpError::PipelineError("Result not found", format!("`{}` requested for the listener to result `{}`, which was not created before execution", handler, label)))
        }
    }
    fn get_broadcast(&self, label: &str, handler: &str) -> CpResult<PolarsBroadcastHandle> {
        match self.results.get(label) {
            Some(x) => Ok(x.get_broadcast_handle(handler)),
            None => Err(CpError::PipelineError("Result not found", format!("`{}` requested for the broadcaster for result `{}`, which was not created before execution", handler, label)))
        }
    }
    fn extract_clone_result(&self, label: &str) -> CpResult<LazyFrame> {
        match self.results.get(label) {
            Some(x) => x.extract_clone(),
            None => Err(CpError::PipelineError("Result not found", format!("Cannot clone result `{}`, which was not created before execution", label)))
        }
    }
}

