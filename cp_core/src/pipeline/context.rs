use polars::frame::DataFrame;

use crate::{
    frame::{
        common::PipelineFrame,
        polars::{PolarsAsyncBroadcastHandle, PolarsAsyncListenHandle, PolarsBroadcastHandle, PolarsListenHandle, PolarsPipelineFrame},
    },
    util::error::{CpError, CpResult},
};

use super::results::PipelineResults;

/// Trait for all PipelineContext methods, which expose listeners, broadcasters and a means to
/// extract and clone cached results.
pub trait PipelineContext<'a, MaterializedType, ListenHandle: 'a, BroadcastHandle: 'a, AsyncListenHandle: 'a, AsyncBroadcastHandle: 'a> {
    fn get_listener(&'a self, label: &str, handler: &str) -> CpResult<ListenHandle>;
    fn get_broadcast(&'a self, label: &str, handler: &str) -> CpResult<BroadcastHandle>;
    fn get_async_listener(&'a self, label: &str, handler: &str) -> CpResult<AsyncListenHandle>;
    fn get_async_broadcast(&'a self, label: &str, handler: &str) -> CpResult<AsyncBroadcastHandle>;
    fn extract_clone_result(&self, label: &str) -> CpResult<MaterializedType>;
}

/// The pipeline context contains the universe of results.
pub struct DefaultPipelineContext {
    results: PipelineResults<PolarsPipelineFrame>,
}

/// We NEVER modify the actual entries in PipelineResults or any other registries
/// after initialization. Hence the underlying HashMap is safe to access in parallel.
unsafe impl Send for DefaultPipelineContext {}
unsafe impl Sync for DefaultPipelineContext {}

impl Default for DefaultPipelineContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Implements the PipelineContext for Polars suite of PipelineFrame tools
impl DefaultPipelineContext {
    pub fn from(results: PipelineResults<PolarsPipelineFrame>) -> Self {
        Self { results }
    }
    pub fn new() -> Self {
        Self::from(PipelineResults::<PolarsPipelineFrame>::new())
    }
    pub fn with_results(labels: &[&str], bufsize: usize) -> Self {
        let mut results = PipelineResults::<PolarsPipelineFrame>::new();
        labels.iter().for_each(|label| {
            results.insert(label.to_owned(), bufsize);
        });
        Self::from(results)
    }
}

impl<'a> PipelineContext<'a, DataFrame, PolarsListenHandle<'a>, PolarsBroadcastHandle<'a>, PolarsAsyncListenHandle<'a>, PolarsAsyncBroadcastHandle<'a>> for DefaultPipelineContext {
    fn get_listener(&'a self, label: &str, handler: &str) -> CpResult<PolarsListenHandle<'a>> {
        log::debug!("Initialized frame listener handle for {}", handler);
        match self.results.get(label) {
            Some(x) => Ok(x.get_listen_handle(handler)),
            None => Err(CpError::PipelineError(
                "Result not found",
                format!(
                    "`{}` requested for the listener to result `{}`, which was not created before execution",
                    handler, label
                ),
            )),
        }
    }
    fn get_broadcast(&'a self, label: &str, handler: &str) -> CpResult<PolarsBroadcastHandle<'a>> {
        log::debug!("Initialized frame broadcast handle for {}", handler);
        match self.results.get(label) {
            Some(x) => Ok(x.get_broadcast_handle(handler)),
            None => Err(CpError::PipelineError(
                "Result not found",
                format!(
                    "`{}` requested for the broadcaster for result `{}`, which was not created before execution",
                    handler, label
                ),
            )),
        }
    }
    fn get_async_listener(&'a self, label: &str, handler: &str) -> CpResult<PolarsAsyncListenHandle<'a>> {
        log::debug!("Initialized ASYNC frame listener handle for {}", handler);
        match self.results.get(label) {
            Some(x) => Ok(x.get_async_listen_handle(handler)),
            None => Err(CpError::PipelineError(
                "Result not found",
                format!(
                    "`{}` requested for the broadcaster for result `{}`, which was not created before execution",
                    handler, label
                ),
            )),
        }
    }
    fn get_async_broadcast(&'a self, label: &str, handler: &str) -> CpResult<PolarsAsyncBroadcastHandle<'a>> {
        log::debug!("Initialized ASYNC frame broadcast handle for {}", handler);
        match self.results.get(label) {
            Some(x) => Ok(x.get_async_broadcast_handle(handler)),
            None => Err(CpError::PipelineError(
                "Result not found",
                format!(
                    "`{}` requested for the broadcaster for result `{}`, which was not created before execution",
                    handler, label
                ),
            )),
        }
    }
    fn extract_clone_result(&self, label: &str) -> CpResult<DataFrame> {
        match self.results.get(label) {
            Some(x) => x.extract_clone(),
            None => Err(CpError::PipelineError(
                "Result not found",
                format!(
                    "Cannot clone result `{}`, which was not created before execution",
                    label
                ),
            )),
        }
    }
}
