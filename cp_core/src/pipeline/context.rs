use async_channel::Receiver;
use async_trait::async_trait;
use polars::{frame::DataFrame, prelude::LazyFrame};

use crate::{
    context::model::ModelRegistry,
    frame::{
        common::{FrameUpdateInfo, PipelineFrame},
        polars::{
            PolarsAsyncBroadcastHandle, PolarsAsyncListenHandle, PolarsBroadcastHandle, PolarsListenHandle,
            PolarsPipelineFrame,
        },
    },
    model::common::{ModelConfig, ModelFields},
    util::error::{CpError, CpResult},
};

use super::{results::PipelineResults, signal::SignalState};

/// Trait for all PipelineContext methods, which expose listeners, broadcasters and a means to
/// extract and clone cached results.
#[async_trait]
pub trait PipelineContext<
    'a,
    FrameType,
    MaterializedType,
    ListenHandle: 'a,
    BroadcastHandle: 'a,
    AsyncListenHandle: 'a,
    AsyncBroadcastHandle: 'a,
>
{
    fn get_listener(&'a self, label: &str, handler: &str) -> CpResult<ListenHandle>;
    fn get_broadcast(&'a self, label: &str, handler: &str) -> CpResult<BroadcastHandle>;
    fn get_async_listener(&'a self, label: &str, handler: &str) -> CpResult<AsyncListenHandle>;
    fn get_async_broadcast(&'a self, label: &str, handler: &str) -> CpResult<AsyncBroadcastHandle>;
    fn extract_clone_result(&self, label: &str) -> CpResult<MaterializedType>;
    fn extract_result(&self, label: &str) -> CpResult<FrameType>;
    fn insert_result(&self, label: &str, frame: FrameType) -> CpResult<()>;

    /// Model Registry
    fn get_model(&self, model_name: &str) -> CpResult<ModelConfig>;
    fn get_substituted_model_fields(&self, model_name: &str, context: &serde_yaml_ng::Mapping)
    -> CpResult<ModelFields>;

    /// The set of context signalling tools are meant to be used in async mode only.
    /// The signalling channels are unusable without calling `with_signal()` previously.
    fn signal_propagator(&self) -> Receiver<FrameUpdateInfo>;
    async fn signal_replace(&self) -> CpResult<()>;
    async fn signal_terminate(&self) -> CpResult<()>;
}

/// The pipeline context contains the universe of results.
pub struct DefaultPipelineContext {
    results: PipelineResults<PolarsPipelineFrame>,
    model_registry: ModelRegistry,
    signal_state: Option<SignalState>,
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
    pub fn from(results: PipelineResults<PolarsPipelineFrame>, model_registry: ModelRegistry) -> Self {
        Self {
            results,
            model_registry,
            signal_state: None,
        }
    }
    pub fn with_model_registry(mut self, model_registry: ModelRegistry) -> Self {
        self.model_registry = model_registry;
        self
    }
    pub fn with_signal(mut self) -> Self {
        if self.signal_state.is_none() {
            let _ = self.signal_state.insert(SignalState::new());
        }
        self
    }
    pub fn new() -> Self {
        Self::from(PipelineResults::<PolarsPipelineFrame>::new(), ModelRegistry::new())
    }
    pub fn with_results(labels: &[&str], bufsize: usize) -> Self {
        let mut results = PipelineResults::<PolarsPipelineFrame>::new();
        labels.iter().for_each(|label| {
            results.insert(label.to_owned(), bufsize);
        });
        Self::from(results, ModelRegistry::new())
    }
    pub fn signal(&self) -> &SignalState {
        self.signal_state
            .as_ref()
            .expect("No signal state initialized, try calling `ctx.with_signal()`")
    }
}

#[async_trait]
impl<'a>
    PipelineContext<
        'a,
        LazyFrame,
        DataFrame,
        PolarsListenHandle<'a>,
        PolarsBroadcastHandle<'a>,
        PolarsAsyncListenHandle<'a>,
        PolarsAsyncBroadcastHandle<'a>,
    > for DefaultPipelineContext
{
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
    fn extract_result(&self, label: &str) -> CpResult<LazyFrame> {
        match self.results.get(label) {
            Some(x) => x.extract(),
            None => Err(CpError::PipelineError(
                "Result not found",
                format!(
                    "Cannot extract result `{}`, which was not created before execution",
                    label
                ),
            )),
        }
    }
    fn insert_result(&self, label: &str, frame: LazyFrame) -> CpResult<()> {
        match self.results.get(label) {
            Some(x) => x.insert(frame),
            None => Err(CpError::PipelineError(
                "Result not found",
                format!(
                    "Cannot extract result `{}`, which was not created before execution",
                    label
                ),
            )),
        }
    }
    fn signal_propagator(&self) -> Receiver<FrameUpdateInfo> {
        self.signal().sig_recver.clone()
    }
    async fn signal_replace(&self) -> CpResult<()> {
        self.signal().send_replace_signal().await
    }
    async fn signal_terminate(&self) -> CpResult<()> {
        self.signal().send_terminate_signal().await
    }
    fn get_model(&self, model_name: &str) -> CpResult<ModelConfig> {
        match self.model_registry.get_model(model_name) {
            Some(x) => Ok(x),
            None => Err(CpError::ConfigError(
                "Missing config for model",
                format!("Config required: {}", model_name),
            )),
        }
    }
    fn get_substituted_model_fields(
        &self,
        model_name: &str,
        context: &serde_yaml_ng::Mapping,
    ) -> CpResult<ModelFields> {
        self.model_registry.get_substituted_model_fields(model_name, context)
    }
}
