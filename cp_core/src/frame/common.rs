use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};

use crate::util::error::CpResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameUpdateType {
    Replace,
    Kill,
}

/// Message template sent by broadcasters
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameUpdateInfo {
    pub source: String,
    pub timestamp: DateTime<Utc>,
    pub msg_type: FrameUpdateType,
}

/// Fully populated message
pub struct FrameUpdate<FrameType> {
    pub info: FrameUpdateInfo,
    pub frame: Arc<RwLock<FrameType>>,
}

impl FrameUpdateInfo {
    pub fn new(source: &str) -> Self {
        Self {
            source: source.to_owned(),
            timestamp: Utc::now(),
            msg_type: FrameUpdateType::Replace,
        }
    }
    pub fn kill(source: &str) -> Self {
        Self {
            source: source.to_owned(),
            timestamp: Utc::now(),
            msg_type: FrameUpdateType::Kill,
        }
    }
}

impl<FrameType> FrameUpdate<FrameType> {
    pub fn new(info: FrameUpdateInfo, frame: Arc<RwLock<FrameType>>) -> Self {
        Self { info, frame }
    }
}

/// Trait for all sinks which want to collect Frames into the exportable type
pub trait FrameCollector<FrameType> {
    fn collect_into(&mut self, frame: &FrameType) -> CpResult<()>;
}

/// Trait for broadcast handles
pub trait FrameBroadcastHandle<'a, FrameType> {
    fn broadcast(&mut self, frame: FrameType) -> CpResult<()>;
}

/// Trait for broadcast handles
pub trait FrameAsyncBroadcastHandle<'a, FrameType> {
    #[allow(async_fn_in_trait)]
    async fn broadcast(&mut self, frame: FrameType) -> CpResult<()>;
    #[allow(async_fn_in_trait)]
    async fn kill(&mut self) -> CpResult<()>;
}

/// Trait for listening handles
pub trait FrameListenHandle<'a, FrameType> {
    fn listen(&'a mut self) -> CpResult<FrameUpdate<FrameType>>;
}

/// Trait for listening handles
pub trait FrameAsyncListenHandle<'a, FrameType> {
    #[allow(async_fn_in_trait)]
    async fn listen(&'a mut self) -> CpResult<FrameUpdate<FrameType>>;
}

/// Trait for the implementation of the synchronized Frame holder.
/// Provides ability to produce broadcast handles, listening handles,
/// and adhoc extraction (without listening)
pub trait PipelineFrame<
    'a,
    FrameType,
    MaterializedFrameType,
    BroadcastHandle,
    ListenHandle,
    AsyncBroadcastHandle,
    AsyncListenHandle,
> where
    BroadcastHandle: FrameBroadcastHandle<'a, FrameType>,
    ListenHandle: FrameListenHandle<'a, FrameType>,
    AsyncBroadcastHandle: FrameAsyncBroadcastHandle<'a, FrameType>,
    AsyncListenHandle: FrameAsyncListenHandle<'a, FrameType>,
{
    fn get_broadcast_handle(&'a self, handle_name: &str) -> BroadcastHandle;
    fn get_listen_handle(&'a self, handle_name: &str) -> ListenHandle;
    fn get_async_broadcast_handle(&'a self, handle_name: &str) -> AsyncBroadcastHandle;
    fn get_async_listen_handle(&'a self, handle_name: &str) -> AsyncListenHandle;
    /// This method is used when the requestor is not waiting for an update but
    /// needs a snapshot of the materialized frame immediately.
    fn extract_clone(&self) -> CpResult<MaterializedFrameType>;
    fn extract(&self) -> CpResult<FrameType>;
    fn insert(&self, frame: FrameType) -> CpResult<()>;
}

/// Trait for sized results with name (aka label)
pub trait NamedSizedResult {
    fn new(label: &str, bufsize: usize) -> Self;
    fn label(&self) -> &str;
}
