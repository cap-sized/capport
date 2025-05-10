use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};

use crate::util::error::CpResult;

pub struct FrameUpdateInfo {
    pub source: String,
    pub timestamp: DateTime<Utc>,
}

pub struct FrameUpdate<FrameType> {
    pub info: FrameUpdateInfo,
    pub frame: Arc<RwLock<FrameType>>,
}

impl FrameUpdateInfo {
    pub fn new(source: &str) -> Self {
        Self {
            source: source.to_owned(),
            timestamp: Utc::now(),
        }
    }
}

impl<FrameType> FrameUpdate<FrameType> {
    pub fn new(info: FrameUpdateInfo, frame: Arc<RwLock<FrameType>>) -> Self {
        Self { info, frame }
    }
}

pub trait FrameCollector<FrameType> {
    fn collect_into(&mut self, frame: &FrameType) -> CpResult<()>;
}

pub trait FrameBroadcastHandle<'a, FrameType> {
    fn broadcast(&mut self, frame: FrameType) -> CpResult<()>;
}

pub trait FrameListenHandle<'a, FrameType> {
    fn listen(&'a mut self) -> CpResult<FrameUpdate<FrameType>>;
}

pub trait PipelineFrame<'a, FrameType, BroadcastHandle, ListenHandle>
where
    BroadcastHandle: FrameBroadcastHandle<'a, FrameType>,
    ListenHandle: FrameListenHandle<'a, FrameType>,
{
    fn get_broadcast_handle(&'a self, handle_name: &str) -> BroadcastHandle;
    fn get_listen_handle(&'a self, handle_name: &str) -> ListenHandle;
}

pub trait NamedSizedResult {
    fn new(label: &str, bufsize: usize) -> Self;
    fn label(&self) -> &str;
}
