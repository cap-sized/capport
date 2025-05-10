use crossbeam::channel::bounded;
use polars::prelude::*;
use std::sync::{RwLock, atomic::AtomicBool};

use crate::util::error::{CpError, CpResult};

use super::common::{
    FrameBroadcastHandle, FrameListenHandle, FrameUpdate, FrameUpdateInfo, NamedSizedResult, PipelineFrame,
};

pub struct PolarsPipelineFrame {
    label: String,
    lf: Arc<RwLock<LazyFrame>>,
    df: Arc<RwLock<DataFrame>>,
    df_dirty: Arc<AtomicBool>,
    sender: crossbeam::channel::Sender<FrameUpdateInfo>,
    receiver: crossbeam::channel::Receiver<FrameUpdateInfo>,
}

#[derive(Clone)]
pub struct PolarsBroadcastHandle<'a> {
    handle_name: String,
    result_label: &'a str,
    sender: crossbeam::channel::Sender<FrameUpdateInfo>,
    lf: Arc<RwLock<LazyFrame>>,
    df_dirty: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct PolarsListenHandle<'a> {
    handle_name: String,
    result_label: &'a str,
    receiver: crossbeam::channel::Receiver<FrameUpdateInfo>,
    lf: Arc<RwLock<LazyFrame>>,
}

impl<'a> FrameBroadcastHandle<'a, LazyFrame> for PolarsBroadcastHandle<'a> {
    fn broadcast(&mut self, frame: LazyFrame) -> CpResult<()> {
        // blocks until all other readers/writers are done
        let mut lf = self.lf.write()?;
        *lf = frame;
        // TODO: Relax when confirmed to be working
        self.df_dirty.store(true, std::sync::atomic::Ordering::SeqCst);
        // informs all readers
        let update = FrameUpdateInfo::new(self.handle_name.as_str());
        log::debug!("Frame sent from {}: {}", &self.handle_name, &self.result_label);
        let _ = self.sender.send(update);
        Ok(())
    }
}

impl<'a> FrameListenHandle<'a, LazyFrame> for PolarsListenHandle<'a> {
    fn listen(&'a mut self) -> CpResult<FrameUpdate<LazyFrame>> {
        // listens for first change. 
        // NOTE: if the channel size is NOT 1, the change read from the frame now may NOT be
        // the one corresponding to the message received.
        let info = match self.receiver.recv() {
            Ok(x) => x,
            Err(e) => {
                return Err(CpError::PipelineError(
                    "Bad frame receiver:",
                    format!("{} could not receive {}: {:?}", &self.handle_name, self.result_label, e),
                ));
            }
        };
        log::debug!("Frame `{}` read by {} after update from: {:?}", self.result_label, self.handle_name, &info);
        let update = FrameUpdate::new(info, self.lf.clone());
        Ok(update)
    }
}

impl PolarsPipelineFrame {
    pub fn from(label: &str, bufsize: usize, lf: LazyFrame) -> Self {
        let (sender, receiver) = bounded(bufsize);
        Self {
            label: label.to_owned(),
            lf: Arc::new(RwLock::new(lf.clone())),
            df: Arc::new(RwLock::new(lf.collect().unwrap())),
            df_dirty: Arc::new(AtomicBool::new(false)),
            sender,
            receiver,
        }
    }

    pub fn is_cache_dirty(&self) -> bool {
        self.df_dirty.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl NamedSizedResult for PolarsPipelineFrame {
    fn new(label: &str, bufsize: usize) -> Self {
        Self::from(label, bufsize, DataFrame::empty().lazy())
    }
    fn label(&self) -> &str {
        self.label.as_str()
    }
}

impl<'a> PipelineFrame<'a, LazyFrame, DataFrame, PolarsBroadcastHandle<'a>, PolarsListenHandle<'a>> for PolarsPipelineFrame {
    fn get_listen_handle(&'a self, handle_name: &str) -> PolarsListenHandle<'a> {
        PolarsListenHandle {
            handle_name: handle_name.to_owned(),
            result_label: self.label(),
            receiver: self.receiver.clone(),
            lf: self.lf.clone(),
        }
    }
    fn get_broadcast_handle(&'a self, handle_name: &str) -> PolarsBroadcastHandle<'a> {
        PolarsBroadcastHandle {
            handle_name: handle_name.to_owned(),
            result_label: self.label(),
            sender: self.sender.clone(),
            df_dirty: self.df_dirty.clone(),
            lf: self.lf.clone(),
        }
    }
    fn extract_clone(&self) -> CpResult<DataFrame> {
        let reset_dirty = self.df_dirty.clone();
        if self.df_dirty.load(std::sync::atomic::Ordering::SeqCst) {
            let lf = self.lf.read()?.clone();
            let mut df = self.df.write()?;
            *df = lf.collect()?;
            reset_dirty.store(false, std::sync::atomic::Ordering::SeqCst);
            Ok(df.clone())
        } else {
            Ok(self.df.read()?.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use crossbeam::thread;

    use polars::{df, frame::DataFrame, prelude::IntoLazy};

    use crate::frame::common::{FrameBroadcastHandle, FrameListenHandle, NamedSizedResult, PipelineFrame};

    use super::PolarsPipelineFrame;

    #[test]
    fn valid_message_passing() {
        const SENDER: &str = "A";
        const RECEIVER: &str = "B";
        let result: PolarsPipelineFrame = PolarsPipelineFrame::new("result", 1);
        let mut listener = result.get_listen_handle(RECEIVER);
        let mut broadcast = result.get_broadcast_handle(SENDER);
        let expected = || df!( "a" => [1, 2, 3], "b" => [4, 5, 6] ).unwrap();
        let _ = thread::scope(|s| {
            let _bhandle = s.spawn(|_| {
                broadcast.broadcast(expected().lazy()).unwrap();
            });
            let _lhandle = s.spawn(|_| {
                let update = listener.listen().unwrap();
                let lf = update.frame.read().unwrap().clone();
                let actual = lf.collect().unwrap();
                assert_eq!(actual, expected().clone());
            });
        });
    }

    #[test]
    fn valid_async_message_passing() {
        const SENDER: &str = "A";
        const RECEIVER: &str = "B";
        let mut rt_builder = tokio::runtime::Builder::new_current_thread();
        rt_builder.enable_all();
        let rt = rt_builder.build().unwrap();
        let event = async || {
            let result: PolarsPipelineFrame = PolarsPipelineFrame::new("result", 1);
            let listener = result.get_listen_handle(RECEIVER);
            let mut broadcast = result.get_broadcast_handle(SENDER);
            let expected = || df!( "a" => [1, 2, 3], "b" => [4, 5, 6] ).unwrap();
            let mut bhandle = async move || {
                broadcast.broadcast(expected().lazy()).unwrap();
            };
            let lhandle = async move || {
                let update = listener.clone().listen().unwrap();
                let lf = update.frame.read().unwrap().clone();
                let actual = lf.collect().unwrap();
                assert_eq!(actual, expected().clone());
            };
            tokio::join!(bhandle(), lhandle());
        };
        rt.block_on(event());
    }

    #[test]
    fn valid_frame_extract_clone() {
        const SENDER1: &str = "A1";
        const SENDER2: &str = "A2";
        const RECEIVER: &str = "B";
        // NOTE: e.g. here we have a size two channel.
        let result: PolarsPipelineFrame = PolarsPipelineFrame::new("result", 2);
        let listener = result.get_listen_handle(RECEIVER);
        let mut broadcast1 = result.get_broadcast_handle(SENDER1);
        let mut broadcast2 = result.get_broadcast_handle(SENDER2);
        assert!(!result.is_cache_dirty());
        let expected = || df!( "a" => [1, 2, 3], "b" => [4, 5, 6] ).unwrap();
        {
            broadcast1.broadcast(expected().lazy()).unwrap();
        }

        {
            // Intercepting extract_clone
            assert!(result.is_cache_dirty());
            let df = result.extract_clone().unwrap();
            assert_eq!(df, expected());
        }

        {
            broadcast2.broadcast(DataFrame::empty().lazy()).unwrap();
        }

        {
            // Another intercepting extract_clone
            assert!(result.is_cache_dirty());
            let empty = result.extract_clone().unwrap();
            assert_eq!(empty, DataFrame::empty());
        }

        {
            // NOTE: here the listener only sees the LAST update, even though it reads the FIRST
            // message.
            let update = listener.clone().listen().unwrap();
            let lf = update.frame.read().unwrap().clone();
            let actual = lf.collect().unwrap();
            assert_eq!(actual, DataFrame::empty());
            assert_eq!(update.info.source.as_str(), SENDER1);
        }

        // Uses cached result
        assert!(!result.is_cache_dirty());
        let empty = result.extract_clone().unwrap();
        assert_eq!(empty, DataFrame::empty());
}
}
