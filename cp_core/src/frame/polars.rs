use polars::prelude::*;
use std::{
    sync::{RwLock, atomic::AtomicBool},
    thread,
};

use crate::util::error::{CpError, CpResult};

use super::common::{
    FrameAsyncBroadcastHandle, FrameAsyncListenHandle, FrameBroadcastHandle, FrameListenHandle, FrameUpdate,
    FrameUpdateInfo, NamedSizedResult, PipelineFrame,
};

pub struct PolarsPipelineFrame {
    label: String,
    lf: Arc<RwLock<LazyFrame>>,
    df: Arc<RwLock<DataFrame>>,
    df_dirty: Arc<AtomicBool>,
    sender: multiqueue::BroadcastSender<FrameUpdateInfo>,
    receiver: multiqueue::BroadcastReceiver<FrameUpdateInfo>,
    asender: async_broadcast::Sender<FrameUpdateInfo>,
    areceiver: async_broadcast::InactiveReceiver<FrameUpdateInfo>,
}

#[derive(Clone)]
pub struct PolarsBroadcastHandle<'a> {
    handle_name: String,
    result_label: &'a str,
    sender: multiqueue::BroadcastSender<FrameUpdateInfo>,
    lf: Arc<RwLock<LazyFrame>>,
    df_dirty: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct PolarsListenHandle<'a> {
    handle_name: String,
    result_label: &'a str,
    receiver: multiqueue::BroadcastReceiver<FrameUpdateInfo>,
    lf: Arc<RwLock<LazyFrame>>,
}

#[derive(Clone)]
pub struct PolarsAsyncBroadcastHandle<'a> {
    handle_name: String,
    result_label: &'a str,
    sender: async_broadcast::Sender<FrameUpdateInfo>,
    lf: Arc<RwLock<LazyFrame>>,
    df_dirty: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct PolarsAsyncListenHandle<'a> {
    handle_name: String,
    result_label: &'a str,
    receiver: async_broadcast::Receiver<FrameUpdateInfo>,
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
        while let Err(e) = self.sender.try_send(update.clone()) {
            log::warn!("{}: {:?}", self.handle_name, e);
            thread::sleep(std::time::Duration::from_millis(100));
        }
        Ok(())
    }
}

impl<'a> FrameAsyncBroadcastHandle<'a, LazyFrame> for PolarsAsyncBroadcastHandle<'a> {
    fn broadcast(&mut self, frame: LazyFrame) -> CpResult<()> {
        // blocks until all other readers/writers are done
        {
            let mut lf = self.lf.write()?;
            *lf = frame;
        }
        // TODO: Relax when confirmed to be working
        self.df_dirty.store(true, std::sync::atomic::Ordering::SeqCst);
        // informs all readers
        let update = FrameUpdateInfo::new(self.handle_name.as_str());
        log::debug!("Frame sent from {}: {}", &self.handle_name, &self.result_label);
        match self.sender.try_broadcast(update) {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.is_disconnected() {
                    log::debug!("({}) {}: {}", &self.handle_name, &self.result_label, e);
                    Ok(())
                } else {
                    Err(CpError::PipelineError("Failed to broadcast replace", e.to_string()))
                }
            }
        }
    }

    fn kill(&mut self) -> CpResult<()> {
        let update = FrameUpdateInfo::kill(self.handle_name.as_str());
        match self.sender.try_broadcast(update) {
            Ok(_) => {
                log::debug!("Kill sent from {}: {}", &self.handle_name, &self.result_label);
                Ok(())
            }
            Err(e) => {
                if e.is_disconnected() {
                    log::debug!("({}) {}: {}", &self.handle_name, &self.result_label, e);
                    Ok(())
                } else {
                    Err(CpError::PipelineError("Failed to broadcast replace", e.to_string()))
                }
            }
        }
    }
}

impl<'a> FrameAsyncListenHandle<'a, LazyFrame> for PolarsAsyncListenHandle<'a> {
    async fn listen(&'a mut self) -> CpResult<FrameUpdate<LazyFrame>> {
        let info = match self.receiver.recv().await {
            Ok(x) => x,
            Err(e) => {
                return Err(CpError::PipelineError(
                    "Bad frame receiver:",
                    format!("{} could not receive {}: {:?}", &self.handle_name, self.result_label, e),
                ));
            }
        };
        log::debug!(
            "Frame `{}` read by {} after update from: {:?}",
            self.result_label,
            self.handle_name,
            &info
        );
        let update = FrameUpdate::new(info, self.lf.clone());
        Ok(update)
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
        log::debug!(
            "Frame `{}` read by {} after update from: {:?}",
            self.result_label,
            self.handle_name,
            &info
        );
        let update = FrameUpdate::new(info, self.lf.clone());
        Ok(update)
    }
    fn force_listen(&'a mut self) -> FrameUpdate<LazyFrame> {
        match self.receiver.try_recv() {
            Ok(info) => {
                log::debug!(
                    "Frame `{}` read by {} after update from: {:?}",
                    self.result_label,
                    self.handle_name,
                    &info
                );
                FrameUpdate::new(info, self.lf.clone())
            }
            Err(_) => FrameUpdate::new(FrameUpdateInfo::anon(), self.lf.clone()),
        }
    }
}

impl PolarsPipelineFrame {
    pub fn from(label: &str, bufsize: usize, lf: LazyFrame) -> Self {
        let (sender, receiver) = multiqueue::broadcast_queue(bufsize as u64);
        let (mut asender, areceiver) = async_broadcast::broadcast(bufsize);
        asender.set_overflow(true);
        Self {
            label: label.to_owned(),
            lf: Arc::new(RwLock::new(lf.clone())),
            df: Arc::new(RwLock::new(lf.collect().unwrap())),
            df_dirty: Arc::new(AtomicBool::new(false)),
            sender,
            receiver,
            asender,
            areceiver: areceiver.deactivate(),
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

impl<'a>
    PipelineFrame<
        'a,
        LazyFrame,
        DataFrame,
        PolarsBroadcastHandle<'a>,
        PolarsListenHandle<'a>,
        PolarsAsyncBroadcastHandle<'a>,
        PolarsAsyncListenHandle<'a>,
    > for PolarsPipelineFrame
{
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
    fn get_async_listen_handle(&'a self, handle_name: &str) -> PolarsAsyncListenHandle<'a> {
        PolarsAsyncListenHandle {
            handle_name: handle_name.to_owned(),
            result_label: self.label(),
            receiver: self.areceiver.clone().activate(),
            lf: self.lf.clone(),
        }
    }
    fn get_async_broadcast_handle(&'a self, handle_name: &str) -> PolarsAsyncBroadcastHandle<'a> {
        PolarsAsyncBroadcastHandle {
            handle_name: handle_name.to_owned(),
            result_label: self.label(),
            sender: self.asender.clone(),
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
            log::debug!("Cloning lazyframe `{}` as dataframe", self.label());
            Ok(df.clone())
        } else {
            Ok(self.df.read()?.clone())
        }
    }
    fn extract(&self) -> CpResult<LazyFrame> {
        let lf = self.lf.read()?.clone();
        log::debug!("Extracting lazyframe `{}`", self.label());
        Ok(lf.clone())
    }
    fn insert(&self, frame: LazyFrame) -> CpResult<()> {
        let mut lf = self.lf.write()?;
        *lf = frame;
        log::debug!("Inserted lazyframe `{}`", self.label());
        self.df_dirty.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crossbeam::thread;

    use polars::{df, frame::DataFrame, prelude::IntoLazy};

    use crate::{async_st, frame::common::{FrameBroadcastHandle, FrameListenHandle, NamedSizedResult, PipelineFrame}};

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
        async_st!(async || {
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
        });
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

        let lf = result.extract().unwrap();
        assert_eq!(lf.clone().collect().unwrap(), expected());

        {
            // Intercepting extract_clone
            assert!(result.is_cache_dirty());
            let df = result.extract_clone().unwrap();
            assert_eq!(df, expected());
        }

        {
            broadcast2.broadcast(DataFrame::empty().lazy()).unwrap();
        }

        assert_eq!(lf.collect().unwrap(), expected());

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
