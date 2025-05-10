use crossbeam::channel::bounded;
use polars::prelude::*;
use std::sync::{atomic::AtomicBool, RwLock};

use crate::util::error::{CpError, CpResult};

use super::common::{FrameBroadcastHandle, FrameListenHandle, FrameUpdate, FrameUpdateInfo, PipelineFrame};

pub struct PolarsPipelineFrame {
    label: String,
    lf: Arc<RwLock<LazyFrame>>,
    // df: Arc<RwLock<DataFrame>>,
    df_dirty: Arc<AtomicBool>,
    sender: crossbeam::channel::Sender<FrameUpdateInfo>,
    receiver: crossbeam::channel::Receiver<FrameUpdateInfo>,
}

pub struct PolarsBroadcastHandle<'a> {
    handle_name: String,
    result_label: &'a str,
    sender: crossbeam::channel::Sender<FrameUpdateInfo>,
    lf: Arc<RwLock<LazyFrame>>,
    df_dirty: Arc<AtomicBool>,
}

pub struct PolarsListenHandle<'a> {
    handle_name: String,
    result_label: &'a str,
    receiver: crossbeam::channel::Receiver<FrameUpdateInfo>,
    lf: Arc<RwLock<LazyFrame>>,
}


impl <'a>FrameBroadcastHandle<'a, LazyFrame> for PolarsBroadcastHandle<'a> {
    fn broadcast(&mut self, frame: LazyFrame) -> CpResult<()> {
        // blocks until all other readers/writers are done
        let mut lf = self.lf.write()?;
        *lf = frame;
        // TODO: Relax when confirmed to be working
        self.df_dirty.store(true, std::sync::atomic::Ordering::SeqCst);
        // informs all readers
        let update = FrameUpdateInfo::new(self.handle_name.as_str());
        log::trace!("Send from {}: {}", &self.handle_name, self.result_label);
        let _ = self.sender.send(update);
        Ok(())
    }
}

impl <'a>FrameListenHandle<'a, LazyFrame> for PolarsListenHandle<'a> {
    fn listen(&'a mut self) -> CpResult<FrameUpdate<LazyFrame>> {
        let info = match self.receiver.recv() {
            Ok(x) => x,
            Err(e) => {
                return Err(CpError::PipelineError(
                    "Bad receiver channel:",
                    format!("{} could not receive {}: {:?}", &self.handle_name, self.result_label, e),
                ));
            }
        };
        let update = FrameUpdate::new(info, self.lf.clone());
        return Ok(update);
    }
}

impl <'a>PipelineFrame<'a, LazyFrame, PolarsBroadcastHandle<'a>, PolarsListenHandle<'a>> for PolarsPipelineFrame {
    fn new(label: &str, bufsize: usize) -> Self {
        let df = DataFrame::empty();
        let (sender, receiver) = bounded(bufsize);
        Self {
            label: label.to_owned(),
            lf: Arc::new(RwLock::new(df.clone().lazy())),
            // df: Arc::new(RwLock::new(df)),
            df_dirty: Arc::new(AtomicBool::new(false)),
            sender,
            receiver,
        }
    }
    fn label(&self) -> &str {
        self.label.as_str()
    }
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
}


#[cfg(test)]
mod tests {
    use crossbeam::thread;

    use polars::{df, prelude::IntoLazy};

    use crate::frame::common::{FrameBroadcastHandle, FrameListenHandle, PipelineFrame};

    use super::PolarsPipelineFrame;

    
    #[test]
    fn valid_message_passing() {
        const SENDER: &str = "A";
        const RECEIVER: &str = "B";
        let result: PolarsPipelineFrame = PolarsPipelineFrame::new("result", 0);
        let mut listener = result.get_listen_handle(SENDER);
        let mut broadcast = result.get_broadcast_handle(RECEIVER);
        let expected = || df!( "a" => [1, 2, 3], "b" => [4, 5, 6] ).unwrap();
        let _ = thread::scope(|s| {
            let _bhandle = s.spawn(|_|{
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
}
