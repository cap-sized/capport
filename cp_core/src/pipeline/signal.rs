use async_broadcast::{InactiveReceiver, Sender};
use chrono::Utc;

use crate::{
    frame::common::{FrameUpdateInfo, FrameUpdateType},
    util::error::{CpError, CpResult},
};

#[derive(Debug, Clone, Copy)]
pub enum SignalStateType {
    Alive,
    RequestedKill,
}

pub struct SignalState {
    pub sig_sender: Sender<FrameUpdateInfo>,
    pub sig_recver: InactiveReceiver<FrameUpdateInfo>,
}

impl Default for SignalState {
    fn default() -> Self {
        Self::new(2)
    }
}

impl SignalState {
    pub fn new(bufsize: usize) -> Self {
        let (mut sig_sender, sig_recver) = async_broadcast::broadcast(bufsize);
        sig_sender.set_overflow(true);
        Self {
            sig_sender,
            sig_recver: sig_recver.deactivate(),
        }
    }

    pub fn send_replace_signal(&self) -> CpResult<()> {
        match self.sig_sender.try_broadcast(FrameUpdateInfo {
            source: "REPLACE".to_owned(),
            timestamp: Utc::now(),
            msg_type: FrameUpdateType::Replace,
        }) {
            Ok(_) => Ok(()),
            Err(e) => Err(CpError::ComponentError(
                "Signal replace failed",
                format!("{}\n{:?}", e, self.sig_recver),
            )),
        }
    }

    pub async fn send_terminate_signal(&self) -> CpResult<()> {
        match self
            .sig_sender
            .broadcast(FrameUpdateInfo {
                source: "SIGTERM".to_owned(),
                timestamp: Utc::now(),
                msg_type: FrameUpdateType::Kill,
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(CpError::ComponentError("Signal terminating failed", e.to_string())),
        }
    }
}
