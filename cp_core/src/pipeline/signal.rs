use async_channel::{Receiver, Sender, unbounded};
use chrono::Utc;
use tokio::signal::unix::{Signal, SignalKind, signal};

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
    pub sig_recver: Receiver<FrameUpdateInfo>,
    sigterm_stream: Signal,
    state_type: SignalStateType,
}

impl Default for SignalState {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalState {
    pub fn new() -> Self {
        let (sig_sender, sig_recver) = unbounded();
        let sigterm_stream = signal(SignalKind::terminate()).expect("Failed to initialize SIGTERM stream");
        Self {
            sig_sender,
            sig_recver,
            sigterm_stream,
            state_type: SignalStateType::Alive,
        }
    }

    pub async fn send_replace_signal(&self) -> CpResult<()> {
        match self
            .sig_sender
            .send(FrameUpdateInfo {
                source: "REPLACE".to_owned(),
                timestamp: Utc::now(),
                msg_type: FrameUpdateType::Replace,
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(CpError::ComponentError("Signal replace failed", e.to_string())),
        }
    }

    pub async fn send_terminate_signal(&self) -> CpResult<()> {
        match self
            .sig_sender
            .send(FrameUpdateInfo {
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

    pub async fn sigterm_listen(&mut self) {
        loop {
            // Its ok to use expect here, these results should never yield Err
            self.sigterm_stream.recv().await.expect("Bad SIGTERM signal received");
            let curr_state = self.state_type;
            match curr_state {
                SignalStateType::Alive => {
                    log::warn!(
                        "Stages will terminate after completing their current event cycle. Ctrl-C again to force-kill"
                    );
                    self.state_type = SignalStateType::RequestedKill;
                    self.send_terminate_signal()
                        .await
                        .expect("Failed to send termination signal to stages");
                }
                SignalStateType::RequestedKill => {
                    log::warn!("Terminating immediately");
                    break;
                }
            }
        }
    }
}
