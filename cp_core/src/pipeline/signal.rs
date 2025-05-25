use async_broadcast::{Receiver, Sender};
use chrono::Utc;
use tokio::signal::unix::{SignalKind, signal};

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
}

impl Default for SignalState {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalState {
    pub fn new() -> Self {
        let (sig_sender, sig_recver) = async_broadcast::broadcast(2);
        Self {
            sig_sender,
            sig_recver,
        }
    }

    pub async fn send_replace_signal(&self) -> CpResult<()> {
        match self
            .sig_sender
            .broadcast(FrameUpdateInfo {
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

    pub async fn sigterm_listen(&self) {
        loop {
            // Its ok to use expect here, these results should never yield Err
            let mut sigterm_stream = signal(SignalKind::terminate()).expect("Failed to initialize SIGTERM stream");
            sigterm_stream.recv().await.expect("Bad SIGTERM signal received");
            let mut curr_state = Some(SignalStateType::Alive);
            match curr_state.as_ref().unwrap() {
                SignalStateType::Alive => {
                    log::warn!(
                        "Stages will terminate after completing their current event cycle. Ctrl-C again to force-kill"
                    );
                    let _ = curr_state.insert(SignalStateType::RequestedKill);
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
