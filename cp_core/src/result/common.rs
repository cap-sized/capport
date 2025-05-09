use std::sync::{mpsc, Arc, RwLock};

use chrono::{DateTime, Utc};


pub struct ResultUpdate {
    pub source: String,
    pub timestamp: DateTime<Utc>
} 

pub trait Result<FrameType> {
    fn label(&self) -> &str;
    fn frame(&self) -> Arc<RwLock<FrameType>>;
    // NOTE: Trying to rely on std libraries right now, will convert to mpmc
    // once released, in order to support broadcast
    fn mpsc_listener(&self) -> mpsc::Receiver<ResultUpdate>;
    fn mpsc_sender(&self) -> mpsc::Sender<ResultUpdate>;
}

