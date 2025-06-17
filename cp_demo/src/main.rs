use std::sync::{atomic::AtomicBool, Arc};

use cp_core::{
    runner::common::Runner,
    util::{args::get_args, error::CpResult},
};

const RESTART_INTERVAL_SECS: u64 = 10;

fn exec() -> CpResult<()> {
    let mut runner = Runner::init(get_args())?;
    runner.start_log()?;
    runner.print_env()?;
    runner.run()
}

fn main() {
    let ok = Arc::new(AtomicBool::new(false));
    while !ok.load(std::sync::atomic::Ordering::Relaxed) {
        match std::panic::catch_unwind(|| {
            match exec() {
                Ok(_) => {}
                Err(e) => {
                    log::error!("Failed to execute pipeline: {:?}.\nRestarting in {} seconds", e, RESTART_INTERVAL_SECS);
                    std::thread::sleep(std::time::Duration::from_secs(RESTART_INTERVAL_SECS));
                }
            }
        }) {
            Ok(_) => {
                ok.clone().store(true, std::sync::atomic::Ordering::Relaxed);
            },
            Err(e) => {
                log::warn!("Pipeline panicked: {:?}.\nRestarting in {} seconds", e, RESTART_INTERVAL_SECS);
                std::thread::sleep(std::time::Duration::from_secs(RESTART_INTERVAL_SECS));
            }
        }
    }
}
