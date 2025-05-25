use cp_core::{runner::common::Runner, util::{args::get_args, error::CpResult}};

fn exec() -> CpResult<()> {
    let mut runner = Runner::init(get_args())?;
    runner.start_log()?;
    runner.run()
}

fn main() {
    match exec() {
        Ok(_) => {},
        Err(e) => {
            log::error!("Failed on building runner:\n{}", e)
        }
    }
}

