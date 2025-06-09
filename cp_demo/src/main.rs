use cp_core::{
    runner::common::Runner,
    util::{args::get_args, error::CpResult},
};

fn exec() -> CpResult<()> {
    let mut runner = Runner::init(get_args())?;
    runner.start_log()?;
    runner.print_env()?;
    runner.run()
}

fn main() {
    exec().expect("runner");
}
