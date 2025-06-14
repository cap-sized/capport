use argh::FromArgs;

pub const KEYWORD_RUN_MODE: &str = "RUN_MODE";

#[derive(FromArgs)]
#[argh(description = "default args for running pipeline")]
pub struct RunPipelineArgs {
    #[argh(option, short = 'c', description = "root directory for input configs")]
    pub config: String,

    #[argh(option, short = 'o', description = "root directory for outputs")]
    pub output: String,

    #[argh(option, short = 'r', description = "name of runner to use")]
    pub runner: String,

    #[argh(option, short = 'p', description = "name of pipeline to run")]
    pub pipeline: String,

    #[argh(option, short = 'd', description = "reference date of pipeline input")]
    pub date: Option<String>,

    #[argh(option, short = 'T', description = "reference datetime of pipeline input")]
    pub datetime: Option<String>,

    #[argh(switch, short = 'E', description = "execute real outputs")]
    pub execute: bool,

    #[argh(switch, short = 'C', description = "print output to console")]
    pub console: bool,
}

pub fn get_args() -> RunPipelineArgs {
    argh::from_env()
}
