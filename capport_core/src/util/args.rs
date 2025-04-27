use argh::FromArgs;

#[derive(FromArgs)]
#[argh(description = "default args for running pipeline")]
pub struct RunPipelineArgs {
    #[argh(option, short = 'c', description = "directory for input configs")]
    pub config_dir: String,

    #[argh(switch, short = 'o', description = "print output to console")]
    pub output_to_console: bool,

    #[argh(option, short = 'p', description = "name of pipeline to run")]
    pub pipeline: String,

    #[argh(option, short = 'd', description = "datetime of pipeline input")]
    pub datetime: Option<String>,
}
