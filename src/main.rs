use capport::config::{
    common::{read_configs, sort_configs},
    pipeline::PipelineRegistry,
};
use capport::util::args::RunPipelineArgs;

fn main() {
    let args: RunPipelineArgs = argh::from_env();
    let configs = read_configs(&args.config_dir, vec!["yml", "yaml"]).unwrap();
    let pack = sort_configs(&configs).unwrap();
    println!("{:?}", pack);
}
