use capport::config::{
    common::{pack_configs_from_files, read_configs},
    pipeline::PipelineRegistry,
};
use capport::util::args::RunPipelineArgs;

fn main() {
    let args: RunPipelineArgs = argh::from_env();
    let config_files = read_configs(&args.config_dir, vec!["yml", "yaml"]).unwrap();
    let pack = pack_configs_from_files(&config_files).unwrap();
    println!("{:?}", pack);
}
