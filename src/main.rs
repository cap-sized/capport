use capport::config::{
    common::{Configurable, pack_configs_from_files, read_configs},
    model::ModelRegistry,
    pipeline::PipelineRegistry,
};
use capport::util::args::RunPipelineArgs;

fn main() {
    let args: RunPipelineArgs = argh::from_env();
    let config_files = read_configs(&args.config_dir, vec!["yml", "yaml"]).unwrap();
    let mut pack = pack_configs_from_files(&config_files).unwrap();
    let model_reg = ModelRegistry::from(&mut pack);
    println!("{:?}", pack);
}
