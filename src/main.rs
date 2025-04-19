use capport::context::{model::ModelRegistry, pipeline::PipelineRegistry, transform::TransformRegistry};
use capport::parser::config::{pack_configs_from_files, read_configs};
use capport::util::args::RunPipelineArgs;

fn main() {
    let args: RunPipelineArgs = argh::from_env();
    let config_files = read_configs(&args.config_dir, &["yml", "yaml"]).unwrap();
    let mut pack = pack_configs_from_files(&config_files).unwrap();
    let _model_reg = ModelRegistry::from(&mut pack);
    let _transform_reg = TransformRegistry::from(&mut pack);
    let _pipieline_reg = PipelineRegistry::from(&mut pack);
    // println!("Models: {:?}", model_reg);
    // println!("Transform: {:?}", transform_reg);
}
