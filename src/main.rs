use std::sync::Arc;

use capport::context::task::TaskDictionary;
use capport::context::{model::ModelRegistry, pipeline::PipelineRegistry, transform::TransformRegistry};
use capport::parser::config::{pack_configs_from_files, read_configs};
use capport::pipeline::context::DefaultContext;
use capport::pipeline::runner::PipelineRunner;
use capport::util::args::RunPipelineArgs;
use polars::prelude::LazyFrame;

fn main() {
    let args: RunPipelineArgs = argh::from_env();
    let config_files = read_configs(&args.config_dir, &["yml", "yaml"]).unwrap();
    let mut pack = pack_configs_from_files(&config_files).unwrap();
    let model_reg = ModelRegistry::from(&mut pack).expect("Failed to build model registry");
    let transform_reg = TransformRegistry::from(&mut pack).expect("Failed to build transform registry");
    let pipeline_reg = PipelineRegistry::from(&mut pack).expect("Failed to build pipeline registry");
    let pipeline = match pipeline_reg.get_pipeline(&args.pipeline) {
        Some(x) => x,
        None => panic!("Pipeline `{}` not found in pipeline registry", &args.pipeline),
    };
    let ctx = Arc::new(DefaultContext::<LazyFrame, ()>::new(
        model_reg,
        transform_reg,
        TaskDictionary::default(),
        (),
    ));
    let pipeline_results = match PipelineRunner::run_lazy(ctx, pipeline) {
        Ok(x) => x,
        Err(e) => panic!("{}", e),
    };
    let final_results = pipeline_results.clone_all();
    for (name, table) in final_results {
        let _ = match table.collect() {
            Ok(x) => {
                println!("{}\n{:?}", name, &x);
                x
            }
            Err(e) => panic!("{}", e),
        };
    }
}
