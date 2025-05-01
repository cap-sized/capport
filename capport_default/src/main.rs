#![allow(clippy::uninlined_format_args)]
use std::sync::Arc;

use capport_core::context::envvar::EnvironmentVariableRegistry;
use capport_core::context::logger::LoggerRegistry;
use capport_core::context::runner::RunnerRegistry;
use capport_core::context::task::TaskDictionary;
use capport_core::context::{model::ModelRegistry, pipeline::PipelineRegistry, transform::TransformRegistry};
use capport_core::logger::common::DEFAULT_CONSOLE_LOGGER_NAME;
use capport_core::parser::config::{pack_configs_from_files, read_configs};
use capport_core::pipeline::context::{DefaultContext, PipelineContext};
use capport_core::pipeline::runner::{PipelineRunner, RunMethodType};
use capport_core::util::args::RunPipelineArgs;
use log::{error, info};
use polars::prelude::LazyFrame;

fn main() {
    let args: RunPipelineArgs = argh::from_env();
    let ev = EnvironmentVariableRegistry::from_args(&args).expect("Failed to register env variables"); // Need to setup env reg
    println!("Initialized environment variables: {:?}", ev.get_keys());
    let config_files = read_configs(&args.config_dir, &["yml", "yaml"]).unwrap();
    let mut pack = pack_configs_from_files(&config_files).unwrap();
    let model_reg = ModelRegistry::from(&mut pack).expect("Failed to build model registry");
    let transform_reg = TransformRegistry::from(&mut pack).expect("Failed to build transform registry");
    let pipeline_reg = PipelineRegistry::from(&mut pack).expect("Failed to build pipeline registry");
    let logger_reg = LoggerRegistry::from(&mut pack).expect("Failed to build logger registry");
    let runner_reg = RunnerRegistry::from(&mut pack).expect("Failed to build runner registry");
    let console_logger_name = if logger_reg.get_logger("default").is_some() {
        "default"
    } else {
        DEFAULT_CONSOLE_LOGGER_NAME
    };

    let mut ctx_setup =
        DefaultContext::<LazyFrame, ()>::new(model_reg, transform_reg, TaskDictionary::default(), (), logger_reg);
    let runner = match runner_reg.get_runner(&args.runner) {
        Some(x) => x,
        None => panic!("Runner `{}` not found in runner registry", &args.runner),
    };
    let pipeline = match pipeline_reg.get_pipeline(&args.pipeline) {
        Some(x) => x,
        None => panic!("Pipeline `{}` not found in pipeline registry", &args.pipeline),
    };
    ctx_setup
        .set_pipeline(pipeline.clone())
        .expect("Failed to attach pipeline to context");
    ctx_setup
        .init_log(console_logger_name, args.print_to_console)
        .expect("Failed to initialize logging");

    let ctx = Arc::new(ctx_setup);

    match runner.run_method {
        RunMethodType::SyncLazy => {
            let pipeline_results = match PipelineRunner::run_lazy(ctx.clone()) {
                Ok(x) => x,
                Err(e) => panic!("{}", e),
            };
            let final_results = pipeline_results.clone_all();
            for (name, table) in final_results {
                let _ = match table.collect() {
                    Ok(x) => {
                        info!("{}\n{:?}", name, &x);
                        x
                    }
                    Err(e) => panic!("{}", e),
                };
            }
        }
        s => error!("RunMethod not yet implemented: {:?}", s),
    }
}
