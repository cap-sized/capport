use std::sync::Arc;

use serde::Deserialize;

use crate::{
    ctx_run_n_async,
    task::stage::{Stage, StageConfig},
    util::error::{CpError, CpResult},
};

use super::context::{DefaultPipelineContext, PipelineContext};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PipelineConfig {
    pub label: String,
    pub stages: Vec<StageConfig>,
}

pub struct Pipeline {
    config: PipelineConfig,
}

impl Pipeline {
    pub fn new(config: &PipelineConfig) -> Self {
        Self { config: config.clone() }
    }
    pub fn prepare_results(
        &self,
        ctx: DefaultPipelineContext,
        bufsize: usize,
    ) -> CpResult<Arc<DefaultPipelineContext>> {
        let mut results_needed = vec![];
        for stage in &self.config.stages {
            let label = &stage.task_name;
            let context = &stage.emplace;
            let ictx = &ctx;
            match stage.task_type.as_str() {
                "transform" => {
                    let transform = ictx.get_transform(label, context)?;
                    results_needed.extend(transform.produces());
                }
                "source" => {
                    let source = ictx.get_source(label, context)?;
                    results_needed.extend(source.produces());
                }
                "sink" => {}
                invalid => {
                    return Err(CpError::ConfigError(
                        "Invalid task type",
                        format!(
                            "Task has to be one of `transform`, `source`, `sink`, found: {}",
                            invalid
                        ),
                    ));
                }
            }
        }
        Ok(Arc::new(ctx.initialize_results(results_needed, bufsize)?))
    }
    pub fn linear(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        let mut actions: Vec<Box<dyn FnOnce() -> CpResult<()>>> = vec![];
        for stage in &self.config.stages {
            let label = &stage.task_name;
            let context = &stage.emplace;
            let ictx = ctx.clone();
            match stage.task_type.as_str() {
                "transform" => {
                    let transform = ictx.get_transform(label, context)?;
                    actions.push(Box::new(move || transform.linear(ictx)));
                }
                "source" => {
                    let source = ictx.get_source(label, context)?;
                    actions.push(Box::new(move || source.linear(ictx)));
                }
                "sink" => {
                    let sink = ictx.get_sink(label, context)?;
                    actions.push(Box::new(move || sink.linear(ictx)));
                }
                invalid => {
                    return Err(CpError::ConfigError(
                        "Invalid task type",
                        format!(
                            "Task has to be one of `transform`, `source`, `sink`, found: {}",
                            invalid
                        ),
                    ));
                }
            }
        }
        for action in actions {
            action()?;
        }
        Ok(())
    }
    pub fn sync_exec(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        let mut actions: Vec<Box<dyn FnOnce() -> CpResult<()>>> = vec![];
        for stage in &self.config.stages {
            let label = &stage.task_name;
            let context = &stage.emplace;
            let ictx = ctx.clone();
            match stage.task_type.as_str() {
                "transform" => {
                    let transform = ictx.get_transform(label, context)?;
                    actions.push(Box::new(move || transform.sync_exec(ictx)));
                }
                "source" => {
                    let source = ictx.get_source(label, context)?;
                    actions.push(Box::new(move || source.sync_exec(ictx)));
                }
                "sink" => {
                    let sink = ictx.get_sink(label, context)?;
                    actions.push(Box::new(move || sink.sync_exec(ictx)));
                }
                invalid => {
                    return Err(CpError::ConfigError(
                        "Invalid task type",
                        format!(
                            "Task has to be one of `transform`, `source`, `sink`, found: {}",
                            invalid
                        ),
                    ));
                }
            }
        }
        for action in actions {
            match action() {
                Ok(_) => {}
                Err(e) => log::error!("{}", e),
            }
        }
        Ok(())
    }

    pub async fn async_exec(&self, ctx: Arc<DefaultPipelineContext>) {
        let config = self.config.clone();
        ctx_run_n_async!(
            "async_pipe",
            &config.stages,
            ctx.clone(),
            async |stage: &StageConfig, ictx: Arc<DefaultPipelineContext>| {
                match stage.task_type.as_str() {
                    "transform" => {
                        let stage = ictx.get_transform(&stage.task_name, &stage.emplace)?;
                        stage.async_exec(ictx).await
                    }
                    "source" => {
                        let stage = ictx.get_source(&stage.task_name, &stage.emplace)?;
                        stage.async_exec(ictx).await
                    }
                    "sink" => {
                        let stage = ictx.get_sink(&stage.task_name, &stage.emplace)?;
                        stage.async_exec(ictx).await
                    }
                    invalid => Err(CpError::ConfigError(
                        "Invalid task type",
                        format!(
                            "Task has to be one of `transform`, `source`, `sink`, found: {}",
                            invalid
                        ),
                    )),
                }
            }
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use polars::{df, frame::DataFrame, io::SerReader, prelude::CsvReader};

    use crate::{
        context::{model::ModelRegistry, sink::SinkRegistry, source::SourceRegistry, transform::TransformRegistry},
        model::common::ModelConfig,
        parser::keyword::{Keyword, StrKeyword},
        pipeline::{
            context::{DefaultPipelineContext, PipelineContext},
            results::PipelineResults,
        },
        task::{
            sink::config::SinkGroupConfig, source::config::SourceGroupConfig, transform::config::RootTransformConfig,
        },
        util::{test::assert_frame_equal, tmp::TempFile},
    };

    use super::Pipeline;

    fn get_ctx(max_threads: usize) -> DefaultPipelineContext {
        let results = PipelineResults::new();
        let mut model_registry = ModelRegistry::new();
        model_registry.insert(ModelConfig {
            label: "instrument".to_owned(),
            fields: HashMap::from([
                (
                    StrKeyword::with_value("ric".to_owned()),
                    serde_yaml_ng::from_str("str").unwrap(),
                ),
                (
                    StrKeyword::with_value("mkt".to_owned()),
                    serde_yaml_ng::from_str("str").unwrap(),
                ),
                (
                    StrKeyword::with_value("id".to_owned()),
                    serde_yaml_ng::from_str("uint64").unwrap(),
                ),
            ]),
        });
        let mut transform_registry = TransformRegistry::new();
        transform_registry.insert(RootTransformConfig {
            label: "trf".to_owned(),
            input: StrKeyword::with_symbol("input"),
            output: StrKeyword::with_symbol("output"),
            steps: serde_yaml_ng::from_str(
                "
- select:
    id: id
    code: ric
    region: mkt
- join:
    right: $joiner
    right_prefix: R_
    left_on: [id]
    right_on: [R_id]
    how: left
- select:
    code: code
    price: R_price
",
            )
            .unwrap(),
        });
        let mut source_registry = SourceRegistry::new();
        source_registry.insert(SourceGroupConfig {
            label: "src".to_owned(),
            max_threads,
            sources: serde_yaml_ng::from_str(
                "
- json:
    filepath: $left_fp
    output: $left
    model: $left_model
- json:
    filepath: $right_fp
    output: $right
    model_fields: 
        price: double
        id: uint64
",
            )
            .unwrap(),
        });
        let mut sink_registry = SinkRegistry::new();
        sink_registry.insert(SinkGroupConfig {
            label: "snk".to_owned(),
            input: StrKeyword::with_symbol("input"),
            max_threads,
            sinks: serde_yaml_ng::from_str(
                "
- csv:
    filepath: $fp1
- csv:
    filepath: $fp2
",
            )
            .unwrap(),
        });
        DefaultPipelineContext::from(
            results,
            model_registry,
            transform_registry,
            source_registry,
            sink_registry,
        )
    }

    fn get_pipeline(left_fp: &str, right_fp: &str, output_fp1: &str, output_fp2: &str) -> Pipeline {
        let config = format!(
            "
label: something
stages:
    - label: load_tables
      task_type: source
      task_name: src
      emplace:
        left_model: instrument
        left_fp: {}
        right_fp: {}
        left: INSTRUMENTS
        right: PRICES
    - label: transforming
      task_type: transform
      task_name: trf
      emplace:
        input: INSTRUMENTS
        joiner: PRICES
        output: PXINST
    - label: final_sinks
      task_type: sink
      task_name: snk
      emplace:
        input: PXINST
        fp1: {}
        fp2: {}
",
            left_fp, right_fp, output_fp1, output_fp2
        );
        Pipeline {
            config: serde_yaml_ng::from_str(&config).unwrap(),
        }
    }

    fn get_prices() -> DataFrame {
        df![
            "id" => 0..7,
            "price" => [50.1, 62.3, 69.10, 88.8, 30.2, 29.2, 40.3],
        ]
        .unwrap()
    }

    fn get_instruments() -> DataFrame {
        df![
            "ric" => ["AAPL", "AMZN", "GOOG", "NVDA", "NOVA", "BABA", "SPOT"],
            "mkt" => ["amer", "amer", "amer", "amer", "emea", "apac", "emea"],
            "id" => 0..7,
        ]
        .unwrap()
    }

    fn get_expected() -> DataFrame {
        df![
            "code" => ["AAPL", "AMZN", "GOOG", "NVDA", "NOVA", "BABA", "SPOT"],
            "price" => [50.1, 62.3, 69.10, 88.8, 30.2, 29.2, 40.3],
        ]
        .unwrap()
    }

    #[test]
    fn basic_linear_pipeline() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let mut prices = get_prices();
        let mut instruments = get_instruments();
        let price_file = TempFile::default();
        price_file.write_json(&mut prices).unwrap();
        let inst_file = TempFile::default();
        inst_file.write_json(&mut instruments).unwrap();
        let output_fp1 = TempFile::default();
        let output_fp2 = TempFile::default();
        let pipeline = get_pipeline(
            &inst_file.filepath,
            &price_file.filepath,
            &output_fp1.filepath,
            &output_fp2.filepath,
        );
        let pctx = get_ctx(1);
        let ctx = pipeline.prepare_results(pctx, 2).unwrap();
        pipeline.linear(ctx.clone()).unwrap();
        let pxinst = ctx.extract_clone_result("PXINST").unwrap();

        let fp1 = output_fp1.get().unwrap();
        let csv_fp1 = CsvReader::new(fp1);
        let fp2 = output_fp2.get().unwrap();
        let csv_fp2 = CsvReader::new(fp2);
        assert_frame_equal(get_expected(), csv_fp1.finish().unwrap());
        assert_frame_equal(get_expected(), csv_fp2.finish().unwrap());
        assert_frame_equal(get_expected(), pxinst);
    }

    #[test]
    fn basic_sync_exec_pipeline() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let mut prices = get_prices();
        let mut instruments = get_instruments();
        let price_file = TempFile::default();
        price_file.write_json(&mut prices).unwrap();
        let inst_file = TempFile::default();
        inst_file.write_json(&mut instruments).unwrap();
        let output_fp1 = TempFile::default();
        let output_fp2 = TempFile::default();
        let pipeline = get_pipeline(
            &inst_file.filepath,
            &price_file.filepath,
            &output_fp1.filepath,
            &output_fp2.filepath,
        );
        let pctx = get_ctx(3);
        let ctx = pipeline.prepare_results(pctx, 2).unwrap();
        pipeline.sync_exec(ctx.clone()).unwrap();
        let pxinst = ctx.extract_clone_result("PXINST").unwrap();

        let fp1 = output_fp1.get().unwrap();
        let csv_fp1 = CsvReader::new(fp1);
        let fp2 = output_fp2.get().unwrap();
        let csv_fp2 = CsvReader::new(fp2);
        assert_frame_equal(get_expected(), csv_fp1.finish().unwrap());
        assert_frame_equal(get_expected(), csv_fp2.finish().unwrap());
        assert_frame_equal(get_expected(), pxinst);
    }

    #[test]
    fn basic_async_exec_pipeline() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let mut rt_builder = tokio::runtime::Builder::new_current_thread();
        rt_builder.enable_all();
        let rt = rt_builder.build().unwrap();
        let event = async move || {
            let mut prices = get_prices();
            let mut instruments = get_instruments();
            let price_file = TempFile::default();
            price_file.write_json(&mut prices).unwrap();
            let inst_file = TempFile::default();
            inst_file.write_json(&mut instruments).unwrap();
            let output_fp1 = TempFile::default();
            let output_fp2 = TempFile::default();
            let pipeline = get_pipeline(
                &inst_file.filepath,
                &price_file.filepath,
                &output_fp1.filepath,
                &output_fp2.filepath,
            );
            let pctx = get_ctx(3).with_signal();
            let ctx = pipeline.prepare_results(pctx, 2).unwrap();

            let ictx = ctx.clone();
            let iictx = ictx.clone();
            let action_path = async move || {
                pipeline.async_exec(ictx).await;
                let pxinst = ctx.extract_clone_result("PXINST").unwrap();

                let fp1 = output_fp1.get().unwrap();
                let csv_fp1 = CsvReader::new(fp1);
                let fp2 = output_fp2.get().unwrap();
                let csv_fp2 = CsvReader::new(fp2);
                assert_frame_equal(get_expected(), pxinst);
                assert_frame_equal(get_expected(), csv_fp1.finish().unwrap());
                assert_frame_equal(get_expected(), csv_fp2.finish().unwrap());
            };
            let terminator = async move || {
                iictx.signal_replace().await.unwrap();
                iictx.signal_terminate().await.unwrap();
            };
            tokio::join!(action_path(), terminator());
        };
        rt.block_on(event());
    }
}
