use std::sync::Arc;

use serde::Deserialize;

use crate::{
    ctx_run_n_async,
    parser::task_type::TaskTypeEnum,
    task::stage::{Stage, StageConfig},
    util::error::CpResult,
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
            match stage.task_type {
                TaskTypeEnum::Transform => {
                    let transform = ictx.get_transform(label, context)?;
                    results_needed.extend(transform.produces());
                }
                TaskTypeEnum::Source => {
                    let source = ictx.get_source(label, context)?;
                    results_needed.extend(source.produces());
                }
                TaskTypeEnum::Sink => {}
                TaskTypeEnum::Request => {
                    let request = ictx.get_request(label, context)?;
                    results_needed.extend(request.produces());
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
            match stage.task_type {
                TaskTypeEnum::Transform => {
                    let transform = ictx.get_transform(label, context)?;
                    actions.push(Box::new(move || transform.linear(ictx)));
                }
                TaskTypeEnum::Source => {
                    let source = ictx.get_source(label, context)?;
                    actions.push(Box::new(move || source.linear(ictx)));
                }
                TaskTypeEnum::Sink => {
                    let sink = ictx.get_sink(label, context)?;
                    actions.push(Box::new(move || sink.linear(ictx)));
                }
                TaskTypeEnum::Request => {
                    let request = ictx.get_request(label, context)?;
                    actions.push(Box::new(move || request.linear(ictx)));
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
            match stage.task_type {
                TaskTypeEnum::Transform => {
                    let transform = ictx.get_transform(label, context)?;
                    actions.push(Box::new(move || transform.sync_exec(ictx)));
                }
                TaskTypeEnum::Source => {
                    let source = ictx.get_source(label, context)?;
                    actions.push(Box::new(move || source.sync_exec(ictx)));
                }
                TaskTypeEnum::Sink => {
                    let sink = ictx.get_sink(label, context)?;
                    actions.push(Box::new(move || sink.sync_exec(ictx)));
                }
                TaskTypeEnum::Request => {
                    let request = ictx.get_request(label, context)?;
                    actions.push(Box::new(move || request.sync_exec(ictx)));
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
            async |stage: &StageConfig, ictx: Arc<DefaultPipelineContext>| {
                match stage.task_type {
                    TaskTypeEnum::Transform => {
                        let stage = ictx.get_transform(&stage.task_name, &stage.emplace)?;
                        stage.async_exec(ictx).await
                    }
                    TaskTypeEnum::Source => {
                        let stage = ictx.get_source(&stage.task_name, &stage.emplace)?;
                        stage.async_exec(ictx).await
                    }
                    TaskTypeEnum::Sink => {
                        let stage = ictx.get_sink(&stage.task_name, &stage.emplace)?;
                        stage.async_exec(ictx).await
                    }
                    TaskTypeEnum::Request => {
                        let stage = ictx.get_request(&stage.task_name, &stage.emplace)?;
                        stage.async_exec(ictx).await
                    }
                }
            },
            ctx.clone()
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use httpmock::{Method::GET, Mock, MockServer};
    use polars::{df, frame::DataFrame, io::SerReader, prelude::CsvReader};
    use serde::{Deserialize, Serialize};

    use crate::{
        context::{
            model::ModelRegistry,
            request::{RequestRegistry},
            sink::SinkRegistry,
            source::SourceRegistry,
            transform::TransformRegistry,
        },
        model::common::ModelConfig,
        parser::keyword::{Keyword, StrKeyword},
        pipeline::{
            context::{DefaultPipelineContext, PipelineContext},
            results::PipelineResults,
        },
        task::{
            request::config::RequestGroupConfig, sink::config::SinkGroupConfig, source::config::SourceGroupConfig,
            transform::config::RootTransformConfig,
        },
        util::{
            test::{DummyData, assert_frame_equal},
            tmp::TempFile,
        },
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
    url: $base_url
- join:
    right: $joiner
    right_prefix: local_
    left_on: [id]
    right_on: [local_id]
    how: left
",
            )
            .unwrap(),
        });
        transform_registry.insert(RootTransformConfig {
            label: "merge_px".to_owned(),
            input: StrKeyword::with_symbol("input"),
            output: StrKeyword::with_symbol("output"),
            steps: serde_yaml_ng::from_str(
                "
- join:
    right: $joiner
    left_on: [id]
    right_on: [id]
    how: left
- select:
    code: code
    local_price: local_price
    live_price: price
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
        let mut request_registry = RequestRegistry::new();
        request_registry.insert(RequestGroupConfig {
            label: "fetch_px".to_owned(),
            input: StrKeyword::with_symbol("urls"),
            max_threads,
            requests: serde_yaml_ng::from_str(
                "
- http_batch:
    method: get
    content_type: application/json
    output: $output
    url_column: url
    model_fields:
        id: uint64
        price: double
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
            request_registry,
        )
    }

    fn get_pipeline(
        left_fp: &str,
        right_fp: &str,
        mock_server_root: &str,
        output_fp1: &str,
        output_fp2: &str,
    ) -> Pipeline {
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
        base_url: 
            format: 
                template: {}/price?id={{}}
                columns: [id]
    - label: fetch_foreign_px
      task_type: request
      task_name: fetch_px
      emplace:
        urls: PXINST
        output: FOREIGNPX
    - label: merge_px
      task_type: transform
      task_name: merge_px
      emplace:
        input: FOREIGNPX
        joiner: PXINST
        output: FIN
    - label: final_sinks
      task_type: sink
      task_name: snk
      emplace:
        input: FIN
        fp1: {}
        fp2: {}
",
            left_fp, right_fp, mock_server_root, output_fp1, output_fp2
        );
        Pipeline {
            config: serde_yaml_ng::from_str(&config).unwrap(),
        }
    }

    fn get_expected() -> DataFrame {
        df![
            "code" => ["AAPL", "AMZN", "GOOG", "NVDA", "NOVA", "BABA", "SPOT"],
            "local_price" => [50.1, 62.3, 69.10, 88.8, 30.2, 29.2, 40.3],
            "live_price" => [50.1, 62.3, 69.10, 88.8, 30.2, 29.2, 40.3],
        ]
        .unwrap()
    }

    #[derive(Serialize, Deserialize)]
    struct InstrPrice {
        id: u64,
        price: f64,
    }

    fn mock_server(server: &MockServer) -> Vec<Mock> {
        DummyData::json_instrument_prices()
            .iter()
            .map(|j| {
                let px: InstrPrice = serde_yaml_ng::from_str(j).unwrap();
                server.mock(|when, then| {
                    when.method(GET).path("/price").query_param("id", px.id.to_string());
                    then.status(200).header("content-type", "application/json").body(j);
                })
            })
            .collect()
    }

    #[test]
    fn basic_linear_pipeline() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let mut prices = DummyData::df_instrument_prices();
        let mut instruments = DummyData::df_instruments();
        let price_file = TempFile::default();
        price_file.write_json(&mut prices).unwrap();
        let inst_file = TempFile::default();
        inst_file.write_json(&mut instruments).unwrap();
        let output_fp1 = TempFile::default();
        let output_fp2 = TempFile::default();
        let server = MockServer::start();
        let mocks = mock_server(&server);
        let base_url = server.base_url();
        let pipeline = get_pipeline(
            &inst_file.filepath,
            &price_file.filepath,
            &base_url,
            &output_fp1.filepath,
            &output_fp2.filepath,
        );
        let pctx = get_ctx(1);
        let ctx = pipeline.prepare_results(pctx, 2).unwrap();
        pipeline.linear(ctx.clone()).unwrap();
        mocks.iter().for_each(|m| {
            m.assert();
        });
        let pxinst = ctx.extract_clone_result("FIN").unwrap();

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
        let mut prices = DummyData::df_instrument_prices();
        let mut instruments = DummyData::df_instruments();
        let price_file = TempFile::default();
        price_file.write_json(&mut prices).unwrap();
        let inst_file = TempFile::default();
        inst_file.write_json(&mut instruments).unwrap();
        let output_fp1 = TempFile::default();
        let output_fp2 = TempFile::default();
        let server = MockServer::start();
        let mocks = mock_server(&server);
        let base_url = server.base_url();
        let pipeline = get_pipeline(
            &inst_file.filepath,
            &price_file.filepath,
            &base_url,
            &output_fp1.filepath,
            &output_fp2.filepath,
        );
        let pctx = get_ctx(3);
        let ctx = pipeline.prepare_results(pctx, 2).unwrap();
        pipeline.sync_exec(ctx.clone()).unwrap();
        mocks.iter().for_each(|m| {
            m.assert();
        });
        let pxinst = ctx.extract_clone_result("FIN").unwrap();

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
        // fern::Dispatch::new().level(log::LevelFilter::Warn).chain(std::io::stdout()).apply().unwrap();
        let mut rt_builder = tokio::runtime::Builder::new_current_thread();
        rt_builder.enable_all();
        let rt = rt_builder.build().unwrap();
        let event = async move || {
            let mut prices = DummyData::df_instrument_prices();
            let mut instruments = DummyData::df_instruments();
            let price_file = TempFile::default();
            price_file.write_json(&mut prices).unwrap();
            let inst_file = TempFile::default();
            inst_file.write_json(&mut instruments).unwrap();
            let output_fp1 = TempFile::default();
            let output_fp2 = TempFile::default();
            let server = MockServer::start();
            let base_url = server.base_url();
            let mocks = mock_server(&server);
            let pipeline = get_pipeline(
                &inst_file.filepath,
                &price_file.filepath,
                &base_url,
                &output_fp1.filepath,
                &output_fp2.filepath,
            );
            let pctx = get_ctx(3).with_signal();
            let ctx = pipeline.prepare_results(pctx, 2).unwrap();

            let ictx = ctx.clone();
            let iictx = ictx.clone();
            let action_path = async move || {
                pipeline.async_exec(ictx).await;
                mocks.iter().for_each(|m| {
                    m.assert();
                });
                let pxinst = ctx.extract_clone_result("FIN").unwrap();

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
