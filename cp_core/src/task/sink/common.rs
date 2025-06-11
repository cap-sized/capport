use std::sync::{Arc, atomic::AtomicUsize};

use async_trait::async_trait;
use polars::{frame::DataFrame, prelude::LazyFrame};

use crate::{
    ctx_run_n_async, ctx_run_n_threads,
    frame::{
        common::{FrameAsyncListenHandle, FrameListenHandle, FrameUpdate, FrameUpdateType},
        polars::PolarsAsyncListenHandle,
    },
    parser::{keyword::Keyword, merge_type::MergeTypeEnum},
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    task::stage::{Stage, StageTaskConfig},
    try_deserialize_stage,
    util::error::{CpError, CpResult},
    valid_or_insert_error,
};

use super::config::{ClickhouseSinkConfig, CsvSinkConfig, JsonSinkConfig, SinkGroupConfig};

/// Base sink trait. Importantly, certain sinks may have dependencies as well.
/// If it receives a termination signal, it is the sink type's responsibility to clean up and
/// kill its dependents as well.
/// Is a syntactic clone of the sink trait, but do semantically different things.
#[async_trait]
pub trait Sink {
    fn connection_type(&self) -> &str;
    fn run(&self, frame: DataFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<()>;
    async fn fetch(&self, frame: DataFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<()>;
}

pub struct BoxedSink(Box<dyn Sink>);

pub trait SinkConfig {
    fn emplace(&mut self, ctx: &DefaultPipelineContext, context: &serde_yaml_ng::Mapping) -> CpResult<()>;
    fn validate(&self) -> Vec<CpError>;
    fn transform(&self) -> Box<dyn Sink>;
}

/// We NEVER modify the individual Sink instantiations after initialization.
/// Hence the Box<dyn Sink> is safe to access in parallel
unsafe impl Send for BoxedSink {}
unsafe impl Sync for BoxedSink {}

pub struct SinkOptions {
    pub merge_type: MergeTypeEnum,
}

pub struct SinkGroup {
    result_name: String,
    label: String,
    max_threads: usize,
    sinks: Vec<BoxedSink>,
}

impl SinkGroup {
    pub fn new(result_name: &str, label: &str, max_threads: usize, sinks: Vec<Box<dyn Sink>>) -> SinkGroup {
        SinkGroup {
            result_name: result_name.to_owned(),
            label: label.to_owned(),
            max_threads,
            sinks: sinks.into_iter().map(BoxedSink).collect::<Vec<BoxedSink>>(),
        }
    }
}

impl Stage for SinkGroup {
    fn linear(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        log::info!("Stage initialized [single-thread]: {}", &self.label);
        let dataframe = ctx.extract_clone_result(&self.result_name).expect("sink");
        log::info!("INPUT `{}`: {:?}", &self.label, dataframe);
        for sink in &self.sinks {
            sink.0.run(dataframe.clone(), ctx.clone())?;
            log::info!(
                "Success pushing frame update via {}: {}",
                sink.0.connection_type(),
                &self.result_name
            );
        }
        Ok(())
    }
    /// WARNING: sync_exec mode only runs EACH STAGE possibly multithreaded.
    /// It does not run the different stages on multiple threads.
    fn sync_exec(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        log::info!(
            "Stage initialized [max_threads: {}]: {}",
            &self.max_threads,
            &self.label,
        );
        let result_name = self.result_name.as_str();
        let label = self.label.as_str();
        let mut listen = ctx.get_listener(result_name, label)?;
        let update = listen.force_listen();
        let mut dataframe: Option<DataFrame> = None;
        {
            let fread = update.frame.read()?;
            let _ = dataframe.insert(fread.clone().collect()?);
        }
        ctx_run_n_threads!(
            self.max_threads,
            self.sinks.as_slice(),
            move |(sinks, ictx, dfh): (_, Arc<DefaultPipelineContext>, Option<DataFrame>)| {
                for sink in sinks {
                    let s: &BoxedSink = sink;
                    let sink = &s.0;
                    match sink.run(dfh.clone().expect("sink.df"), ictx.clone()) {
                        Ok(_) => log::info!(
                            "pushed frame `{}` on connection `{}`",
                            result_name,
                            &s.0.connection_type(),
                        ),
                        Err(e) => log::error!(
                            "Failed fetch frame `{}` of type `{}`: {:?}",
                            result_name,
                            &s.0.connection_type(),
                            e
                        ),
                    };
                }
            },
            ctx,
            dataframe
        );
        Ok(())
    }

    async fn async_exec(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<u64> {
        log::info!("Stage initialized [async fetch]: {}", &self.label);
        let label = self.label.as_str();
        let result_name = self.result_name.as_str();
        let mut listen = ctx.get_async_listener(result_name, label)?;
        let lp: *mut PolarsAsyncListenHandle = &mut listen;
        let mut loops: u64 = 0;
        let terminations = AtomicUsize::new(0);
        loop {
            // unsafe hack to get past the borrow checker
            let update: FrameUpdate<LazyFrame> = unsafe { (*lp).listen().await? };
            ctx_run_n_async!(
                label,
                &self.sinks,
                async |bsink: &BoxedSink, ctx: Arc<DefaultPipelineContext>| {
                    let sink = &bsink.0;
                    let upd = update.clone();
                    match upd.info.msg_type {
                        FrameUpdateType::Replace => {
                            let mut dataframe: Option<DataFrame> = None;
                            {
                                let fread = update.frame.read()?;
                                match fread.clone().collect() {
                                    Ok(x) => {
                                        let _ = dataframe.insert(x);
                                    }
                                    Err(e) => log::error!("{}", e),
                                }
                            }
                            if let Some(frame) = dataframe.take() {
                                sink.fetch(frame, ctx.clone()).await?;
                                log::info!(
                                    "Success pushing frame update via {}: {}",
                                    sink.connection_type(),
                                    result_name
                                );
                            }
                        }
                        FrameUpdateType::Kill => {
                            let prev = terminations.fetch_add(1, std::sync::atomic::Ordering::Release);
                            log::info!(
                                "Terminating sink substage `{}.{}` [{}/{}]",
                                &self.label,
                                sink.connection_type(),
                                prev + 1,
                                self.sinks.len()
                            );
                        }
                    }
                    Ok::<(), CpError>(())
                },
                ctx.clone()
            );
            let tcount = terminations.load(std::sync::atomic::Ordering::Acquire);
            if tcount >= self.sinks.len() {
                log::info!("Terminating sink stage `{}` after {} iterations", &self.label, loops);
                break;
            }
            loops += 1;
        }
        Ok(loops)
    }
}

impl SinkGroupConfig {
    fn parse_subsinks(&self) -> Vec<Result<Box<dyn SinkConfig>, CpError>> {
        self.sinks
            .iter()
            .map(|transform| {
                let config = try_deserialize_stage!(
                    transform,
                    dyn SinkConfig,
                    CsvSinkConfig,
                    ClickhouseSinkConfig,
                    JsonSinkConfig
                );
                config.ok_or_else(|| {
                    CpError::ConfigError(
                        "Sink config parsing error",
                        format!("Failed to parse sink config: {:?}", transform),
                    )
                })
            })
            .collect()
    }
}

impl StageTaskConfig<SinkGroup> for SinkGroupConfig {
    fn parse(&self, ctx: &DefaultPipelineContext, context: &serde_yaml_ng::Mapping) -> Result<SinkGroup, Vec<CpError>> {
        let mut subsinks = vec![];
        let mut errors = vec![];
        for result in self.parse_subsinks() {
            match result {
                Ok(mut config) => {
                    if let Err(e) = config.emplace(ctx, context) {
                        errors.push(e);
                    }
                    let errs = config.validate();
                    if errs.is_empty() {
                        subsinks.push(BoxedSink(config.transform()));
                    } else {
                        errors.extend(errs);
                    }
                }
                Err(e) => errors.push(e),
            }
        }
        let mut input = self.input.clone();
        let _ = input.insert_value_from_context(context);
        valid_or_insert_error!(errors, input, "sink.input");
        if errors.is_empty() {
            Ok(SinkGroup {
                label: self.label.clone(),
                result_name: input.value().expect("sink.input").to_owned(),
                max_threads: self.max_threads,
                sinks: subsinks,
            })
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, thread::sleep, time::Duration};

    use async_trait::async_trait;
    use polars::{
        df,
        frame::DataFrame,
        io::SerReader,
        prelude::{CsvReader, IntoLazy},
    };

    use crate::{
        async_st,
        context::model::ModelRegistry,
        frame::common::{FrameAsyncBroadcastHandle, FrameBroadcastHandle},
        model::common::ModelConfig,
        parser::keyword::{Keyword, StrKeyword},
        pipeline::context::{DefaultPipelineContext, PipelineContext},
        task::{
            sink::{common::SinkGroup, config::SinkGroupConfig},
            stage::{Stage, StageTaskConfig},
        },
        util::{error::CpResult, test::assert_frame_equal, tmp::TempFile},
    };

    use super::Sink;

    struct MockSink {
        out: String,
    }

    impl MockSink {
        fn new(out: &str) -> Self {
            Self { out: out.to_owned() }
        }
    }

    fn default_df() -> DataFrame {
        df!( "a" => [1, 2, 3], "b" => [4, 5, 6] ).unwrap()
    }

    fn default_next() -> DataFrame {
        df!( "c" => ["a", "b", "c"], "d" => [4, 5, 6] ).unwrap()
    }

    #[async_trait]
    impl Sink for MockSink {
        fn connection_type(&self) -> &str {
            "mock"
        }
        fn run(&self, frame: DataFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
            log::info!("DONE\n{:?}", frame);
            let lf = frame.lazy();
            sleep(Duration::new(1, 0));
            ctx.insert_result(self.out.as_str(), lf.clone()).unwrap();
            Ok(())
        }
        async fn fetch(&self, frame: DataFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
            log::info!("DONE\n{:?}", frame);
            let lf = frame.lazy();
            ctx.insert_result(self.out.as_str(), lf.clone()).unwrap();
            Ok(())
        }
    }

    #[test]
    fn success_mock_sink_run() {
        let ctx = Arc::new(DefaultPipelineContext::with_results(&["test"], 1));
        let src = MockSink::new("test");
        let _ = src.run(default_df(), ctx.clone());
        let actual = ctx.extract_clone_result("test").unwrap();
        assert_eq!(actual, default_df());
    }

    #[test]
    fn success_mock_sink_linear_exec() {
        let ctx = Arc::new(DefaultPipelineContext::with_results(&["df", "next1", "next2"], 1));
        let mut df_handle = ctx.get_broadcast("df", "orig").unwrap();
        df_handle.broadcast(default_df().lazy()).unwrap();
        let src = SinkGroup::new(
            "df",
            "msink",
            1,
            vec![Box::new(MockSink::new("next1")), Box::new(MockSink::new("next2"))],
        );
        src.linear(ctx.clone()).unwrap();
        assert_eq!(ctx.extract_clone_result("next1").unwrap(), default_df());
        assert_eq!(ctx.extract_clone_result("next2").unwrap(), default_df());
    }

    #[test]
    fn success_mock_sink_sync_exec_single_thread() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let ctx = Arc::new(DefaultPipelineContext::with_results(&["df", "next1", "next2"], 2));
        let mut df_handle = ctx.get_broadcast("df", "orig").unwrap();
        df_handle.broadcast(default_df().lazy()).unwrap();
        let snk = SinkGroup::new(
            "df",
            "msink",
            1,
            vec![Box::new(MockSink::new("next1")), Box::new(MockSink::new("next2"))],
        );
        snk.sync_exec(ctx.clone()).unwrap();
        assert_eq!(ctx.extract_clone_result("next1").unwrap(), default_df());
        assert_eq!(ctx.extract_clone_result("next2").unwrap(), default_df());
    }

    #[test]
    fn success_mock_sink_sync_exec_multi_thread() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let ctx = Arc::new(DefaultPipelineContext::with_results(
            &["df", "next1", "next2", "next3"],
            2,
        ));
        let mut df_handle = ctx.get_broadcast("df", "orig").unwrap();
        df_handle.broadcast(default_df().lazy()).unwrap();
        let snk = SinkGroup::new(
            "df",
            "msink",
            3,
            vec![
                Box::new(MockSink::new("next1")),
                Box::new(MockSink::new("next2")),
                Box::new(MockSink::new("next3")),
            ],
        );
        snk.sync_exec(ctx.clone()).unwrap();
        assert_eq!(ctx.extract_clone_result("next1").unwrap(), default_df());
        assert_eq!(ctx.extract_clone_result("next2").unwrap(), default_df());
        assert_eq!(ctx.extract_clone_result("next3").unwrap(), default_df());
    }

    #[test]
    fn success_mock_sink_async_exec() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        async_st!(async || {
            let ctx =
                Arc::new(DefaultPipelineContext::with_results(&["next", "next1", "next2", "next3"], 2).with_signal(2));
            let ictx = ctx.clone();
            let mnext1 = Box::new(MockSink::new("next1"));
            let mnext2 = Box::new(MockSink::new("next2"));
            let mnext3 = Box::new(MockSink::new("next3"));
            let snk = SinkGroup::new("next", "msink", 1, vec![mnext1, mnext2, mnext3]);
            let action_path = async move || {
                // This will NOT work with linear! mock_src depends on mock_src_df
                snk.async_exec(ictx.clone()).await.unwrap();
                assert_eq!(ictx.extract_clone_result("next1").unwrap(), default_next());
                assert_eq!(ictx.extract_clone_result("next2").unwrap(), default_next());
                assert_eq!(ictx.extract_clone_result("next3").unwrap(), default_next());
            };
            let terminator = async move || {
                let mut next_handle = ctx.get_async_broadcast("next", "orig").unwrap();
                next_handle.broadcast(default_next().lazy()).unwrap();
                next_handle.kill().unwrap();
            };
            tokio::join!(action_path(), terminator());
        });
    }

    #[test]
    fn create_sink_group_good_config() {
        let configs_str = "
- csv:
    filepath: /fp
    merge_type: make_next
- csv:
    filepath: $replace
    merge_type: make_next
";
        let configs = serde_yaml_ng::from_str::<Vec<serde_yaml_ng::Value>>(configs_str).unwrap();
        let context =
            serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>("{replace: /filepaaath, input: SAMPLE}").unwrap();
        let sgconfig = SinkGroupConfig {
            label: "".to_owned(),
            input: StrKeyword::with_symbol("input"),
            max_threads: 1,
            sinks: configs,
        };
        let mut model_registry = ModelRegistry::new();
        model_registry.insert(ModelConfig {
            label: "SAMPLE".to_owned(),
            fields: HashMap::new(),
        });
        let ctx = Arc::new(DefaultPipelineContext::new().with_model_registry(model_registry));
        let actual = sgconfig.parse(&ctx, &context);
        actual.unwrap();
    }

    #[test]
    fn create_sink_group_bad_configs() {
        [
            "
- bad:
    filepath: /fp
",
            "
- csv:
    filepath: $notoutput
",
        ]
        .iter()
        .for_each(|configs_str| {
            let configs = serde_yaml_ng::from_str::<Vec<serde_yaml_ng::Value>>(configs_str).unwrap();
            let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>("output: SAMPLE2").unwrap();
            let sgconfig = SinkGroupConfig {
                label: "".to_owned(),
                input: StrKeyword::with_value("input".to_owned()),
                max_threads: 1,
                sinks: configs,
            };
            let mut model_registry = ModelRegistry::new();
            model_registry.insert(ModelConfig {
                label: "input".to_owned(),
                fields: HashMap::new(),
            });
            let ctx = Arc::new(DefaultPipelineContext::new().with_model_registry(model_registry));
            let actual = sgconfig.parse(&ctx, &context);
            assert!(actual.is_err());
        });
    }

    #[test]
    fn valid_sink_group_basic() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let configs_str = "
- csv:
    filepath: $fp1
    merge_type: insert
- csv:
    filepath: $fp2
    merge_type: insert
";
        let configs = serde_yaml_ng::from_str::<Vec<serde_yaml_ng::Value>>(configs_str).unwrap();
        let tmp1 = TempFile::default();
        let tmp2 = TempFile::default();
        let context_str = format!(
            "
fp1: {}
fp2: {}
",
            &tmp1.filepath, &tmp2.filepath
        );
        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>(&context_str).unwrap();
        let sgconfig = SinkGroupConfig {
            label: "".to_owned(),
            input: StrKeyword::with_value("SAMPLE".to_owned()),
            max_threads: 1,
            sinks: configs,
        };
        let ctx = Arc::new(DefaultPipelineContext::with_results(&["SAMPLE"], 1).with_executing_sink(true));
        let mut bcast = ctx.get_broadcast("SAMPLE", "main").unwrap();
        bcast.broadcast(default_next().lazy()).unwrap();
        let actual = sgconfig.parse(&ctx, &context).unwrap();
        actual.linear(ctx.clone()).unwrap();
        for tmp in [tmp1, tmp2] {
            let buffer = tmp.get().unwrap();
            let reader = CsvReader::new(buffer);
            assert_frame_equal(reader.finish().unwrap(), default_next());
        }
    }
}
