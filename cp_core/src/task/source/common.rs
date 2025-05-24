use std::sync::Arc;

use async_trait::async_trait;
use crossbeam::thread;
use polars::prelude::LazyFrame;

use crate::{
    ctx_run_n_async, ctx_run_n_threads,
    frame::common::{FrameAsyncBroadcastHandle, FrameBroadcastHandle, FrameUpdateType},
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    task::stage::{Stage, StageTaskConfig},
    try_deserialize_transform,
    util::error::{CpError, CpResult},
};

use super::config::{JsonSourceConfig, SourceGroupConfig};

/// Base source trait. Importantly, certain sources may have dependencies as well.
/// If it receives a termination signal, it is the source type's responsibility to clean up and
/// kill its dependents as well.
#[async_trait]
pub trait Source {
    fn connection_type(&self) -> &str;
    fn name(&self) -> &str;
    fn run(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame>;
    async fn fetch(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame>;
}

pub struct BoxedSource(Box<dyn Source>);

pub trait SourceConfig {
    fn emplace(&mut self, ctx: Arc<DefaultPipelineContext>, context: &serde_yaml_ng::Mapping) -> CpResult<()>;
    fn validate(&self) -> Vec<CpError>;
    fn transform(&self) -> Box<dyn Source>;
}

/// We NEVER modify the individual Source instantiations after initialization.
/// Hence the Box<dyn Source> is safe to access in parallel
unsafe impl Send for BoxedSource {}
unsafe impl Sync for BoxedSource {}

/// Unlike RootTransform, SourceGroup/RootSink do not implement Source/Sink respectively
/// Their stage interface is run directly without calling SourceGroup
/// TODO: change naming to SourceGroup
pub struct SourceGroup {
    label: String,
    max_threads: usize,
    sources: Vec<BoxedSource>,
}

impl SourceGroup {
    pub fn new(label: &str, max_threads: usize, sources: Vec<Box<dyn Source>>) -> SourceGroup {
        SourceGroup {
            label: label.to_owned(),
            max_threads,
            sources: sources.into_iter().map(BoxedSource).collect::<Vec<BoxedSource>>(),
        }
    }
}

fn run_source(label: &str, bsource: &BoxedSource, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
    let source = &bsource.0;
    let mut bcast = ctx.get_broadcast(source.name(), label)?;
    log::info!("Fetching frame from {}: {}", source.connection_type(), source.name());
    let result = source.run(ctx.clone())?;
    bcast.broadcast(result)?;
    log::info!(
        "Success fetching frame from {}: {}",
        source.connection_type(),
        source.name()
    );
    Ok(())
}

impl Stage for SourceGroup {
    fn linear(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        log::info!("Stage initialized [single-thread]: {}", &self.label);
        for source in &self.sources {
            run_source(&self.label, source, ctx.clone())?;
        }
        Ok(())
    }
    /// WARNING: This method shouldn't be chosen to run concurrently any source tasks
    /// that might have dependencies on each other: if they are scheduled out of order
    /// on the same thread execution will definitely be blocked. If any ordering is
    /// required, use separate source tasks so execution reliably runs.
    fn sync_exec(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        log::info!(
            "Stage initialized [max_threads: {}]: {}",
            &self.label,
            &self.max_threads
        );
        if self.max_threads > 1 {
            let label = self.label.as_str();
            ctx_run_n_threads!(
                self.max_threads,
                self.sources.as_slice(),
                move |(sources, ictx): (_, Arc<DefaultPipelineContext>)| {
                    for source in sources {
                        let s: &BoxedSource = source;
                        match run_source(label, s, ictx.clone()) {
                            Ok(_) => {}
                            Err(e) => log::error!(
                                "Failed fetch frame `{}` of type `{}`: {:?}",
                                &s.0.name(),
                                &s.0.connection_type(),
                                e
                            ),
                        };
                    }
                },
                ctx.clone()
            );
        } else {
            for source in &self.sources {
                run_source(&self.label, source, ctx.clone())?;
            }
        }
        Ok(())
    }
    /// async exec should NOT fail if a connection fails. it should just log the error and poll again.
    /// suitable for running concurrently even tasks that have dependencies on each other, but for
    /// clarity this is unadvised.
    async fn async_exec(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<u64> {
        log::info!("Stage initialized [async fetch]: {}", &self.label);
        let label = self.label.as_str();
        let mut loops: u64 = 0;
        let mut signal = ctx.signal_propagator();
        loop {
            // Source tasks do not fetch until an explicit "Replace" signal has been received.
            // i.e. the scheduler has to send the signal
            match signal.recv().await {
                Ok(x) => match x.msg_type {
                    FrameUpdateType::Kill => {
                        log::info!("Terminating source stage `{}`...", self.label);
                        ctx_run_n_async!(label, &self.sources, ctx.clone(), async |source: &BoxedSource,
                                                                                   ctx: Arc<
                            DefaultPipelineContext,
                        >| {
                            let ictx = ctx.clone();
                            let mut bcast = match ictx.get_async_broadcast(source.0.name(), label) {
                                Ok(x) => x,
                                Err(e) => {
                                    return Err(CpError::PipelineError("Broadcast channel failed", e.to_string()));
                                }
                            };
                            bcast.kill().await?;
                            log::info!("Sent termination signal for frame {}", source.0.name());
                            Ok(())
                        });
                        loops += 1;
                        break;
                    }
                    FrameUpdateType::Replace => {
                        ctx_run_n_async!(label, &self.sources, ctx.clone(), async |source: &BoxedSource,
                                                                                   ctx: Arc<
                            DefaultPipelineContext,
                        >| {
                            let ictx = ctx.clone();
                            let mut bcast = match ictx.get_async_broadcast(source.0.name(), label) {
                                Ok(x) => x,
                                Err(e) => {
                                    return Err(CpError::PipelineError("Broadcast channel failed", e.to_string()));
                                }
                            };
                            match source.0.fetch(ctx.clone()).await {
                                Ok(lf) => {
                                    bcast.broadcast(lf).await?;
                                    log::info!("Sent update for frame {}", source.0.name());
                                    Ok(())
                                }
                                Err(e) => Err(CpError::PipelineError("Fetch source failed", e.to_string())),
                            }
                        });
                        loops += 1;
                    }
                },
                Err(e) => {
                    log::warn!("Error while receiving signal: {:?}", e)
                }
            }
        }

        Ok(loops)
    }
}

impl SourceGroupConfig {
    fn parse_subsources(&self) -> Vec<Result<Box<dyn SourceConfig>, CpError>> {
        self.sources
            .iter()
            .map(|transform| {
                let config = try_deserialize_transform!(transform, dyn SourceConfig, JsonSourceConfig);
                config.ok_or_else(|| {
                    CpError::ConfigError(
                        "Source config parsing error",
                        format!("Failed to parse source config: {:?}", transform),
                    )
                })
            })
            .collect()
    }
}

impl StageTaskConfig<SourceGroup> for SourceGroupConfig {
    fn parse(
        &self,
        ctx: Arc<DefaultPipelineContext>,
        context: &serde_yaml_ng::Mapping,
    ) -> Result<SourceGroup, Vec<CpError>> {
        let mut subsources = vec![];
        let mut errors = vec![];
        for result in self.parse_subsources() {
            match result {
                Ok(mut config) => {
                    if let Err(e) = config.emplace(ctx.clone(), context) {
                        errors.push(e);
                    }
                    let errs = config.validate();
                    if errs.is_empty() {
                        subsources.push(BoxedSource(config.transform()));
                    } else {
                        errors.extend(errs);
                    }
                }
                Err(e) => errors.push(e),
            }
        }
        if errors.is_empty() {
            Ok(SourceGroup {
                label: self.label.clone(),
                max_threads: self.max_threads,
                sources: subsources,
            })
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use async_trait::async_trait;
    use polars::{
        df,
        frame::DataFrame,
        functions::concat_df_horizontal,
        prelude::{IntoLazy, LazyFrame, UnionArgs, concat_lf_horizontal},
    };

    use crate::{
        context::model::ModelRegistry,
        frame::common::{FrameAsyncBroadcastHandle, FrameAsyncListenHandle, FrameBroadcastHandle, FrameListenHandle},
        model::common::ModelConfig,
        pipeline::context::{DefaultPipelineContext, PipelineContext},
        task::{
            source::{common::SourceGroup, config::SourceGroupConfig},
            stage::{Stage, StageTaskConfig},
        },
        util::error::CpResult,
    };

    use super::Source;

    struct MockSource {
        out: String,
        dep: Vec<String>,
    }

    impl MockSource {
        fn new(out: &str) -> Self {
            Self {
                out: out.to_string(),
                dep: vec![],
            }
        }
        fn from(out: &str, dep: &[&str]) -> Self {
            Self {
                out: out.to_string(),
                dep: dep.iter().map(|x| x.to_string()).collect(),
            }
        }
    }

    fn default_df() -> DataFrame {
        df!( "a" => [1, 2, 3], "b" => [4, 5, 6] ).unwrap()
    }

    fn default_next() -> DataFrame {
        df!( "c" => ["a", "b", "c"], "d" => [4, 5, 6] ).unwrap()
    }

    #[async_trait]
    impl Source for MockSource {
        fn connection_type(&self) -> &str {
            "mock"
        }
        fn name(&self) -> &str {
            &self.out
        }
        fn run(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
            if self.dep.is_empty() {
                Ok(default_df().lazy())
            } else {
                let mut collected: Vec<LazyFrame> = vec![];
                for x in &self.dep {
                    let mut x = ctx.get_listener(x, self.name()).unwrap();
                    let update = x.listen().unwrap();
                    let frame = update.frame.read()?;
                    collected.push(frame.clone());
                }
                let result = concat_lf_horizontal(
                    collected,
                    UnionArgs {
                        parallel: false,
                        ..Default::default()
                    },
                )
                .unwrap();
                Ok(result)
            }
        }
        async fn fetch(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
            if self.dep.is_empty() {
                Ok(default_df().lazy())
            } else {
                let mut collected: Vec<LazyFrame> = vec![];
                for x in &self.dep {
                    let mut x = ctx.get_async_listener(x, self.name()).unwrap();
                    let update = x.listen().await.unwrap();
                    let frame = update.frame.read()?;
                    collected.push(frame.clone());
                }
                let result = concat_lf_horizontal(
                    collected,
                    UnionArgs {
                        parallel: false,
                        ..Default::default()
                    },
                )
                .unwrap();
                Ok(result)
            }
        }
    }

    #[test]
    fn success_mock_source_run() {
        let ctx = Arc::new(DefaultPipelineContext::new());
        let src = MockSource::new("mock");
        let expected = src.run(ctx.clone());
        assert_eq!(expected.unwrap().collect().unwrap(), default_df());
    }

    #[test]
    fn success_mock_source_linear_exec() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let ctx = Arc::new(DefaultPipelineContext::with_results(&["df", "next", "mock_source"], 1));
        let mut df_handle = ctx.get_broadcast("df", "orig").unwrap();
        let mut next_handle = ctx.get_broadcast("next", "orig").unwrap();
        df_handle.broadcast(default_df().lazy()).unwrap();
        next_handle.broadcast(default_next().lazy()).unwrap();
        let mock_src = MockSource::from("mock_source", &["df", "next"]);
        let src = SourceGroup::new("root", 1, vec![Box::new(mock_src)]);
        src.linear(ctx.clone()).unwrap();
        let actual = concat_df_horizontal(&[default_df(), default_next()], true).unwrap();
        assert_eq!(ctx.extract_clone_result("mock_source").unwrap(), actual);
    }

    #[test]
    fn success_mock_source_sync_exec_single_thread() {
        let ctx = Arc::new(DefaultPipelineContext::with_results(
            &["df", "next", "test_df", "test_next"],
            1,
        ));
        let mut next_handle = ctx.get_broadcast("next", "orig").unwrap();
        next_handle.broadcast(default_next().lazy()).unwrap();
        let mock_src_next = MockSource::from("test_next", &["next"]);
        let mock_src = MockSource::new("df");
        // mock_src depends on mock_src_df. If max_threads is any less than 3,
        // there is a chance this blocks.
        let src = SourceGroup::new("root", 1, vec![Box::new(mock_src_next), Box::new(mock_src)]);
        // This will NOT work with linear! mock_src depends on mock_src_df
        src.sync_exec(ctx.clone()).unwrap();
        assert_eq!(ctx.extract_clone_result("df").unwrap(), default_df());
        assert_eq!(ctx.extract_clone_result("test_next").unwrap(), default_next());
    }

    #[test]
    fn success_mock_source_sync_exec() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let count = [3, 4];
        for thread_count in count {
            let ctx = Arc::new(DefaultPipelineContext::with_results(
                &["df", "next", "test_df", "test_next"],
                1,
            ));
            let mut next_handle = ctx.get_broadcast("next", "orig").unwrap();
            next_handle.broadcast(default_next().lazy()).unwrap();
            let mock_src_df = MockSource::from("test_df", &["df"]);
            let mock_src_next = MockSource::from("test_next", &["next"]);
            let mock_src = MockSource::new("df");
            // mock_src depends on mock_src_df. If max_threads is any less than 3,
            // there is a chance this blocks.
            let src = SourceGroup::new(
                "root",
                thread_count,
                vec![Box::new(mock_src_df), Box::new(mock_src_next), Box::new(mock_src)],
            );
            // This will NOT work with linear! mock_src depends on mock_src_df
            src.sync_exec(ctx.clone()).unwrap();
            assert_eq!(ctx.extract_clone_result("df").unwrap(), default_df());
            assert_eq!(ctx.extract_clone_result("test_next").unwrap(), default_next());
            assert_eq!(ctx.extract_clone_result("test_df").unwrap(), default_df());
        }
    }

    #[test]
    fn success_mock_source_async_exec() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let mut rt_builder = tokio::runtime::Builder::new_current_thread();
        rt_builder.enable_all();
        let rt = rt_builder.build().unwrap();
        let event = async || {
            let ctx = Arc::new(
                DefaultPipelineContext::with_results(&["df", "next", "test_df", "test_next"], 2).with_signal(),
            );
            let ictx = ctx.clone();
            let mut next_handle = ctx.get_async_broadcast("next", "orig").unwrap();
            next_handle.broadcast(default_next().lazy()).await.unwrap();
            let mock_src_df = MockSource::from("test_df", &["df"]);
            let mock_src_next = MockSource::from("test_next", &["next"]);
            let mock_src = MockSource::new("df");
            let src = SourceGroup::new(
                "root",
                3,
                vec![Box::new(mock_src_df), Box::new(mock_src_next), Box::new(mock_src)],
            );
            let action_path = async move || {
                // This will NOT work with linear! mock_src depends on mock_src_df
                src.async_exec(ctx.clone()).await.unwrap();
                assert_eq!(ctx.extract_clone_result("df").unwrap(), default_df());
                assert_eq!(ctx.extract_clone_result("test_next").unwrap(), default_next());
                assert_eq!(ctx.extract_clone_result("test_df").unwrap(), default_df());
            };
            let terminator = async move || {
                match ictx.signal_replace().await {
                    Ok(_) => log::info!("Replace signal successfully sent"),
                    Err(e) => log::error!("Error signalling replace: {}", e),
                };
                match ictx.signal_terminate().await {
                    Ok(_) => log::info!("Termination successfully sent"),
                    Err(e) => log::error!("Error terminating: {}", e),
                };
            };
            tokio::join!(action_path(), terminator());
        };
        rt.block_on(event());
    }

    #[test]
    fn create_source_group_good_config() {
        let configs_str = "
- json:
    filepath: fp
    output: SAMPLE3
- json:
    filepath: fp
    output: SAMPLE
    model: test
- json:
    filepath: fp
    output: $output
    model: test
- json:
    filepath: fp
    output: $output
    model: test
    model_fields: 
        aaa: int64
        bbb: str
";
        let configs = serde_yaml_ng::from_str::<Vec<serde_yaml_ng::Value>>(configs_str).unwrap();
        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>("output: SAMPLE2").unwrap();
        let sgconfig = SourceGroupConfig {
            label: "".to_owned(),
            max_threads: 1,
            sources: configs,
        };
        let mut model_registry = ModelRegistry::new();
        model_registry.insert(ModelConfig {
            label: "test".to_owned(),
            fields: HashMap::new(),
        });
        let ctx = Arc::new(DefaultPipelineContext::new().with_model_registry(model_registry));
        let actual = sgconfig.parse(ctx, &context).unwrap();
        assert_eq!(actual.sources.len(), 4);
    }

    #[test]
    fn create_source_group_bad_configs() {
        [
            "
- bad:
    filepath: fp
    output: $output
    model: test
",
            "
- csv:
    filepath: fp
    output: output
    model: not_a_test
",
            "
- json:
    filepath: fp
    output: $output2
    model: not_a_test
",
        ]
        .iter()
        .for_each(|configs_str| {
            let configs = serde_yaml_ng::from_str::<Vec<serde_yaml_ng::Value>>(configs_str).unwrap();
            let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>("output: SAMPLE2").unwrap();
            let sgconfig = SourceGroupConfig {
                label: "".to_owned(),
                max_threads: 1,
                sources: configs,
            };
            let mut model_registry = ModelRegistry::new();
            model_registry.insert(ModelConfig {
                label: "test".to_owned(),
                fields: HashMap::new(),
            });
            let ctx = Arc::new(DefaultPipelineContext::new().with_model_registry(model_registry));
            let actual = sgconfig.parse(ctx, &context);
            assert!(actual.is_err());
        });
    }
}
