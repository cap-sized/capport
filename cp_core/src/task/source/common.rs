use std::sync::Arc;

use async_trait::async_trait;
use crossbeam::thread;
use polars::prelude::LazyFrame;

use crate::{
    ctx_run_n_async, ctx_run_n_threads,
    frame::common::{FrameAsyncBroadcastHandle, FrameBroadcastHandle, FrameUpdateType},
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    task::stage::Stage,
    util::error::{CpError, CpResult},
};

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
    fn validate(&mut self, ctx: Arc<DefaultPipelineContext>, context: &serde_yaml_ng::Mapping) -> Vec<CpError>;
    fn transform(&self, ctx: Arc<DefaultPipelineContext>) -> Box<dyn Source>;
}

/// We NEVER modify the individual Source instantiations after initialization.
/// Hence the Box<dyn Source> is safe to access in parallel
unsafe impl Send for BoxedSource {}
unsafe impl Sync for BoxedSource {}

pub struct RootSource {
    label: String,
    max_threads: usize,
    sources: Vec<BoxedSource>,
}

impl RootSource {
    pub fn new(label: &str, max_threads: usize, sources: Vec<Box<dyn Source>>) -> RootSource {
        RootSource {
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

impl Stage for RootSource {
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
                ctx.clone(),
                move |sources, ictx: Arc<DefaultPipelineContext>| {
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
                }
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
        let signal = ctx.signal_propagator();
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use polars::{
        df,
        frame::DataFrame,
        functions::concat_df_horizontal,
        prelude::{IntoLazy, LazyFrame, UnionArgs, concat_lf_horizontal},
    };

    use crate::{
        frame::common::{FrameAsyncBroadcastHandle, FrameAsyncListenHandle, FrameBroadcastHandle, FrameListenHandle},
        pipeline::context::{DefaultPipelineContext, PipelineContext},
        task::{source::common::RootSource, stage::Stage},
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
        let src = RootSource::new("root", 1, vec![Box::new(mock_src)]);
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
        let src = RootSource::new("root", 1, vec![Box::new(mock_src_next), Box::new(mock_src)]);
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
            let src = RootSource::new(
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
            let src = RootSource::new(
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
}
