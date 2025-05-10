use polars::prelude::*;

use crate::{
    frame::common::{FrameAsyncBroadcastHandle, FrameAsyncListenHandle, FrameBroadcastHandle, FrameListenHandle, FrameUpdateType},
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    task::stage::Stage,
    util::error::CpResult,
};

/// Base transform trait. Takes
pub trait Transform {
    fn run(&self, main: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame>;
}

/// All transforms should come with a config that is serializable/deserializable.
pub struct RootTransformConfig {
    pub label: String,
    pub input: String,
    pub output: String,
    pub stages: Vec<serde_yaml_ng::Value>,
}

/// The root transform node that runs all its substages
pub struct RootTransform {
    label: String,
    input: String,
    output: String,
    stages: Vec<Box<dyn Transform>>,
}

impl RootTransform {
    pub fn new(label: &str, input: &str, output: &str, stages: Vec<Box<dyn Transform>>) -> RootTransform {
        RootTransform {
            label: label.to_string(),
            input: input.to_string(),
            output: output.to_string(),
            stages,
        }
    }
}

impl Stage for RootTransform {
    /// The synchronous, run-once `exec` listens to input for the initial frame, and broadcasts to output
    /// the produced frame
    fn exec(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        let lctx = ctx.clone();
        let bctx = ctx.clone();
        log::info!("Stage initialized: {}", &self.label);
        let mut input_listener = lctx.get_listener(&self.input, &self.label)?;
        let mut output_broadcast = bctx.get_broadcast(&self.output, &self.label)?;
        let update = input_listener.listen()?;
        let input = update.frame.read()?.clone();
        let output = self.run(input, ctx)?;
        output_broadcast.broadcast(output)
    }
    async fn aloop(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<u64> {
        let mut loops = 0;
        let lctx = ctx.clone();
        let bctx = ctx.clone();
        log::info!("Stage initialized: {}", &self.label);
        loop {
            let mut input_listener = lctx.get_async_listener(&self.input, &self.label)?;
            let mut output_broadcast = bctx.get_async_broadcast(&self.output, &self.label)?;
            log::trace!("AWAIT RootTransform handle {}", &self.label);
            let update = input_listener.listen().await?;
            match update.info.msg_type {
                FrameUpdateType::Replace => {
                    let input = update.frame.read()?.clone();
                    let output = self.run(input, ctx.clone())?;
                    log::trace!("BCAST RootTransform handle {} to: {:?}", &self.label, &self.output);
                    output_broadcast.broadcast(output).await?;
                    loops += 1;
                },
                FrameUpdateType::Kill => {
                    log::info!("Stage killed after {} iterations: {}", loops, &self.label);
                    return Ok(loops);
                }
            }
        }
    }
}

impl Transform for RootTransform {
    /// Runs execution in order. ctx is passed to children stages
    /// which can also extract changes on frames
    fn run(&self, main: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        let mut next = main;
        for stage in &self.stages {
            next = stage.as_ref().run(next, ctx.clone())?
        }
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use polars::{df, frame::DataFrame, prelude::IntoLazy};

    use crate::{
        frame::common::{FrameAsyncBroadcastHandle, FrameAsyncListenHandle, FrameBroadcastHandle, FrameListenHandle},
        pipeline::context::{DefaultPipelineContext, PipelineContext},
        task::stage::Stage,
    };

    use super::{RootTransform, Transform};

    fn expected() -> DataFrame {
        df!( "a" => [1, 2, 3], "b" => [4, 5, 6] ).unwrap()
    }

    #[test]
    fn success_run_no_stages() {
        let ctx = Arc::new(DefaultPipelineContext::with_results(&["orig"], 1));
        let trf = RootTransform::new("trf", "orig", "actual", Vec::new());
        let expected = trf.run(DataFrame::empty().lazy(), ctx.clone());
        assert_eq!(expected.unwrap().collect().unwrap(), DataFrame::empty());
    }

    #[test]
    fn success_exec_no_stages() {
        let ctx = Arc::new(DefaultPipelineContext::with_results(&["orig", "actual"], 1));
        let lctx = ctx.clone();
        let bctx = ctx.clone();
        let fctx = ctx.clone();
        let _ = thread::scope(|s| {
            let _b = s.spawn(move || {
                let mut broadcast = bctx.get_broadcast("orig", "source").unwrap();
                broadcast.broadcast(expected().lazy()).unwrap();
            });
            let _t = s.spawn(move || {
                let trf = RootTransform::new("trf", "orig", "actual", Vec::new());
                trf.exec(lctx).unwrap();
            });
            let _s = s.spawn(move || {
                let mut listener = fctx.get_listener("actual", "dest").unwrap();
                let update = listener.listen().unwrap();
                let lf = update.frame.read().unwrap();
                let actual = lf.clone().collect().unwrap();
                assert_eq!(actual, expected());
            });
        });
    }

    #[test]
    fn success_aloop_no_stages() {
        fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let mut rt_builder = tokio::runtime::Builder::new_current_thread();
        rt_builder.enable_all();
        let rt = rt_builder.build().unwrap();
        let event = async || {
            let ctx = Arc::new(DefaultPipelineContext::with_results(&["orig", "actual"], 1));
            let lctx = ctx.clone();
            let bctx = ctx.clone();
            let fctx = ctx.clone();
            let expected = || df!( "a" => [1, 2, 3], "b" => [4, 5, 6] ).unwrap();
            let bhandle = async move || {
                let mut broadcast = bctx.get_async_broadcast("orig", "source").unwrap();
                broadcast.broadcast(expected().lazy()).await.unwrap();
            };
            let lhandle = async move || {
                let trf = RootTransform::new("trf", "orig", "actual", Vec::new());
                assert_eq!(trf.aloop(lctx).await.unwrap(), 1);
            };
            let thandle = async move || {
                let mut listener = fctx.get_async_listener("actual", "killer").unwrap();
                let mut killer = fctx.get_async_broadcast("orig", "killer").unwrap();
                log::debug!("AWAIT handle killer");
                let update = listener.listen().await.unwrap();
                let lf = update.frame.read().unwrap();
                let actual = lf.clone().collect().unwrap();
                assert_eq!(actual, expected());
                println!("actual {:?}", actual);
                killer.kill().await.unwrap();
                println!("killed");
            };
            tokio::join!(bhandle(), lhandle(), thandle());
        };
        rt.block_on(event());
    }
    /*
    */
}
