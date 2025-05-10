use std::fmt;

use polars::prelude::*;

use crate::{
    frame::common::{FrameBroadcastHandle, FrameListenHandle},
    pipeline::context::PipelineContext,
    task::stage::Stage,
    util::error::CpResult,
};

pub trait Transform {
    fn run(&self, main: LazyFrame, ctx: Arc<dyn PipelineContext>) -> CpResult<LazyFrame>;
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result;
}

pub struct RootTransformConfig {
    pub label: String,
    pub input: String,
    pub output: String,
}

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
    fn exec(&self, ctx: Arc<dyn PipelineContext>) -> CpResult<()> {
        let lctx = ctx.clone();
        let bctx = ctx.clone();
        let mut input_listener = lctx.get_listener(&self.input, &self.label)?;
        let mut output_broadcast = bctx.get_broadcast(&self.output, &self.label)?;
        let update = input_listener.listen()?;
        let input = update.frame.read()?.clone();
        let output = self.run(input, ctx)?;
        output_broadcast.broadcast(output)
    }
}

impl Transform for RootTransform {
    fn run(&self, main: LazyFrame, ctx: Arc<dyn PipelineContext>) -> CpResult<LazyFrame> {
        let mut next = main;
        for stage in &self.stages {
            next = stage.as_ref().run(next, ctx.clone())?
        }
        Ok(next)
    }
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let _ = write!(f, "{} [ ", &self.label);
        self.stages.iter().for_each(|transform| {
            transform.as_ref().fmt(f).unwrap();
            let _ = write!(f, ", ");
        });
        write!(f, " ]")
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, thread};

    use polars::{df, frame::DataFrame, prelude::IntoLazy};

    use crate::{
        frame::{
            common::{FrameBroadcastHandle, FrameListenHandle, NamedSizedResult},
            polars::PolarsPipelineFrame,
        },
        pipeline::{
            context::{DefaultPipelineContext, PipelineContext},
            results::PipelineResults,
        },
        task::stage::Stage,
    };

    use super::{RootTransform, Transform};

    fn expected() -> DataFrame {
        df!( "a" => [1, 2, 3], "b" => [4, 5, 6] ).unwrap()
    }

    fn build_context() -> Arc<DefaultPipelineContext> {
        let frame = PolarsPipelineFrame::from("orig", 1, expected().lazy());
        let results = PipelineResults::<PolarsPipelineFrame>::from(HashMap::from([(frame.label().to_owned(), frame)]));
        Arc::new(DefaultPipelineContext::from(results))
    }

    #[test]
    fn success_run_no_stages() {
        let ctx = build_context();
        let trf = RootTransform::new("trf", "orig", "actual", Vec::new());
        let expected = trf.run(DataFrame::empty().lazy(), ctx.clone());
        assert_eq!(expected.unwrap().collect().unwrap(), DataFrame::empty());
    }

    #[test]
    fn success_exec_no_stages() {
        let ctx = Arc::new(DefaultPipelineContext::new());
        let lctx = ctx.clone();
        let bctx = ctx.clone();
        let fctx = ctx.clone();
        thread::spawn(move || {
            let mut broadcast = bctx.get_broadcast("orig", "source").unwrap();
            broadcast.broadcast(expected().lazy()).unwrap();
        });
        thread::spawn(move || {
            let trf = RootTransform::new("trf", "orig", "actual", Vec::new());
            trf.exec(lctx).unwrap();
        });
        thread::spawn(move || {
            let mut listener = fctx.get_listener("actual", "dest").unwrap();
            let update = listener.listen().unwrap();
            let lf = update.frame.read().unwrap();
            let actual = lf.clone().collect().unwrap();
            assert_eq!(actual, expected());
        });
    }
}
