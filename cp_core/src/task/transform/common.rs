use polars::prelude::*;

use super::config::{JoinTransformConfig, RootTransformConfig, SelectTransformConfig};
use crate::parser::keyword::Keyword;
use crate::{
    frame::common::{
        FrameAsyncBroadcastHandle, FrameAsyncListenHandle, FrameBroadcastHandle, FrameListenHandle, FrameUpdateType,
    },
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    task::stage::Stage,
    util::error::{CpError, CpResult},
};

/// Base transform trait. Takes
pub trait Transform {
    fn run(&self, main: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame>;
}

pub trait TransformConfig {
    fn validate(&self) -> Vec<CpError>;
    fn transform(&self) -> Box<dyn Transform>;
}

/// The root transform node that runs all its subtransforms
pub struct RootTransform {
    label: String,
    input: String,
    output: String,
    subtransforms: Vec<Box<dyn Transform>>,
}

impl RootTransform {
    pub fn new(label: &str, input: &str, output: &str, subtransforms: Vec<Box<dyn Transform>>) -> RootTransform {
        RootTransform {
            label: label.to_string(),
            input: input.to_string(),
            output: output.to_string(),
            subtransforms,
        }
    }
}

impl Stage for RootTransform {
    /// The synchronous, linear execution doesn't use the broadcast/listen channels at all
    fn linear(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        log::info!("Stage initialized: {}", &self.label);
        let input = ctx.extract_result(&self.input)?;
        let output = self.run(input, ctx.clone())?;
        ctx.insert_result(&self.output, output)
    }
    /// The synchronous, concurrent execution listens to input for the initial frame, and broadcasts to output
    /// the produced frame
    fn sync_exec(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
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
    /// The asynchronous, concurrent execution executes until it receives a Kill message.
    async fn async_exec(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<u64> {
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
                }
                FrameUpdateType::Kill => {
                    log::info!("Stage killed after {} iterations: {}", loops, &self.label);
                    return Ok(loops);
                }
            }
        }
    }
}

impl Transform for RootTransform {
    /// Runs execution in order. ctx is passed to children subtransforms
    /// which can also extract changes on frames
    fn run(&self, main: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        let mut next = main;
        for stage in &self.subtransforms {
            next = stage.as_ref().run(next, ctx.clone())?
        }
        Ok(next)
    }
}

macro_rules! try_deserialize {
    ($value:expr, $($type:ty),+) => {
        $(if let Ok(config) = serde_yaml_ng::from_value::<$type>($value.clone()) {
            Some(Box::new(config) as Box<dyn TransformConfig>)
        }) else+
        else {
            None
        }
    };
}

impl RootTransformConfig {
    fn parse_subtransforms(&self) -> Vec<Result<Box<dyn TransformConfig>, CpError>> {
        self.steps
            .iter()
            .map(|transform| {
                let config = try_deserialize!(transform, SelectTransformConfig, JoinTransformConfig);

                config.ok_or_else(|| {
                    CpError::ConfigError(
                        "Transform config parsing error",
                        format!("Failed to parse transform config: {:?}", transform),
                    )
                })
            })
            .collect()
    }
}

impl TransformConfig for RootTransformConfig {
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];

        for result in self.parse_subtransforms() {
            match result {
                Ok(config) => {
                    errors.extend(config.validate().into_iter().map(|e| {
                        CpError::ConfigError("Transform config validation error", format!("Subtransform: {}", e))
                    }));
                }
                Err(e) => errors.push(e),
            }
        }

        errors
    }

    fn transform(&self) -> Box<dyn Transform> {
        let mut subtransforms: Vec<Box<dyn Transform>> = vec![];
        for result in self.parse_subtransforms() {
            subtransforms.push(result.unwrap().transform());
        }

        Box::new(RootTransform {
            label: self.label.clone(),
            input: self.input.value().expect("input").clone(),
            output: self.output.value().expect("output").clone(),
            subtransforms,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use polars::{df, frame::DataFrame, prelude::IntoLazy};

    use super::{RootTransform, Transform, TransformConfig};
    use crate::parser::keyword::{Keyword, StrKeyword};
    use crate::task::transform::config::RootTransformConfig;
    use crate::{
        frame::common::{FrameAsyncBroadcastHandle, FrameAsyncListenHandle, FrameBroadcastHandle, FrameListenHandle},
        pipeline::context::{DefaultPipelineContext, PipelineContext},
        task::stage::Stage,
    };

    fn expected() -> DataFrame {
        df!( "a" => [1, 2, 3], "b" => [4, 5, 6] ).unwrap()
    }

    #[test]
    fn success_run_no_subtransforms() {
        let ctx = Arc::new(DefaultPipelineContext::with_results(&["orig"], 1));
        let trf = RootTransform::new("trf", "orig", "actual", Vec::new());
        let expected = trf.run(DataFrame::empty().lazy(), ctx.clone());
        assert_eq!(expected.unwrap().collect().unwrap(), DataFrame::empty());
    }

    #[test]
    fn success_linear_exec_no_subtransforms() {
        let ctx = Arc::new(DefaultPipelineContext::with_results(&["orig", "actual"], 1));
        ctx.insert_result("orig", expected().lazy()).unwrap();
        let trf = RootTransform::new("trf", "orig", "actual", Vec::new());
        trf.linear(ctx.clone()).unwrap();
        let actual = ctx.extract_clone_result("actual").unwrap();
        assert_eq!(actual, expected());
    }

    #[test]
    fn success_sync_exec_no_subtransforms() {
        let ctx = Arc::new(DefaultPipelineContext::with_results(&["orig", "actual"], 1));
        let lctx = ctx.clone();
        let bctx = ctx.clone();
        let fctx = ctx.clone();
        thread::scope(|s| {
            let _b = s.spawn(move || {
                let mut broadcast = bctx.get_broadcast("orig", "source").unwrap();
                broadcast.broadcast(expected().lazy()).unwrap();
            });
            let _t = s.spawn(move || {
                let trf = RootTransform::new("trf", "orig", "actual", Vec::new());
                trf.sync_exec(lctx).unwrap();
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
    fn success_async_exec_no_subtransforms() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
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
                assert_eq!(trf.async_exec(lctx).await.unwrap(), 1);
            };
            let thandle = async move || {
                let mut listener = fctx.get_async_listener("actual", "killer").unwrap();
                let mut killer = fctx.get_async_broadcast("orig", "killer").unwrap();
                // log::debug!("AWAIT handle killer");
                let update = listener.listen().await.unwrap();
                {
                    let lf = update.frame.read().unwrap();
                    let actual = lf.clone().collect().unwrap();
                    assert_eq!(actual, expected());
                }
                killer.kill().await.unwrap();
            };
            tokio::join!(bhandle(), lhandle(), thandle());
        };
        rt.block_on(event());
    }

    #[test]
    fn create_root_transform_good_config() {
        let select_yaml = r#"
select:
    key: value
"#;
        let select_value: serde_yaml_ng::Value = serde_yaml_ng::from_str(select_yaml).unwrap();

        let join_yaml = r#"
join:
    right: BASIC
    left_on: [col]
    right_on: [my_col]
    how: left
"#;
        let join_value: serde_yaml_ng::Value = serde_yaml_ng::from_str(join_yaml).unwrap();

        let config = RootTransformConfig {
            label: "test_label".to_string(),
            input: StrKeyword::with_value("test_input".to_owned()),
            output: StrKeyword::with_value("test_output".to_owned()),
            steps: vec![select_value, join_value],
        };

        assert!(config.validate().is_empty());
    }

    #[test]
    fn create_root_transform_bad_config() {
        let select_yaml = r#"
select:
    key: value
"#;
        let select_value: serde_yaml_ng::Value = serde_yaml_ng::from_str(select_yaml).unwrap();

        let invalid_yaml = r#"
purr_transform:
    key: value
"#;
        let invalid_value: serde_yaml_ng::Value = serde_yaml_ng::from_str(invalid_yaml).unwrap();

        let config = RootTransformConfig {
            label: "test_label".to_string(),
            input: StrKeyword::with_value("test_input".to_owned()),
            output: StrKeyword::with_value("test_output".to_owned()),
            steps: vec![select_value, invalid_value],
        };

        assert_eq!(config.validate().len(), 1);
    }

    #[test]
    fn valid_root_transform_basic() {
        let select_yaml_1 = r#"
select:
    one: two
    three: two
"#;
        let select_value_1: serde_yaml_ng::Value = serde_yaml_ng::from_str(select_yaml_1).unwrap();

        let select_yaml_2 = r#"
select:
    one: one
    four: three
"#;
        let select_value_2: serde_yaml_ng::Value = serde_yaml_ng::from_str(select_yaml_2).unwrap();

        let config = RootTransformConfig {
            label: "test_label".to_string(),
            input: StrKeyword::with_value("test_input".to_owned()),
            output: StrKeyword::with_value("test_output".to_owned()),
            steps: vec![select_value_1, select_value_2],
        };

        assert!(config.validate().is_empty());
        let root_transform = config.transform();
        let ctx = Arc::new(DefaultPipelineContext::new());
        let main = df!(
            "two" => [1, 2, 3]
        )
        .unwrap()
        .lazy();
        let actual = root_transform.run(main, ctx).unwrap();
        let expected = df!(
            "one" => [1, 2, 3],
            "four" => [1, 2, 3],
        )
        .unwrap();
        assert_eq!(actual.collect().unwrap(), expected);
    }
}
