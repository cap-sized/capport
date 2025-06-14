use polars::prelude::*;

use super::config::{
    DropTransformConfig, JoinTransformConfig, RootTransformConfig, SelectTransformConfig, SqlTransformConfig,
    TimeConvertConfig, UniformIdTypeConfig, UnnestTransformConfig, WithColTransformConfig,
};
use crate::frame::common::{FrameAsyncListenHandle, FrameUpdate};
use crate::frame::polars::PolarsAsyncListenHandle;
use crate::parser::keyword::Keyword;
use crate::task::stage::StageTaskConfig;
use crate::try_deserialize_stage;
use crate::util::common::format_schema;
use crate::{
    frame::common::{FrameAsyncBroadcastHandle, FrameBroadcastHandle, FrameListenHandle, FrameUpdateType},
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    task::stage::Stage,
    util::error::{CpError, CpResult},
};

/// Base transform trait.
pub trait Transform {
    fn run(&self, main: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame>;
}

pub trait TransformConfig {
    fn emplace(&mut self, context: &serde_yaml_ng::Mapping) -> CpResult<()>;
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

    pub fn produces(&self) -> Vec<String> {
        vec![self.output.clone()]
    }
}

impl Stage for RootTransform {
    /// The synchronous, linear execution doesn't use the broadcast/listen channels at all
    fn linear(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        log::info!("Stage initialized: {}", &self.label);
        let input = ctx.extract_result(&self.input)?;
        log::info!(
            "INPUT `{}`: {:?}",
            &self.label,
            input.clone().collect().expect("before transform")
        );
        let output = self.run(input, ctx.clone())?;
        let df = output.clone().collect().expect("transform");
        log::info!(
            "[Transform] OUTPUT `{}`: {:?}\n{}",
            &self.label,
            &df,
            format_schema(&df.schema())
        );
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
        let update = input_listener.force_listen();
        let input = update.frame.read()?.clone();
        let output = self.run(input, ctx)?;
        output_broadcast.broadcast(output)
    }
    /// The asynchronous, concurrent execution executes until it receives a Kill message.
    async fn async_exec(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<u64> {
        let mut loops = 0;
        log::info!("Stage initialized: {}", &self.label);
        let mut listen = ctx.get_async_listener(&self.input, &self.label)?;
        let lp: *mut PolarsAsyncListenHandle = &mut listen;
        let mut output_broadcast = ctx.get_async_broadcast(&self.output, &self.label)?;
        loop {
            let update: FrameUpdate<LazyFrame> = unsafe { (*lp).listen().await? };
            log::trace!("{} Received update: {:?}", &self.label, update.info);
            match update.info.msg_type {
                FrameUpdateType::Replace => {
                    let input = update.frame.read()?.clone();
                    let output = self.run(input, ctx.clone())?;
                    log::trace!("BCAST RootTransform handle {} to: {:?}", &self.label, &self.output);
                    match output_broadcast.broadcast(output) {
                        Ok(_) => log::info!("Sent update for frame {}", &self.output),
                        Err(e) => log::error!("{}: {:?}", &self.output, e),
                    };
                    loops += 1;
                }
                FrameUpdateType::Kill => {
                    log::info!("[Transform] Sent termination signal for frame {}", &self.output);
                    output_broadcast.kill().unwrap();
                    log::info!(
                        "Terminating transform stage `{}` after {} iterations",
                        &self.label,
                        loops
                    );
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
        for subtransform in &self.subtransforms {
            next = subtransform.as_ref().run(next, ctx.clone())?
        }
        Ok(next)
    }
}

impl RootTransformConfig {
    fn parse_subtransforms(&self) -> Vec<Result<Box<dyn TransformConfig>, CpError>> {
        self.steps
            .iter()
            .map(|transform| {
                let config = try_deserialize_stage!(
                    transform,
                    dyn TransformConfig,
                    SelectTransformConfig,
                    JoinTransformConfig,
                    DropTransformConfig,
                    SqlTransformConfig,
                    UnnestTransformConfig,
                    WithColTransformConfig,
                    TimeConvertConfig,
                    UniformIdTypeConfig
                );
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

impl StageTaskConfig<RootTransform> for RootTransformConfig {
    fn parse(
        &self,
        _ctx: &DefaultPipelineContext,
        context: &serde_yaml_ng::Mapping,
    ) -> Result<RootTransform, Vec<CpError>> {
        let mut subtransforms = vec![];
        let mut errors = vec![];
        for result in self.parse_subtransforms() {
            match result {
                Ok(mut config) => {
                    if let Err(e) = config.emplace(context) {
                        errors.push(e);
                    }
                    let errs = config.validate();
                    if errs.is_empty() {
                        subtransforms.push(config.transform());
                    } else {
                        errors.extend(errs);
                    }
                }
                Err(e) => errors.push(e),
            }
        }
        let mut input = self.input.clone();
        let mut output = self.output.clone();
        match input.insert_value_from_context(context) {
            Ok(_) => {}
            Err(e) => errors.push(e),
        }
        match output.insert_value_from_context(context) {
            Ok(_) => {}
            Err(e) => errors.push(e),
        }
        if errors.is_empty() {
            Ok(RootTransform {
                label: self.label.clone(),
                input: input.value().expect("input").clone(),
                output: output.value().expect("output").clone(),
                subtransforms,
            })
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use polars::{df, frame::DataFrame, prelude::IntoLazy};

    use super::{RootTransform, Transform};
    use crate::async_st;
    use crate::parser::keyword::{Keyword, StrKeyword};
    use crate::task::stage::StageTaskConfig;
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
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let ctx = Arc::new(DefaultPipelineContext::with_results(&["orig", "actual"], 1));
        let lctx = ctx.clone();
        let bctx = ctx.clone();
        let fctx = ctx.clone();
        let mut broadcast = bctx.get_broadcast("orig", "source").unwrap();
        broadcast.broadcast(expected().lazy()).unwrap();
        thread::scope(|s| {
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
        async_st!(async || {
            let ctx = Arc::new(DefaultPipelineContext::with_results(&["orig", "actual"], 2));
            let lctx = ctx.clone();
            let fctx = ctx.clone();
            let expected = || df!( "a" => [1, 2, 3], "b" => [4, 5, 6] ).unwrap();
            let lhandle = async move || {
                let trf = RootTransform::new("trf", "orig", "actual", Vec::new());
                assert_eq!(trf.async_exec(lctx).await.unwrap(), 1);
            };
            let thandle = async move || {
                let mut broadcast = fctx.get_async_broadcast("orig", "source").unwrap();
                broadcast.broadcast(expected().lazy()).unwrap();
                let mut listener = fctx.get_async_listener("actual", "killer").unwrap();
                let mut killer = fctx.get_async_broadcast("orig", "killer").unwrap();
                let update = listener.listen().await.unwrap();
                {
                    let lf = update.frame.read().unwrap();
                    let actual = lf.clone().collect().unwrap();
                    assert_eq!(actual, expected());
                }
                log::debug!("AWAIT handle killer");
                killer.kill().unwrap();
            };
            tokio::join!(lhandle(), thandle());
        });
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
        let context = serde_yaml_ng::Mapping::new();
        let ctx = Arc::new(DefaultPipelineContext::new());
        let errs = config.parse(&ctx, &context);

        assert!(errs.is_ok());
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
        let context = serde_yaml_ng::Mapping::new();
        let ctx = Arc::new(DefaultPipelineContext::new());
        let errs = config.parse(&ctx, &context);

        assert!(errs.is_err());
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
    one: $input
    four: $output
"#;
        let select_value_2: serde_yaml_ng::Value = serde_yaml_ng::from_str(select_yaml_2).unwrap();

        let config = RootTransformConfig {
            label: "test_label".to_string(),
            input: StrKeyword::with_value("test_input".to_owned()),
            output: StrKeyword::with_value("test_output".to_owned()),
            steps: vec![select_value_1, select_value_2],
        };
        let mapping = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>("{input: one, output: three}").unwrap();
        let ctx = Arc::new(DefaultPipelineContext::new());

        let root_transform = config.parse(&ctx, &mapping).unwrap();
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
