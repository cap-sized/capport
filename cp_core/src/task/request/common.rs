use std::sync::{Arc, atomic::AtomicUsize};

use async_trait::async_trait;
use polars::prelude::LazyFrame;

use crate::{
    ctx_run_n_async, ctx_run_n_threads,
    frame::{
        common::{FrameAsyncBroadcastHandle, FrameAsyncListenHandle, FrameListenHandle, FrameUpdate, FrameUpdateType},
        polars::PolarsAsyncListenHandle,
    },
    parser::keyword::Keyword,
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    task::stage::{Stage, StageTaskConfig},
    try_deserialize_stage,
    util::error::{CpError, CpResult},
    valid_or_insert_error,
};

use super::config::{HttpBatchConfig, RequestGroupConfig};

/// Base sink trait. Importantly, certain requests may have dependencies as well.
/// If it receives a termination signal, it is the sink type's responsibility to clean up and
/// kill its dependents as well.
/// Is a syntactic clone of the sink trait, but do semantically different things.
#[async_trait]
pub trait Request {
    fn connection_type(&self) -> &str;
    fn name(&self) -> &str;
    fn run(&self, frame: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<()>;
    async fn fetch(&self, frame: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<()>;
}

pub struct BoxedRequest(Box<dyn Request>);

pub trait RequestConfig {
    fn emplace(&mut self, ctx: &DefaultPipelineContext, context: &serde_yaml_ng::Mapping) -> CpResult<()>;
    fn validate(&self) -> Vec<CpError>;
    fn transform(&self) -> Box<dyn Request>;
}

/// We NEVER modify the individual Request instantiations after initialization.
/// Hence the Box<dyn Request> is safe to access in parallel
unsafe impl Send for BoxedRequest {}
unsafe impl Sync for BoxedRequest {}

pub struct RequestGroup {
    label: String,
    input: String,
    max_threads: usize,
    requests: Vec<BoxedRequest>,
}

impl RequestGroup {
    pub fn new(input: &str, label: &str, max_threads: usize, requests: Vec<Box<dyn Request>>) -> RequestGroup {
        RequestGroup {
            label: label.to_owned(),
            input: input.to_owned(),
            max_threads,
            requests: requests.into_iter().map(BoxedRequest).collect::<Vec<BoxedRequest>>(),
        }
    }

    pub fn produces(&self) -> Vec<String> {
        self.requests.iter().map(|x| x.0.name().to_owned()).collect()
    }
}

impl Stage for RequestGroup {
    fn linear(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        log::info!("Stage initialized [single-thread]: {}", &self.label);
        let lf = ctx.extract_result(&self.input)?;
        for req in &self.requests {
            req.0.run(lf.clone(), ctx.clone())?;
            log::info!(
                "Success pushing frame update to {}: {}",
                req.0.connection_type(),
                req.0.name()
            );
        }
        Ok(())
    }
    fn sync_exec(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        log::info!(
            "Stage initialized [max_threads: {}]: {}",
            &self.label,
            &self.max_threads
        );
        let input = self.input.as_str();
        let label = self.label.as_str();
        let mut listen = ctx.get_listener(input, label)?;
        let update = listen.force_listen();
        let mut lf: Option<LazyFrame> = None;
        {
            let fread = update.frame.read()?;
            let _ = lf.insert(fread.clone());
        }
        ctx_run_n_threads!(
            self.max_threads,
            self.requests.as_slice(),
            move |(reqs, ictx, dfh): (_, Arc<DefaultPipelineContext>, Option<LazyFrame>)| {
                for req in reqs {
                    let r: &BoxedRequest = req;
                    let req = &r.0;
                    match req.run(dfh.clone().expect("req.df"), ictx.clone()) {
                        Ok(_) => log::info!("pushed frame `{}` on connection `{}`", input, &r.0.connection_type(),),
                        Err(e) => log::error!(
                            "Failed fetch frame `{}` of type `{}`: {:?}",
                            input,
                            &r.0.connection_type(),
                            e
                        ),
                    };
                }
            },
            ctx,
            lf
        );
        Ok(())
    }

    async fn async_exec(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<u64> {
        log::info!("Stage initialized [async fetch]: {}", &self.label);
        let label = self.label.as_str();
        let input = self.input.as_str();
        let mut listen = ctx.get_async_listener(input, label)?;
        let lp: *mut PolarsAsyncListenHandle = &mut listen;
        let mut loops: u64 = 0;
        let terminations = AtomicUsize::new(0);
        loop {
            // unsafe hack to get past the borrow checker
            let update: FrameUpdate<LazyFrame> = unsafe { (*lp).listen().await? };
            ctx_run_n_async!(
                label,
                &self.requests,
                async |breq: &BoxedRequest, ctx: Arc<DefaultPipelineContext>| {
                    let req = &breq.0;
                    let upd = update.clone();
                    match upd.info.msg_type {
                        FrameUpdateType::Replace => {
                            let mut dataframe: Option<LazyFrame> = None;
                            {
                                let fread = update.frame.read()?;
                                let _ = dataframe.insert(fread.clone());
                            }
                            req.fetch(dataframe.expect("must have dataframe"), ctx.clone()).await?;
                            log::info!(
                                "Success pushing frame update via {}: {}",
                                req.connection_type(),
                                req.name()
                            );
                        }
                        FrameUpdateType::Kill => {
                            terminations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let mut bcast = match ctx.get_async_broadcast(breq.0.name(), label) {
                                Ok(x) => x,
                                Err(e) => {
                                    return Err(CpError::PipelineError("Broadcast channel failed", e.to_string()));
                                }
                            };
                            bcast.kill().await?;
                            log::info!("Sent termination signal for frame {}", breq.0.name());
                        }
                    }
                    Ok::<(), CpError>(())
                },
                ctx.clone()
            );
            if terminations.load(std::sync::atomic::Ordering::Relaxed) >= self.requests.len() {
                log::info!("Stage killed after {} iterations: {}", loops, &self.label);
                break;
            }
            loops += 1;
        }
        Ok(loops)
    }
}

impl RequestGroupConfig {
    fn parse_subrequests(&self) -> Vec<Result<Box<dyn RequestConfig>, CpError>> {
        self.requests
            .iter()
            .map(|transform| {
                let config = try_deserialize_stage!(transform, dyn RequestConfig, HttpBatchConfig);
                config.ok_or_else(|| {
                    CpError::ConfigError(
                        "Request config parsing error",
                        format!("Failed to parse request config: {:?}", transform),
                    )
                })
            })
            .collect()
    }
}

impl StageTaskConfig<RequestGroup> for RequestGroupConfig {
    fn parse(
        &self,
        ctx: &DefaultPipelineContext,
        context: &serde_yaml_ng::Mapping,
    ) -> Result<RequestGroup, Vec<CpError>> {
        let mut subrequests = vec![];
        let mut errors = vec![];
        for result in self.parse_subrequests() {
            match result {
                Ok(mut config) => {
                    if let Err(e) = config.emplace(ctx, context) {
                        errors.push(e);
                    }
                    let errs = config.validate();
                    if errs.is_empty() {
                        subrequests.push(BoxedRequest(config.transform()));
                    } else {
                        errors.extend(errs);
                    }
                }
                Err(e) => errors.push(e),
            }
        }
        let mut input = self.input.clone();
        if input.insert_value_from_context(context).is_err() {
            valid_or_insert_error!(errors, input, "request.input");
        }
        if errors.is_empty() {
            Ok(RequestGroup {
                label: self.label.clone(),
                input: input.value().expect("request.input").to_owned(),
                max_threads: self.max_threads,
                requests: subrequests,
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
    use httpmock::{Method::GET, Mock, MockServer};
    use polars::{
        df,
        frame::DataFrame,
        functions::concat_df_horizontal,
        prelude::{IntoLazy, LazyFrame, UnionArgs, concat_lf_horizontal},
    };
    use serde::{Deserialize, Serialize};

    use crate::{
        context::model::ModelRegistry,
        frame::common::{FrameAsyncBroadcastHandle, FrameBroadcastHandle},
        model::common::ModelConfig,
        parser::keyword::{Keyword, StrKeyword},
        pipeline::context::{DefaultPipelineContext, PipelineContext},
        task::{
            request::config::RequestGroupConfig,
            stage::{Stage, StageTaskConfig},
        },
        util::{
            common::vec_str_json_to_df,
            error::CpResult,
            test::{DummyData, assert_frame_equal},
        },
    };

    use super::{Request, RequestGroup};

    struct MockRequest {
        out: String,
        app: String,
    }

    impl MockRequest {
        fn new(out: &str, app: &str) -> Self {
            Self {
                out: out.to_string(),
                app: app.to_string(),
            }
        }
    }

    fn default_in() -> DataFrame {
        df!( "x" => [true, true, false] ).unwrap()
    }

    fn default_df() -> DataFrame {
        df!( "a" => [1, 2, 3], "b" => [4, 5, 6] ).unwrap()
    }

    fn default_next() -> DataFrame {
        df!( "c" => ["a", "b", "c"], "d" => [4, 5, 6] ).unwrap()
    }

    fn default_merged() -> DataFrame {
        df!(
            "a" => [1, 2, 3],
            "b" => [4, 5, 6],
            "c" => ["a", "b", "c"],
            "d" => [4, 5, 6]
        )
        .unwrap()
    }

    #[async_trait]
    impl Request for MockRequest {
        fn connection_type(&self) -> &str {
            "mock"
        }
        fn name(&self) -> &str {
            &self.out
        }
        fn run(&self, input: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
            let fetch_immd = ctx.extract_result(&self.app)?;
            let collected = [fetch_immd, input];
            let result = concat_lf_horizontal(
                collected,
                UnionArgs {
                    parallel: false,
                    ..Default::default()
                },
            )
            .unwrap();
            let mut bcast = ctx.get_broadcast(&self.out, self.connection_type())?;
            bcast.broadcast(result)?;
            Ok(())
        }
        async fn fetch(&self, input: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
            let fetch_immd = ctx.extract_result(&self.app)?;
            let collected = [fetch_immd, input];
            let result = concat_lf_horizontal(
                collected,
                UnionArgs {
                    parallel: false,
                    ..Default::default()
                },
            )
            .unwrap();
            let mut bcast = ctx.get_async_broadcast(&self.out, self.connection_type())?;
            bcast.broadcast(result).await?;
            Ok(())
        }
    }

    #[test]
    fn success_mock_request_run() {
        let ctx = Arc::new(DefaultPipelineContext::with_results(&["mock", "app"], 2));
        ctx.insert_result("app", default_df().lazy()).unwrap();
        let req = MockRequest::new("mock", "app");
        req.run(default_next().lazy(), ctx.clone()).unwrap();
        let expected = ctx.extract_clone_result("mock").unwrap();
        assert_eq!(expected, default_merged());
    }

    #[test]
    fn success_mock_request_linear_exec() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let ctx = Arc::new(DefaultPipelineContext::with_results(
            &["df", "next", "mock", "final", "in"],
            1,
        ));
        ctx.insert_result("next", default_next().lazy()).unwrap();
        ctx.insert_result("df", default_df().lazy()).unwrap();
        let mut in_handle = ctx.get_broadcast("in", "orig").unwrap();
        in_handle.broadcast(default_in().lazy()).unwrap();
        let req1 = MockRequest::new("mock", "df");
        let req2 = MockRequest::new("final", "next");
        let req = RequestGroup::new("in", "root", 1, vec![Box::new(req1), Box::new(req2)]);
        req.linear(ctx.clone()).unwrap();
        let actual_1 = concat_df_horizontal(&[default_in(), default_df()], true).unwrap();
        let actual_2 = concat_df_horizontal(&[default_in(), default_next()], true).unwrap();
        assert_frame_equal(ctx.extract_clone_result("mock").unwrap(), actual_1);
        assert_frame_equal(ctx.extract_clone_result("final").unwrap(), actual_2);
    }

    #[test]
    fn success_mock_request_sync_exec_single_thread() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let ctx = Arc::new(DefaultPipelineContext::with_results(
            &["df", "next", "mock", "final", "in"],
            1,
        ));
        ctx.insert_result("next", default_next().lazy()).unwrap();
        ctx.insert_result("df", default_df().lazy()).unwrap();
        let mut in_handle = ctx.get_broadcast("in", "orig").unwrap();
        in_handle.broadcast(default_in().lazy()).unwrap();
        let req1 = MockRequest::new("mock", "df");
        let req2 = MockRequest::new("final", "next");
        let req = RequestGroup::new("in", "root", 1, vec![Box::new(req1), Box::new(req2)]);
        req.linear(ctx.clone()).unwrap();
        let actual_1 = concat_df_horizontal(&[default_in(), default_df()], true).unwrap();
        let actual_2 = concat_df_horizontal(&[default_in(), default_next()], true).unwrap();
        assert_frame_equal(ctx.extract_clone_result("mock").unwrap(), actual_1);
        assert_frame_equal(ctx.extract_clone_result("final").unwrap(), actual_2);
    }

    #[test]
    fn success_mock_request_sync_exec_multi_thread() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let count = [3, 4];
        for thread_count in count {
            let ctx = Arc::new(DefaultPipelineContext::with_results(
                &["df", "next", "mock", "final", "in"],
                1,
            ));
            ctx.insert_result("next", default_next().lazy()).unwrap();
            ctx.insert_result("df", default_df().lazy()).unwrap();
            let mut in_handle = ctx.get_broadcast("in", "orig").unwrap();
            in_handle.broadcast(default_in().lazy()).unwrap();
            let req1 = MockRequest::new("mock", "df");
            let req2 = MockRequest::new("final", "next");
            let req = RequestGroup::new("in", "root", thread_count, vec![Box::new(req1), Box::new(req2)]);
            req.sync_exec(ctx.clone()).unwrap();
            let actual_1 = concat_df_horizontal(&[default_in(), default_df()], true).unwrap();
            let actual_2 = concat_df_horizontal(&[default_in(), default_next()], true).unwrap();
            assert_frame_equal(ctx.extract_clone_result("mock").unwrap(), actual_1);
            assert_frame_equal(ctx.extract_clone_result("final").unwrap(), actual_2);
        }
    }

    #[test]
    fn success_mock_request_async_exec() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let mut rt_builder = tokio::runtime::Builder::new_current_thread();
        rt_builder.enable_all();
        let rt = rt_builder.build().unwrap();
        let event = async || {
            let ctx =
                Arc::new(DefaultPipelineContext::with_results(&["df", "next", "mock", "final", "in"], 2).with_signal());
            let ictx = ctx.clone();
            ctx.insert_result("next", default_next().lazy()).unwrap();
            ctx.insert_result("df", default_df().lazy()).unwrap();
            let mut in_handle = ctx.get_async_broadcast("in", "orig").unwrap();
            in_handle.broadcast(default_in().lazy()).await.unwrap();
            let req1 = MockRequest::new("mock", "df");
            let req2 = MockRequest::new("final", "next");
            let req = RequestGroup::new("in", "root", 1, vec![Box::new(req1), Box::new(req2)]);
            let action_path = async move || {
                req.async_exec(ictx.clone()).await.unwrap();
                let actual_1 = concat_df_horizontal(&[default_in(), default_df()], true).unwrap();
                let actual_2 = concat_df_horizontal(&[default_in(), default_next()], true).unwrap();
                assert_frame_equal(ictx.extract_clone_result("mock").unwrap(), actual_1);
                assert_frame_equal(ictx.extract_clone_result("final").unwrap(), actual_2);
            };
            let terminator = async move || {
                let mut inh = ctx.get_async_broadcast("in", "orig").unwrap();
                inh.kill().await.unwrap();
            };
            tokio::join!(action_path(), terminator());
        };
        rt.block_on(event());
    }

    #[test]
    fn create_request_group_good_config() {
        let configs_str = r#"
- http_batch:
    method: get
    content_type: application/json
    output: $output
    url_column: url
    options:
        max_retry: 1
        init_retry_interval_ms: 1000
#- http_single:
#   method: GET
#   content_type: application/json
#   output: WEB_PLAYERS
#   url_column: 
#       str: https://api-web.nhle.com/v1/meta
#   url_params:
#       - col: player
#         template: players={}
#         separator: ","
#   model_fields:
#       test: str
"#;
        let configs = serde_yaml_ng::from_str::<Vec<serde_yaml_ng::Value>>(configs_str).unwrap();
        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>("{input: SIN, output: SAMPLE}").unwrap();
        let sgconfig = RequestGroupConfig {
            label: "".to_owned(),
            input: StrKeyword::with_symbol("input"),
            max_threads: 1,
            requests: configs,
        };
        let ctx = Arc::new(DefaultPipelineContext::with_results(&["SIN", "SAMPLE"], 2));
        let actual = sgconfig.parse(&ctx, &context);
        actual.unwrap();
    }

    #[test]
    fn create_request_group_bad_configs() {
        [
            "
- http_batch:
    method: get
    content_type: application/json
    output: $missing
    url_column: url
    model: player
",
            "
- bad:
    method: get
    content_type: application/json
    output: $output
    url_column: url
    model: player
",
        ]
        .iter()
        .for_each(|configs_str| {
            let configs = serde_yaml_ng::from_str::<Vec<serde_yaml_ng::Value>>(configs_str).unwrap();
            let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>("{input: SIN, output: SAMPLE}").unwrap();
            let sgconfig = RequestGroupConfig {
                label: "".to_owned(),
                input: StrKeyword::with_value("input".to_owned()),
                max_threads: 1,
                requests: configs,
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

    #[derive(Serialize, Deserialize)]
    struct SampleAct {
        id: String,
        label: Option<String>,
    }

    #[test]
    fn valid_request_group_basic() {
        // fern::Dispatch::new().level(log::LevelFilter::Trace).chain(std::io::stdout()).apply().unwrap();
        let configs_str = r#"
- http_batch:
    method: get
    content_type: application/json
    output: $output
    url_column: url
#- http_single:
#   method: GET
#   content_type: application/json
#   output: WEB_PLAYERS
#   url_column: 
#       str: https://api-web.nhle.com/v1/meta
#   url_params:
#       - col: player
#         template: players={}
#         separator: ","
#   model_fields:
#       test: str
"#;
        let configs = serde_yaml_ng::from_str::<Vec<serde_yaml_ng::Value>>(configs_str).unwrap();
        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>("output: OUTPUT").unwrap();
        let sgconfig = RequestGroupConfig {
            label: "".to_owned(),
            input: StrKeyword::with_value("URLS".to_owned()),
            max_threads: 1,
            requests: configs,
        };
        let ctx = Arc::new(DefaultPipelineContext::with_results(&["OUTPUT", "URLS"], 1));
        let mut bcast = ctx.get_broadcast("URLS", "main").unwrap();
        let server = MockServer::start();
        let _mocks = DummyData::json_actions()
            .iter()
            .map(|j| {
                let act: SampleAct = serde_yaml_ng::from_str(j).unwrap();
                server.mock(|when, then| {
                    when.method(GET).path("/actions").query_param("id", act.id);
                    then.status(200).header("content-type", "application/json").body(j);
                })
            })
            .collect::<Vec<Mock>>();
        let urls = DummyData::json_actions()
            .iter()
            .map(|j| {
                let act: SampleAct = serde_yaml_ng::from_str(j).unwrap();
                server.url(format!("/actions?id={}", act.id))
            })
            .collect::<Vec<String>>();
        let expected = vec_str_json_to_df(&DummyData::json_actions()).unwrap();
        bcast.broadcast(df!( "url" => urls ).unwrap().lazy()).unwrap();
        let actual_stage = sgconfig.parse(&ctx, &context).unwrap();
        actual_stage.linear(ctx.clone()).unwrap();
        let actual = ctx.clone().extract_clone_result("OUTPUT").unwrap();
        assert_frame_equal(actual, expected);
    }
}
