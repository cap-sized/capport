use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use polars::{
    frame::DataFrame,
    prelude::{Expr, IntoLazy, LazyFrame},
};
use reqwest::header::HeaderValue;

use crate::{
    ctx_run_n_async,
    frame::common::{FrameAsyncBroadcastHandle, FrameBroadcastHandle},
    model::common::ModelConfig,
    model_emplace,
    parser::keyword::Keyword,
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    util::{
        common::{DEFAULT_HTTP_REQ_INIT_RETRY_INTERVAL_MS, DEFAULT_HTTP_REQ_MAX_RETRY, vec_str_json_to_df},
        error::{CpError, CpResult},
    },
    valid_or_insert_error,
};

use super::{
    common::{Request, RequestConfig},
    config::HttpBatchConfig,
};

pub struct HttpBatchRequest {
    url_column: Expr,
    output: String,
    content_type: String,
    schema: Option<Vec<Expr>>,
    max_retry: u8,
    init_retry_interval_ms: u64,
}

fn default_ctype() -> HeaderValue {
    HeaderValue::from_str("unknown").expect("default_ctype")
}

pub fn sync_url(
    client: &reqwest::blocking::Client,
    url: &str,
    max_retry: u8,
    init_retry_interval: u64,
    content_type: &str,
) -> CpResult<String> {
    let dctype = default_ctype();
    let mut retry_interval = init_retry_interval;
    for attempt in 0..max_retry {
        let req = client.get(url);
        log::debug!("#{}: HTTP GET {}", attempt, url);
        match req.send() {
            Ok(resp) => {
                let actual_ctype = match resp.headers().get("content-type").unwrap_or(&dctype).to_str() {
                    Ok(x) => x,
                    Err(e) => {
                        return Err(CpError::ConnectionError(e.to_string()));
                    }
                };
                if actual_ctype != content_type {
                    return Err(CpError::ConnectionError(format!(
                        "Failed #{} [HTTP GET {}]: content-type `{}` does not match expected `{}`. headers: {:?}",
                        attempt,
                        url,
                        actual_ctype,
                        content_type,
                        resp.headers()
                    )));
                }
                return Ok(resp.text()?);
            }
            Err(e) => {
                log::trace!(
                    "Failed #{} [HTTP GET {}]: {}. Retrying in {}ms...",
                    attempt,
                    url,
                    e,
                    retry_interval
                );
                std::thread::sleep(std::time::Duration::from_millis(retry_interval));
                retry_interval *= 2;
            }
        }
    }
    Err(CpError::ConnectionError(format!(
        "Reach max retries [HTTP GET {}]",
        &url
    )))
}

pub async fn async_url(
    client: &reqwest::Client,
    url: &str,
    max_retry: u8,
    init_retry_interval: u64,
    content_type: &str,
) -> CpResult<String> {
    let dctype = default_ctype();
    let mut retry_interval = init_retry_interval;
    for attempt in 0..max_retry {
        let req = client.get(url);
        log::debug!("#{}: HTTP GET {}", attempt, url);
        match req.send().await {
            Ok(resp) => {
                let actual_ctype = match resp.headers().get("content-type").unwrap_or(&dctype).to_str() {
                    Ok(x) => x,
                    Err(e) => {
                        return Err(CpError::ConnectionError(e.to_string()));
                    }
                };
                if actual_ctype != content_type {
                    return Err(CpError::ConnectionError(format!(
                        "Failed #{} [HTTP GET {}]: content-type `{}` does not match expected `{}`. headers: {:?}",
                        attempt,
                        url,
                        actual_ctype,
                        content_type,
                        resp.headers()
                    )));
                }
                return Ok(resp.text().await?);
            }
            Err(e) => {
                log::trace!(
                    "Failed #{} [HTTP GET {}]: {}. Retrying in {}ms...",
                    attempt,
                    url,
                    e,
                    retry_interval
                );
                std::thread::sleep(std::time::Duration::from_millis(retry_interval));
                retry_interval *= 2;
            }
        }
    }
    Err(CpError::ConnectionError(format!(
        "Reach max retries [HTTP GET {}]",
        &url
    )))
}

fn sync_urls(urls: Vec<String>, max_retry: u8, retry_interval: u64, content_type: &str) -> CpResult<LazyFrame> {
    let mut rt_builder = tokio::runtime::Builder::new_multi_thread();
    rt_builder.enable_all();
    let rt = rt_builder.build().unwrap();
    rt.block_on(async move { async_urls(urls, max_retry, retry_interval, content_type).await })
}

async fn async_urls(urls: Vec<String>, max_retry: u8, retry_interval: u64, content_type: &str) -> CpResult<LazyFrame> {
    let results: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));
    let errors: Arc<Mutex<Vec<CpError>>> = Arc::new(Mutex::new(vec![]));
    ctx_run_n_async!(
        urls.len(),
        urls.as_slice(),
        async move |url: &String, resps: Arc<Mutex<Vec<String>>>, errs: Arc<Mutex<Vec<CpError>>>| {
            let client = reqwest::Client::new();
            match async_url(&client, url, max_retry, retry_interval, content_type).await {
                Ok(result) => resps.lock()?.push(result),
                Err(e) => errs.lock()?.push(e),
            }
            Ok::<(), CpError>(())
        },
        results.clone(),
        errors.clone()
    );
    errors.lock()?.iter().for_each(|e| {
        log::error!("{}", e);
    });
    match content_type {
        "application/json" => {
            let df = vec_str_json_to_df(results.lock()?.as_slice())?;
            Ok(df.lazy())
        }
        invalid => Err(CpError::TaskError(
            "Invalid content type",
            format!("Parsing of this content type as a dataframe not supported: {}", invalid),
        )),
    }
}

pub fn get_urls(base: LazyFrame, url_column: Expr) -> CpResult<Vec<String>> {
    let df = match base.select([url_column.alias("url")]).collect() {
        Ok(x) => x,
        Err(e) => {
            log::error!("{}", e);
            DataFrame::empty()
        }
    };
    let df_type = df.column("url")?.dtype().clone();
    let url_column = df.column("url")?.drop_nulls().unique()?;
    let url_series = match url_column.try_str() {
        Some(x) => x,
        None => {
            return Err(CpError::ComponentError(
                "Invalid HTTP url column",
                format!(
                    "Expected a column of strings, received a column of {:?} in {:?}",
                    df_type, url_column
                ),
            ));
        }
    };
    let mut urls = vec![];
    url_series.for_each(|x| x.map_or_else(|| {}, |url| urls.push(url.to_owned())));
    Ok(urls)
}

#[async_trait]
impl Request for HttpBatchRequest {
    fn connection_type(&self) -> &str {
        "http_batch"
    }

    fn name(&self) -> &str {
        self.output.as_str()
    }

    fn run(&self, frame: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        let urls = match get_urls(frame, self.url_column.clone()) {
            Ok(x) => x,
            Err(e) => {
                log::error!("{}", e);
                vec![]
            }
        };
        let frame = match sync_urls(urls, self.max_retry, self.init_retry_interval_ms, &self.content_type) {
            Ok(x) => x,
            Err(e) => {
                log::error!("{}", e);
                DataFrame::empty().lazy()
            }
        };
        let frame_modelled = if let Some(schema) = &self.schema {
            frame.with_columns(schema.clone())
        } else {
            frame
        };
        let mut bcast = ctx.get_broadcast(self.name(), self.connection_type())?;
        bcast.broadcast(frame_modelled.clone())?;
        Ok(())
    }

    async fn fetch(&self, frame: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        let urls = match get_urls(frame, self.url_column.clone()) {
            Ok(x) => x,
            Err(e) => {
                log::error!("{}", e);
                vec![]
            }
        };
        let frame = match async_urls(urls, self.max_retry, self.init_retry_interval_ms, &self.content_type).await {
            Ok(x) => x,
            Err(e) => {
                log::error!("{}", e);
                DataFrame::empty().lazy()
            }
        };
        let frame_modelled = if let Some(schema) = &self.schema {
            frame.with_columns(schema.clone())
        } else {
            frame
        };
        let mut bcast = ctx.get_async_broadcast(self.name(), self.connection_type())?;
        match bcast.broadcast(frame_modelled.clone()) {
            Ok(_) => log::info!("Sent update for frame {}", &self.output),
            Err(e) => log::error!("{}: {:?}", &self.output, e),
        };
        Ok(())
    }
}

impl RequestConfig for HttpBatchConfig {
    fn emplace(&mut self, ctx: &DefaultPipelineContext, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        self.http_batch.output.insert_value_from_context(context)?;
        model_emplace!(self.http_batch, ctx, context);
        self.http_batch.url_column.insert_value_from_context(context)?;
        Ok(())
    }
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        valid_or_insert_error!(errors, self.http_batch.url_column, "source[http_batch].url_column");
        valid_or_insert_error!(errors, self.http_batch.output, "source[http_batch].output");
        if let Some(model_fields) = &self.http_batch.model_fields {
            for (key_kw, field_kw) in model_fields.iter() {
                valid_or_insert_error!(errors, key_kw, "source[http_batch].model.key");
                valid_or_insert_error!(errors, field_kw, "source[http_batch].model.field");
            }
        }
        errors
    }
    fn transform(&self) -> Box<dyn Request> {
        // By here the model_fields should be completely populated.
        let schema = self.http_batch.model_fields.as_ref().map(|x| {
            ModelConfig {
                label: "".to_string(),
                fields: x.clone(),
            }
            .columns()
            .expect("failed to build schema")
        });

        let options = self.http_batch.options.as_ref();
        Box::new(HttpBatchRequest {
            url_column: self.http_batch.url_column.value().expect("url_column").clone(),
            output: self.http_batch.output.value().expect("output").to_owned(),
            content_type: self.http_batch.content_type.clone(),
            init_retry_interval_ms: options
                .map(|opt| {
                    opt.init_retry_interval_ms
                        .unwrap_or(DEFAULT_HTTP_REQ_INIT_RETRY_INTERVAL_MS)
                })
                .unwrap_or(DEFAULT_HTTP_REQ_INIT_RETRY_INTERVAL_MS),
            max_retry: options
                .map(|opt| opt.max_retry.unwrap_or(DEFAULT_HTTP_REQ_MAX_RETRY))
                .unwrap_or(DEFAULT_HTTP_REQ_MAX_RETRY),
            schema,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use httpmock::{Method::GET, Mock, MockServer};
    use polars::{df, frame::DataFrame, prelude::IntoLazy};
    use serde::{Deserialize, Serialize};

    use crate::{
        async_st,
        parser::{
            http::{HttpMethod, HttpOptionsConfig},
            keyword::{Keyword, StrKeyword},
        },
        pipeline::context::{DefaultPipelineContext, PipelineContext},
        task::request::{
            common::RequestConfig,
            config::{HttpBatchConfig, HttpReqConfig},
        },
        util::{
            common::vec_str_json_to_df,
            test::{DummyData, assert_frame_equal},
        },
    };

    #[derive(Serialize, Deserialize)]
    struct SampleAct {
        id: String,
        label: Option<String>,
    }

    fn mock_server(server: &MockServer) -> Vec<Mock> {
        DummyData::json_actions()
            .iter()
            .map(|j| {
                let act: SampleAct = serde_yaml_ng::from_str(j).unwrap();
                server.mock(|when, then| {
                    when.method(GET).path("/actions").query_param("id", act.id);
                    then.status(200).header("content-type", "application/json").body(j);
                })
            })
            .collect()
    }
    fn get_url_df(server: &MockServer) -> DataFrame {
        let urls = DummyData::json_actions()
            .iter()
            .map(|j| {
                let act: SampleAct = serde_yaml_ng::from_str(j).unwrap();
                server.url(format!("/actions?id={}", act.id))
            })
            .collect::<Vec<String>>();
        df!( "url" => urls ).unwrap()
    }
    fn get_expected() -> DataFrame {
        vec_str_json_to_df(&DummyData::json_actions()).unwrap()
    }

    #[test]
    fn valid_http_batch_config_to_http_batch_request_sync() {
        let mut source_config = HttpBatchConfig {
            http_batch: HttpReqConfig {
                content_type: "application/json".to_owned(),
                model: None,
                method: HttpMethod::Get,
                output: StrKeyword::with_symbol("output"),
                options: Some(HttpOptionsConfig {
                    max_retry: None,
                    init_retry_interval_ms: None,
                }),
                url_column: serde_yaml_ng::from_str("url").unwrap(),
                url_params: None,
                model_fields: Some(serde_yaml_ng::from_str("{id: str, label: str}").unwrap()),
            },
        };
        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>("output: OUT").unwrap();
        let ctx = Arc::new(DefaultPipelineContext::with_results(&["OUT"], 2));
        let _ = source_config.emplace(&ctx, &context);
        let errors = source_config.validate();
        assert!(errors.is_empty());
        // with connection sync
        {
            let actual_node = source_config.transform();
            let server = MockServer::start();
            let mocks = mock_server(&server);
            let url_df = get_url_df(&server);
            actual_node.run(url_df.lazy(), ctx.clone()).unwrap();
            mocks.iter().for_each(|m| {
                m.assert();
            });
            let actual = ctx.extract_clone_result("OUT").unwrap();
            let expected = get_expected();
            assert_frame_equal(expected, actual);
        }
        // without connection sync
        {
            let actual_node = source_config.transform();
            let server = MockServer::start();
            let url_df = get_url_df(&server);
            actual_node.run(url_df.lazy(), ctx.clone()).unwrap();
            let actual_with_messages = ctx.extract_clone_result("OUT").unwrap();
            let actual = actual_with_messages
                .select(["id", "label"])
                .unwrap()
                .drop_nulls::<String>(None)
                .unwrap();
            let expected = df!( "id" => Vec::<String>::new(), "label" => Vec::<String>::new() ).unwrap();
            assert_frame_equal(expected, actual);
        }
        // with connection async
        {
            async_st!(async || {
                let actual_node = source_config.transform();
                let server = MockServer::start();
                let mocks = mock_server(&server);
                let url_df = get_url_df(&server);
                actual_node.fetch(url_df.lazy(), ctx.clone()).await.unwrap();
                mocks.iter().for_each(|m| {
                    m.assert();
                });
                let actual = ctx.extract_clone_result("OUT").unwrap();
                let expected = get_expected();
                assert_frame_equal(expected, actual);
            });
        }
        // without connection async
        {
            async_st!(async || {
                let actual_node = source_config.transform();
                let server = MockServer::start();
                let url_df = get_url_df(&server);
                actual_node.fetch(url_df.lazy(), ctx.clone()).await.unwrap();
                let actual_with_messages = ctx.extract_clone_result("OUT").unwrap();
                let actual = actual_with_messages
                    .select(["id", "label"])
                    .unwrap()
                    .drop_nulls::<String>(None)
                    .unwrap();
                let expected = df!( "id" => Vec::<String>::new(), "label" => Vec::<String>::new() ).unwrap();
                assert_frame_equal(expected, actual);
            });
        }
    }
}
