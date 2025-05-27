use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use polars::prelude::{Expr, IntoLazy, LazyFrame};

use crate::{
    ctx_run_n_async, ctx_run_n_threads,
    frame::common::{FrameAsyncBroadcastHandle, FrameBroadcastHandle},
    model::common::ModelConfig,
    parser::keyword::Keyword,
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    util::{
        common::vec_str_json_to_df,
        error::{CpError, CpResult},
    },
    valid_or_insert_error,
};

use super::{
    common::{Request, RequestConfig},
    config::HttpBatchConfig,
};

const DEFAULT_HTTP_REQ_WORKERS: u8 = 8;
const DEFAULT_HTTP_REQ_MAX_RETRY: u8 = 8;
const DEFAULT_HTTP_REQ_INIT_RETRY_INTERVAL_MS: u64 = 1000;

pub struct HttpBatchRequest {
    url_column: Expr,
    output: String,
    schema: Option<Vec<Expr>>,
    workers: u8,
    max_retry: u8,
    init_retry_interval_ms: u64,
}

fn sync_url(
    client: &reqwest::blocking::Client,
    url: &str,
    max_retry: u8,
    init_retry_interval: u64,
) -> CpResult<String> {
    let mut retry_interval = init_retry_interval;
    for attempt in 0..max_retry {
        let req = client.get(url);
        log::debug!("#{}: HTTP GET {}", attempt, url);
        match req.send() {
            Ok(resp) => return Ok(resp.text()?),
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

async fn async_url(client: &reqwest::Client, url: &str, max_retry: u8, init_retry_interval: u64) -> CpResult<String> {
    let mut retry_interval = init_retry_interval;
    for attempt in 0..max_retry {
        let req = client.get(url);
        log::debug!("#{}: HTTP GET {}", attempt, url);
        match req.send().await {
            Ok(resp) => return Ok(resp.text().await?),
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

fn sync_urls(urls: Vec<String>, max_threads: u8, max_retry: u8, retry_interval: u64) -> CpResult<LazyFrame> {
    let results: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));
    let errors: Arc<Mutex<Vec<CpError>>> = Arc::new(Mutex::new(vec![]));
    type UrlResErrType<'a> = (&'a [String], Arc<Mutex<Vec<String>>>, Arc<Mutex<Vec<CpError>>>);
    ctx_run_n_threads!(
        max_threads,
        urls.as_slice(),
        move |(urls, resps, errs): UrlResErrType| {
            let client = reqwest::blocking::Client::new();
            for url in urls {
                match sync_url(&client, url, max_retry, retry_interval) {
                    Ok(result) => resps.lock()?.push(result),
                    Err(e) => errs.lock()?.push(e),
                }
            }
            Ok::<(), CpError>(())
        },
        results.clone(),
        errors.clone()
    );
    errors.lock()?.iter().for_each(|e| {
        log::error!("{}", e);
    });
    let df = vec_str_json_to_df(results.lock()?.as_slice())?;
    Ok(df.lazy())
}

async fn async_urls(urls: Vec<String>, max_threads: u8, max_retry: u8, retry_interval: u64) -> CpResult<LazyFrame> {
    let results: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));
    let errors: Arc<Mutex<Vec<CpError>>> = Arc::new(Mutex::new(vec![]));
    ctx_run_n_async!(
        max_threads,
        urls.as_slice(),
        async move |url: &String, resps: Arc<Mutex<Vec<String>>>, errs: Arc<Mutex<Vec<CpError>>>| {
            let client = reqwest::Client::new();
            match async_url(&client, url, max_retry, retry_interval).await {
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
    let df = vec_str_json_to_df(results.lock()?.as_slice())?;
    Ok(df.lazy())
}

fn get_urls(base: LazyFrame, url_column: Expr) -> CpResult<Vec<String>> {
    let df = base.select([url_column.alias("url")]).collect()?;
    let df_type = df.column("url")?.dtype().clone();
    let url_column = df.column("url")?.drop_nulls().unique()?;
    let url_series = match url_column.as_series().unwrap().try_str() {
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
        let urls = get_urls(frame, self.url_column.clone())?;
        let frame = sync_urls(urls, self.workers, self.max_retry, self.init_retry_interval_ms)?;
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
        let urls = get_urls(frame, self.url_column.clone())?;
        let frame = async_urls(urls, self.workers, self.max_retry, self.init_retry_interval_ms).await?;
        let frame_modelled = if let Some(schema) = &self.schema {
            frame.with_columns(schema.clone())
        } else {
            frame
        };
        let mut bcast = ctx.get_async_broadcast(self.name(), self.connection_type())?;
        bcast.broadcast(frame_modelled.clone()).await?;
        Ok(())
    }
}

impl RequestConfig for HttpBatchConfig {
    fn emplace(&mut self, ctx: &DefaultPipelineContext, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        self.http_batch.output.insert_value_from_context(context)?;
        if let Some(mut model_name) = self.http_batch.model.take() {
            model_name.insert_value_from_context(context)?;
            if let Some(name) = model_name.value() {
                let model = ctx.get_model(name)?;
                self.http_batch.model_fields = Some(model.fields);
            }
            let _ = self.http_batch.model.insert(model_name.clone());
        }
        if let Some(model_fields) = self.http_batch.model_fields.take() {
            let model = ModelConfig {
                label: "".to_string(),
                fields: model_fields,
            };
            let fields = model.substitute_model_fields(context)?;
            let _ = self.http_batch.model_fields.insert(fields);
        }
        self.http_batch.url_column.insert_value_from_context(context)?;
        Ok(())
    }
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        valid_or_insert_error!(errors, self.http_batch.url_column, "source[http_batch].url_column");
        valid_or_insert_error!(errors, self.http_batch.output, "source[http_batch].output");
        if let Some(model_fields) = &self.http_batch.model_fields {
            for (key_kw, field_kw) in model_fields {
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
            init_retry_interval_ms: options
                .map(|opt| {
                    opt.init_retry_interval_ms
                        .unwrap_or(DEFAULT_HTTP_REQ_INIT_RETRY_INTERVAL_MS)
                })
                .unwrap_or(DEFAULT_HTTP_REQ_INIT_RETRY_INTERVAL_MS),
            workers: options
                .map(|opt| opt.workers.unwrap_or(DEFAULT_HTTP_REQ_WORKERS))
                .unwrap_or(DEFAULT_HTTP_REQ_WORKERS),
            max_retry: options
                .map(|opt| opt.max_retry.unwrap_or(DEFAULT_HTTP_REQ_MAX_RETRY))
                .unwrap_or(DEFAULT_HTTP_REQ_MAX_RETRY),
            schema,
        })
    }
}
