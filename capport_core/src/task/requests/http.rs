use serde::{Deserialize, Serialize};
use yaml_rust2::Yaml;

use crate::{
    pipeline::{
        common::{HasTask, PipelineTask},
        context::PipelineContext,
    },
    task::common::{deserialize_arg_str, yaml_to_task_arg_str},
    transform::expr::parse_str_to_col_expr,
    util::error::{CpError, CpResult},
};
use polars::prelude::*;

#[derive(Serialize, Deserialize, Clone)]
pub struct HttpRequestTask {
    pub df_from: String,
    pub df_to: String,
    pub url_column: String,
    pub format: Option<String>, // defaults to JSON
    pub method: Option<String>, // defaults to GET
}

fn run<S>(ctx: Arc<dyn PipelineContext<LazyFrame, S>>, task: HttpRequestTask) -> CpResult<()> {
    let mtd = task.method.clone().unwrap_or("get".to_owned().to_lowercase());
    let urls = task.to_urls(ctx.clone())?;
    match mtd.as_str() {
        "get" => task.get(&urls, ctx),
        _ => Err(CpError::TaskError(
            "Invalid method for HttpRequestTask",
            format!("No method found: `{}`", &mtd),
        )),
    }
}

impl HttpRequestTask {
    fn get<S>(&self, urls: &Vec<String>, ctx: Arc<dyn PipelineContext<LazyFrame, S>>) -> CpResult<()> {
        let client = reqwest::blocking::Client::new();
        let mut jsons: Vec<String> = vec![];
        for url in urls {
            let req = client.get(url);
            let req_log = format!("{:?}, url {}", req, &url);
            let resp = match req.send() {
                Ok(x) => x,
                Err(e) => {
                    return Err(CpError::ConnectionError(format!(
                        "{:?} failed: {}",
                        &req_log,
                        e.to_string()
                    )));
                }
            };
            match resp.text() {
                Ok(x) => jsons.push(x),
                Err(e) => {
                    return Err(CpError::TaskError(
                        "HttpRequestTask",
                        format!("Response to GET[{}] cannot be converted to text: {:?}", &url, e),
                    ));
                }
            }
        }
        println!("{:?}", &jsons);
        // ctx.insert_result(&self.df_to, result)
        Ok(())
    }
    fn to_urls<S>(&self, ctx: Arc<dyn PipelineContext<LazyFrame, S>>) -> CpResult<Vec<String>> {
        let lf = ctx.clone_result(&self.df_from)?;
        let url_expr = match parse_str_to_col_expr(&self.url_column) {
            Some(x) => x,
            None => {
                return Err(CpError::TaskError(
                    "HttpRequestTask",
                    format!("Failed to parse url_column: {}", &self.url_column),
                ));
            }
        };
        let df = lf.select([url_expr.alias("url")]).collect()?;
        let urls = df
            .column("url")?
            .phys_iter()
            .map(|x| {
                let url = x.get_str();
                match url {
                    Some(x) => x.to_owned(),
                    None => x.to_string(),
                }
            })
            .collect::<Vec<String>>();
        Ok(urls)
    }
}

impl HasTask for HttpRequestTask {
    fn lazy_task<S>(args: &Yaml) -> CpResult<PipelineTask<LazyFrame, S>> {
        let arg_str = yaml_to_task_arg_str(args, "HttpRequestTask")?;
        let task: HttpRequestTask = deserialize_arg_str(&arg_str, "HttpRequestTask")?;
        Ok(Box::new(move |ctx| run(ctx, task.clone())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use polars::{df, prelude::IntoLazy};

    use crate::{
        pipeline::{
            common::HasTask,
            context::{DefaultContext, PipelineContext},
        },
        util::common::yaml_from_str,
    };

    use super::HttpRequestTask;

    #[test]
    fn valid_req() {
        let raw_ctx = DefaultContext::default();
        raw_ctx
            .insert_result(
                "URLS",
                df![
                    "url" => ["http://httpbin.org/json"]
                ]
                .unwrap()
                .lazy(),
            )
            .unwrap();
        let ctx = Arc::new(raw_ctx);
        let config = format!(
            "
---
df_from: URLS
df_to: DATA
url_column: url
format: json
method: get
"
        );
        let args = yaml_from_str(&config).unwrap();
        let t = HttpRequestTask::lazy_task(&args).unwrap();
        t(ctx.clone()).unwrap();
        // assert!(false);
    }
}
