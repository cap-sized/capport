use serde::{Deserialize, Serialize};
use yaml_rust2::Yaml;

use crate::{
    pipeline::{
        common::{HasTask, PipelineTask},
        context::PipelineContext,
    },
    task::common::{deserialize_arg_str, yaml_to_task_arg_str},
    transform::expr::parse_str_to_col_expr,
    util::{
        error::{CpError, CpResult},
        json::vec_str_json_to_df,
    },
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

async fn get_http_urls(urls: Vec<String>) -> CpResult<Vec<String>> {
    let client = Arc::new(reqwest::Client::new());
    let handles = urls
        .iter()
        .map(|urlref| {
            let cli = client.clone();
            let req = cli.get(urlref);
            tokio::task::spawn(async move { req.send().await })
        })
        .collect::<Vec<_>>();
    let mut responses = vec![];
    for result in handles {
        match result.await {
            Ok(x) => match x {
                Ok(r) => responses.push(r.text().await),
                Err(e) => return Err(CpError::ConnectionError(e.to_string())),
            },
            Err(e) => return Err(CpError::TaskError("Tokio error", e.to_string())),
        }
    }
    let mut texts = vec![];
    for response in responses {
        match response {
            Ok(x) => texts.push(x),
            Err(e) => return Err(CpError::TaskError("Failed to decode as text", e.to_string())),
        }
    }
    Ok(texts)
}

fn run_get_http_urls(url: Vec<String>) -> CpResult<Vec<String>> {
    let mut rt_builder = tokio::runtime::Builder::new_current_thread();
    rt_builder.enable_all();
    let rt = rt_builder.build().unwrap();
    rt.block_on(get_http_urls(url))
}

fn run<S>(ctx: Arc<dyn PipelineContext<LazyFrame, S>>, task: HttpRequestTask) -> CpResult<()> {
    let mtd = task.method.clone().unwrap_or("get".to_owned()).to_lowercase();
    let urls = task.to_urls(ctx.clone())?;
    match mtd.as_str() {
        "get" => task.get(urls, ctx),
        _ => Err(CpError::TaskError(
            "Invalid method for HttpRequestTask",
            format!("No method found: `{}`", &mtd),
        )),
    }
}

impl HttpRequestTask {
    fn get<S>(&self, urls: Vec<String>, ctx: Arc<dyn PipelineContext<LazyFrame, S>>) -> CpResult<()> {
        let results = run_get_http_urls(urls)?;
        let fmt = self.format.clone().unwrap_or("json".to_owned()).to_lowercase();
        let df = match fmt {
            _ => vec_str_json_to_df(&results)?,
        };
        ctx.insert_result(&self.df_to, df.lazy())?;
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

    use httpmock::{Mock, prelude::*};
    use polars::{
        df,
        prelude::{IntoLazy, LazyFrame},
    };
    use serde::{Deserialize, Serialize};

    use crate::{
        pipeline::{
            common::HasTask,
            context::{DefaultContext, PipelineContext},
        },
        util::{
            common::{DummyData, yaml_from_str},
            json::vec_str_json_to_df,
        },
    };

    use super::HttpRequestTask;

    #[derive(Serialize, Deserialize)]
    struct SampleAct {
        id: String,
        label: Option<String>,
    }

    fn mock_server(server: &MockServer) -> Vec<Mock> {
        DummyData::json_actions()
            .iter()
            .map(|j| {
                let act: SampleAct = serde_json::from_str(j).unwrap();
                server.mock(|when, then| {
                    when.method(GET).path("/actions").query_param("id", act.id);
                    then.status(200)
                        .header("content-type", "application/json")
                        .body(j.to_owned());
                })
            })
            .collect()
    }
    fn get_urls(server: &MockServer) -> Vec<String> {
        DummyData::json_actions()
            .iter()
            .map(|j| {
                let act: SampleAct = serde_json::from_str(j).unwrap();
                server.url(format!("/actions?id={}", act.id))
            })
            .collect()
    }
    fn get_ctx(urls: Vec<String>) -> Arc<DefaultContext<LazyFrame, ()>> {
        let raw_ctx = DefaultContext::default();
        raw_ctx
            .insert_result(
                "URLS",
                df![
                    "url" => urls
                ]
                .unwrap()
                .lazy(),
            )
            .unwrap();
        Arc::new(raw_ctx)
    }

    #[test]
    fn valid_req() {
        let optionals_pairs = [
            "
format: json
method: get
",
            "
method: get
",
            "
",
            "
format: JSON
method: GET
",
        ]
        .iter()
        .map(|x| x.to_owned())
        .collect::<Vec<&str>>();
        for optional in optionals_pairs {
            let server = MockServer::start();
            let mocks = mock_server(&server);
            let urls = get_urls(&server);
            let ctx = get_ctx(urls);
            let config = format!(
                "
---
df_from: URLS
df_to: DATA
url_column: url
{}
",
                optional
            );
            let args = yaml_from_str(&config).unwrap();
            let t = HttpRequestTask::lazy_task(&args).unwrap();
            t(ctx.clone()).unwrap();
            let actual = ctx.clone_result("DATA").unwrap().collect().unwrap();
            let expected = vec_str_json_to_df(&DummyData::json_actions()).unwrap();
            mocks.iter().for_each(|m| {
                m.assert();
            });
            assert_eq!(actual, expected);
        }
    }
}
