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
            // TODO: This may timeout! Need to have a backoff retry strategy
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
    // TODO: This is unnecessary if it is one url, and overkill if it is like 10000
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
        let df = match fmt.as_str() {
            "json" => vec_str_json_to_df(&results)?, // json default
            _ => panic!("Should never reach here"),
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

#[derive(Serialize, Deserialize, Clone)]
pub struct UrlParam {
    pub df_from: String,
    pub param_column: String,
    pub template: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct HttpSingleRequestTask {
    pub df_to: String,
    pub template: String,
    pub url_params: Vec<UrlParam>,
    pub format: Option<String>, // defaults to JSON
    pub method: Option<String>, // defaults to GET
}

impl HttpSingleRequestTask {
    fn run<S>(&self, ctx: Arc<dyn PipelineContext<LazyFrame, S>>) -> CpResult<()> {
        let method = self.method.clone().unwrap_or("get".to_owned()).to_lowercase();
        let url = self.construct_url(&ctx)?;
        match method.as_str() {
            "get" => self.get(&url, &ctx),
            _ => Err(CpError::TaskError(
                "Invalid method for HttpSingleRequestTask",
                format!("No method found: `{}`", &method),
            )),
        }
    }

    fn construct_url<S>(&self, ctx: &Arc<dyn PipelineContext<LazyFrame, S>>) -> CpResult<String> {
        let mut url = self.template.clone();

        let query_string = &self
            .url_params
            .iter()
            .map(|url_param| {
                let template = url_param
                    .template
                    .clone()
                    .unwrap_or(format!("{}={{}}", url_param.param_column));
                let lf = ctx.clone_result(&url_param.df_from)?;
                let param_column_expr = match parse_str_to_col_expr(&url_param.param_column) {
                    Some(x) => x,
                    None => {
                        return Err(CpError::TaskError(
                            "HttpSingleRequestTask",
                            format!("Failed to parse param_column: {}", &url_param.param_column),
                        ));
                    }
                };
                let df = lf.select([param_column_expr.alias("param")]).collect()?;
                let param_val = df
                    .column("param")?
                    .to_owned()
                    .rechunk()
                    .phys_iter()
                    .map(|x| {
                        let url = x.get_str();
                        match url {
                            Some(x) => x.to_owned(),
                            None => x.to_string(),
                        }
                    })
                    .collect::<Vec<String>>()
                    .join(",");
                Ok(template.replace("{}", &param_val))
            })
            .collect::<Result<Vec<String>, CpError>>()?
            .join("&");

        if !query_string.is_empty() {
            url = format!("{}?{}", url, query_string);
        }

        Ok(url)
    }

    fn get<S>(&self, url: &str, ctx: &Arc<dyn PipelineContext<LazyFrame, S>>) -> CpResult<()> {
        let results = run_get_http_urls(vec![url.to_string()])?;
        let fmt = self.format.clone().unwrap_or("json".to_owned()).to_lowercase();
        let df = match fmt.as_str() {
            "json" => vec_str_json_to_df(&results)?, // json default
            _ => panic!("Should never reach here"),
        };
        ctx.insert_result(&self.df_to, df.lazy())?;
        Ok(())
    }
}

impl HasTask for HttpSingleRequestTask {
    fn lazy_task<S>(args: &Yaml) -> CpResult<PipelineTask<LazyFrame, S>> {
        let arg_str = yaml_to_task_arg_str(args, "HttpSingleRequestTask")?;
        let task: HttpSingleRequestTask = deserialize_arg_str(&arg_str, "HttpSingleRequestTask")?;
        Ok(Box::new(move |ctx| task.clone().run(ctx)))
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
    use super::HttpSingleRequestTask;

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
                    then.status(200).header("content-type", "application/json").body(j);
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

    #[test]
    fn valid_single_req() {
        let url_params_configs = [
            "
url_params:
    - df_from: PLAYERS_DF
      param_column: players

    - df_from: TEAMS_DF
      param_column: teams
",
            "
url_params:
    - df_from: PLAYERS_DF
      param_column: players
      template: players={}

    - df_from: TEAMS_DF
      param_column: teams
      template: teams={}
",
        ]
        .iter()
        .map(|x| x.to_owned())
        .collect::<Vec<&str>>();

        for url_params_config in url_params_configs {
            let players = [8478401, 8478402];
            let teams = ["EDM", "TOR"];

            let server = MockServer::start();
            let mock: Mock = server.mock(|when, then| {
                when.method(GET)
                    .path("/v1/meta")
                    .query_param(
                        "players",
                        players.iter().map(|n| n.to_string()).collect::<Vec<String>>().join(","),
                    )
                    .query_param("teams", teams.join(","));
                then.status(200)
                    .header("content-type", "application/json")
                    .body(DummyData::meta_info());
            });

            let ctx = Arc::new(DefaultContext::default());
            ctx.insert_result("PLAYERS_DF", df!["players" => players].unwrap().lazy())
                .unwrap();

            ctx.insert_result("TEAMS_DF", df!["teams" => teams].unwrap().lazy())
                .unwrap();

            let template = server.url("/v1/meta");
            let config = format!(
                "
---
df_to: DATA
template: {}
{}",
                template, url_params_config
            );

            let args = yaml_from_str(&config).unwrap();

            let t = HttpSingleRequestTask::lazy_task(&args).unwrap();
            t(ctx.clone()).unwrap();

            let actual = ctx.clone_result("DATA").unwrap().collect().unwrap();
            let expected = vec_str_json_to_df(&[DummyData::meta_info()]).unwrap();

            mock.assert();
            assert_eq!(actual, expected);
        }
    }
}
