use crate::frame::common::{FrameAsyncBroadcastHandle, FrameBroadcastHandle};
use crate::model::common::ModelConfig;
use crate::parser::keyword::Keyword;
use crate::pipeline::context::{DefaultPipelineContext, PipelineContext};
use crate::task::request::common::{Request, RequestConfig};
use crate::task::request::config::{HttpParamConfig, HttpSingleConfig};
use crate::task::request::http_batch::get_urls;
use crate::util::common::{
    DEFAULT_HTTP_REQ_INIT_RETRY_INTERVAL_MS, DEFAULT_HTTP_REQ_MAX_RETRY, explode_df, str_json_to_df,
};
use crate::util::error::{CpError, CpResult};
use crate::{model_emplace, valid_or_insert_error};
use async_trait::async_trait;
use polars::prelude::{Expr, IntoLazy, LazyFrame};
use serde_yaml_ng::Mapping;
use std::sync::Arc;

pub struct HttpSingleRequest {
    url_column: Expr,
    url_params: Option<Vec<HttpParamConfig>>,
    output: String,
    content_type: String,
    schema: Option<Vec<Expr>>,
    max_retry: u8,
    init_retry_interval_ms: u64,
}

impl HttpSingleRequest {
    fn get_column_name(&self, expr: Expr) -> Option<String> {
        match expr {
            Expr::Column(name) => Some(name.as_str().to_owned()),
            _ => None,
        }
    }

    fn construct_url(&self, frame: &LazyFrame, ctx: &Arc<DefaultPipelineContext>) -> CpResult<String> {
        let urls = get_urls(frame.clone(), self.url_column.clone())?;
        if urls.len() != 1 {
            return Err(CpError::ComponentError(
                "Invalid url dataframe",
                format!(
                    "Expected only 1 row in url data frame for HttpSingleRequest, received {:?} rows",
                    urls.len()
                ),
            ));
        }

        let mut url = urls[0].clone();

        if self.url_params.is_none() {
            return Ok(url);
        }

        let url_params = self.url_params.clone().unwrap_or_default();

        let query_string = url_params
            .iter()
            .map(|url_param| {
                let col_expr = url_param.col.value().expect("url_param.col").clone();

                let template = url_param
                    .template
                    .clone()
                    .unwrap_or(format!("{}={{}}", self.get_column_name(col_expr.clone()).unwrap()));

                let lf = ctx.extract_result(url_param.df.value().expect("url_param.df"))?;

                let df = lf.select([col_expr.clone().alias("param")]).collect()?;
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
                    .join(url_param.separator.clone().unwrap_or(",".to_string()).as_str());
                Ok(template.replace("{}", &param_val))
            })
            .collect::<Result<Vec<String>, CpError>>()?
            .join("&");

        if !query_string.is_empty() {
            url = format!("{}?{}", url, query_string);
        }

        Ok(url)
    }
}

fn sync_url(url: &str, max_retry: u8, retry_interval: u64, content_type: &str) -> CpResult<LazyFrame> {
    let client = reqwest::blocking::Client::new();
    let result = crate::task::request::http_batch::sync_url(&client, url, max_retry, retry_interval, content_type)?;
    let result_df = str_json_to_df(&result)?;
    Ok(explode_df(&result_df)?.lazy())
}

async fn async_url(url: &str, max_retry: u8, retry_interval: u64, content_type: &str) -> CpResult<LazyFrame> {
    let client = reqwest::Client::new();
    let result =
        crate::task::request::http_batch::async_url(&client, url, max_retry, retry_interval, content_type).await?;
    let result_df = str_json_to_df(&result)?;
    Ok(explode_df(&result_df)?.lazy())
}

#[async_trait]
impl Request for HttpSingleRequest {
    fn connection_type(&self) -> &str {
        "http"
    }

    fn name(&self) -> &str {
        self.output.as_str()
    }

    fn run(&self, frame: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        let url = self.construct_url(&frame, &ctx)?;
        let frame = sync_url(&url, self.max_retry, self.init_retry_interval_ms, &self.content_type)?;
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
        let url = self.construct_url(&frame, &ctx)?;
        let frame = async_url(&url, self.max_retry, self.init_retry_interval_ms, &self.content_type).await?;
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

impl RequestConfig for HttpSingleConfig {
    fn emplace(&mut self, ctx: &DefaultPipelineContext, context: &Mapping) -> CpResult<()> {
        self.http_single.output.insert_value_from_context(context)?;
        model_emplace!(self.http_single, ctx, context);
        self.http_single.url_column.insert_value_from_context(context)?;
        if let Some(url_params) = &mut self.http_single.url_params {
            for url_param in url_params {
                url_param.df.insert_value_from_context(context)?;
            }
        }
        Ok(())
    }

    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        valid_or_insert_error!(errors, self.http_single.url_column, "source[http_single].url_column");
        if let Some(url_params) = &self.http_single.url_params {
            for url_param in url_params {
                valid_or_insert_error!(errors, url_param.df, "source[http_single].url_params.df");
            }
        }
        valid_or_insert_error!(errors, self.http_single.output, "source[http_single].output");
        if let Some(model_fields) = &self.http_single.model_fields {
            for (key_kw, field_kw) in model_fields {
                valid_or_insert_error!(errors, key_kw, "source[http_single].model.key");
                valid_or_insert_error!(errors, field_kw, "source[http_single].model.field");
            }
        }
        errors
    }

    fn transform(&self) -> Box<dyn Request> {
        let schema = self.http_single.model_fields.as_ref().map(|x| {
            ModelConfig {
                label: "".to_string(),
                fields: x.clone(),
            }
            .columns()
            .expect("failed to build schema")
        });

        let options = self.http_single.options.as_ref();
        Box::new(HttpSingleRequest {
            url_column: self.http_single.url_column.value().expect("url_column").clone(),
            url_params: self.http_single.url_params.clone(),
            output: self.http_single.output.value().expect("output").to_owned(),
            content_type: self.http_single.content_type.clone(),
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
    use crate::async_st;
    use crate::parser::http::HttpMethod;
    use crate::parser::keyword::{Keyword, StrKeyword};
    use crate::pipeline::context::{DefaultPipelineContext, PipelineContext};
    use crate::task::request::common::RequestConfig;
    use crate::task::request::config::{HttpParamConfig, HttpReqConfig, HttpSingleConfig};
    use crate::util::common::{explode_df, str_json_to_df};
    use crate::util::test::DummyData;
    use httpmock::Method::GET;
    use httpmock::{Mock, MockServer};
    use polars::df;
    use polars::prelude::IntoLazy;
    use std::sync::Arc;

    fn mock_server<'a>(server: &'a MockServer, players: [i32; 2], teams: [&'a str; 2]) -> Mock<'a> {
        server.mock(|when, then| {
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
        })
    }

    #[test]
    fn valid_http_single_config_to_http_single_request() {
        let mut source_config = HttpSingleConfig {
            http_single: HttpReqConfig {
                content_type: "application/json".to_owned(),
                model: None,
                method: HttpMethod::Get,
                output: StrKeyword::with_symbol("output"),
                options: None,
                url_column: serde_yaml_ng::from_str("url").unwrap(),
                url_params: Some(vec![
                    HttpParamConfig {
                        df: StrKeyword::with_value("PLAYERS_DF".to_owned()),
                        col: serde_yaml_ng::from_str("players").unwrap(),
                        template: None,
                        separator: None,
                    },
                    HttpParamConfig {
                        df: StrKeyword::with_value("TEAMS_DF".to_owned()),
                        col: serde_yaml_ng::from_str("teams").unwrap(),
                        template: Some("teams={}".to_string()),
                        separator: Some(",".to_string()),
                    },
                ]),
                model_fields: None,
            },
        };

        let players = [8478401, 8478402];
        let teams = ["EDM", "TOR"];

        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>("output: OUT").unwrap();
        let ctx = Arc::new(DefaultPipelineContext::with_results(
            &["PLAYERS_DF", "TEAMS_DF", "OUT"],
            2,
        ));

        ctx.insert_result("PLAYERS_DF", df!["players" => players].unwrap().lazy())
            .unwrap();
        ctx.insert_result("TEAMS_DF", df!["teams" => teams].unwrap().lazy())
            .unwrap();

        let _ = source_config.emplace(&ctx, &context);
        assert!(source_config.validate().is_empty());

        // with connection sync
        {
            let actual_node = source_config.transform();
            let server = MockServer::start();
            let mock = mock_server(&server, players, teams);
            let template = server.url("/v1/meta");
            let url_df = df!("url" => [template]).unwrap();
            actual_node.run(url_df.lazy(), ctx.clone()).unwrap();
            mock.assert();
            let actual = ctx.extract_clone_result("OUT").unwrap();
            let expected = explode_df(&str_json_to_df(&DummyData::meta_info()).unwrap()).unwrap();
            assert_eq!(expected, actual);
        }
        // without connection sync
        {
            let actual_node = source_config.transform();
            let server = MockServer::start();
            let template = server.url("/v1/meta");
            let url_df = df!("url" => [template]).unwrap();
            actual_node.run(url_df.lazy(), ctx.clone()).unwrap();
            let actual_with_messages = ctx.extract_clone_result("OUT").unwrap();
            let actual = actual_with_messages;
            let expected = explode_df(&str_json_to_df(&DummyData::meta_info()).unwrap()).unwrap();
            assert_ne!(expected, actual);
        }
        // with connection async
        {
            async_st!(async || {
                let actual_node = source_config.transform();
                let server = MockServer::start();
                let mock = mock_server(&server, players, teams);
                let template = server.url("/v1/meta");
                let url_df = df!("url" => [template]).unwrap();
                actual_node.fetch(url_df.lazy(), ctx.clone()).await.unwrap();
                mock.assert();
                let actual = ctx.extract_clone_result("OUT").unwrap();
                let expected = explode_df(&str_json_to_df(&DummyData::meta_info()).unwrap()).unwrap();
                assert_eq!(expected, actual);
            });
        }
        // without connection async
        {
            async_st!(async || {
                let actual_node = source_config.transform();
                let server = MockServer::start();
                let template = server.url("/v1/meta");
                let url_df = df!("url" => [template]).unwrap();
                actual_node.fetch(url_df.lazy(), ctx.clone()).await.unwrap();
                let actual_with_messages = ctx.extract_clone_result("OUT").unwrap();
                let actual = actual_with_messages;
                let expected = explode_df(&str_json_to_df(&DummyData::meta_info()).unwrap()).unwrap();
                assert_ne!(expected, actual);
            });
        }
    }
}
