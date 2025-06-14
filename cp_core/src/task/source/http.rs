use std::{io::Cursor, sync::Arc};

use async_trait::async_trait;
use polars::{
    io::SerReader,
    prelude::{IntoLazy, JsonReader, LazyFrame, Schema},
};

use crate::{
    model::common::ModelConfig,
    model_emplace,
    parser::keyword::Keyword,
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    util::{
        common::str_json_to_df,
        error::{CpError, CpResult},
    },
    valid_or_insert_error,
};

use super::{
    common::{Source, SourceConfig},
    config::HttpSourceConfig,
};

pub struct HttpSource {
    url: String,
    output: String,
    schema: Option<Arc<Schema>>,
}

#[async_trait]
impl Source for HttpSource {
    fn connection_type(&self) -> &str {
        "http"
    }
    fn name(&self) -> &str {
        self.output.as_str()
    }
    fn run(&self, _ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        let client = reqwest::blocking::Client::new();
        let request = client.get(&self.url);
        let response = request.send()?;
        let body = response.text()?;
        if let Some(schema) = &self.schema {
            let reader = JsonReader::new(Cursor::new(body.trim())).with_schema(schema.clone());
            Ok(reader.finish()?.lazy())
        } else {
            Ok(str_json_to_df(&body)?.lazy())
        }
    }
    async fn fetch(&self, _ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        let client = reqwest::Client::new();
        let request = client.get(&self.url);
        let response = request.send().await?;
        let body = response.text().await?;
        if let Some(schema) = &self.schema {
            let reader = JsonReader::new(Cursor::new(body.trim())).with_schema(schema.clone());
            Ok(reader.finish()?.lazy())
        } else {
            Ok(str_json_to_df(&body)?.lazy())
        }
    }
}

impl SourceConfig for HttpSourceConfig {
    fn emplace(
        &mut self,
        ctx: &DefaultPipelineContext,
        context: &serde_yaml_ng::Mapping,
    ) -> crate::util::error::CpResult<()> {
        self.http.url.insert_value_from_context(context)?;
        self.http.output.insert_value_from_context(context)?;
        model_emplace!(self.http, ctx, context);
        Ok(())
    }
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        valid_or_insert_error!(errors, self.http.url, "source[http].url");
        valid_or_insert_error!(errors, self.http.output, "source[http].output");
        if let Some(model_fields) = &self.http.model_fields {
            for (key_kw, field_kw) in model_fields.iter() {
                valid_or_insert_error!(errors, key_kw, "source[http].model.key");
                valid_or_insert_error!(errors, field_kw, "source[http].model.field");
            }
        }
        errors
    }
    fn transform(&self) -> Box<dyn super::common::Source> {
        let schema = self.http.model_fields.as_ref().map(|x| {
            ModelConfig {
                label: "".to_string(),
                fields: x.clone(),
            }
            .schema()
            .expect("failed to build schema")
        });

        Box::new(HttpSource {
            url: self.http.url.value().expect("source[http].url").to_string(),
            output: self.http.output.value().expect("output").to_owned(),
            schema: schema.map(Arc::new),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use httpmock::{Method::GET, Mock, MockServer};

    use crate::{
        async_st,
        parser::keyword::{Keyword, StrKeyword},
        pipeline::context::DefaultPipelineContext,
        task::source::{
            common::SourceConfig,
            config::{HttpSourceConfig, SingleLinkConfig},
        },
        util::{common::str_json_to_df, test::DummyData},
    };

    fn mock_server<'a>(server: &'a MockServer) -> Mock<'a> {
        server.mock(|when, then| {
            when.method(GET).path("/v1/shifts");
            then.status(200)
                .header("content-type", "application/json")
                .body(DummyData::shift_charts());
        })
    }

    #[test]
    fn valid_http_source_config_to_http_source_sync_async() {
        let server = MockServer::start();
        mock_server(&server);
        let mut source_config = HttpSourceConfig {
            http: SingleLinkConfig {
                url: StrKeyword::with_value(server.url("/v1/shifts")),
                output: StrKeyword::with_symbol("output"),
                options: None,
                model: None,
                model_fields: None,
            },
        };
        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>("output: table").unwrap();
        let ctx = Arc::new(DefaultPipelineContext::new());
        source_config.emplace(&ctx, &context).unwrap();
        let errs = source_config.validate();
        assert!(errs.is_empty());
        let node = source_config.transform();
        let actual_lf = node.run(ctx.clone()).unwrap();
        let actual_df = actual_lf.collect().unwrap();
        let expected_df = str_json_to_df(&DummyData::shift_charts()).unwrap();
        assert_eq!(node.name(), "table");
        assert_eq!(node.connection_type(), "http");
        assert_eq!(actual_df, expected_df);
        async_st!(async || {
            let actual_lf = node.fetch(ctx).await.unwrap();
            let actual_df = actual_lf.collect().unwrap();
            assert_eq!(actual_df, expected_df);
        });
    }
}
