use std::{fs::File, path::PathBuf, str::FromStr, sync::Arc};

use async_trait::async_trait;
use polars::prelude::{ArrowSchema, LazyFileListReader, LazyFrame, LazyJsonLineReader, Schema};

use crate::{
    model::common::ModelConfig,
    parser::keyword::Keyword,
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    util::error::{CpError, CpResult},
};

use super::{
    common::{Source, SourceConfig},
    config::JsonSourceConfig,
};

pub struct JsonSource {
    filepath: String,
    output: String,
    schema: Option<Schema>,
}

#[async_trait]
impl Source for JsonSource {
    fn connection_type(&self) -> &str {
        "json"
    }

    fn name(&self) -> &str {
        self.output.as_str()
    }

    async fn fetch(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        // let filepath = tokio::fs::File::open(self.filepath).await;
        self.run(ctx)
    }

    fn run(&self, _ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        // Reopens files every run
        let filepath = PathBuf::from_str(&self.filepath).unwrap(); // infallible
        if !filepath.exists() {
            return Err(CpError::ConfigError("File not found", self.filepath.clone()));
        }
        let reader = match &self.schema {
            Some(schema) => LazyJsonLineReader::new(filepath).with_schema(Some(Arc::new(schema.clone()))),
            None => LazyJsonLineReader::new(filepath).with_infer_schema_length(None),
        };
        let lf = reader.finish()?;
        return Ok(lf);
    }
}

impl SourceConfig for JsonSourceConfig {
    fn validate(&mut self, ctx: Arc<DefaultPipelineContext>, context: &serde_yaml_ng::Mapping) -> Vec<CpError> {
        let mut errors = vec![];
        match self.filepath.value() {
            Some(_) => {}
            None => errors.push(CpError::SymbolMissingValueError(
                "filepath",
                self.filepath.symbol().unwrap_or("?").to_owned(),
            )),
        }
        match self.output.value() {
            Some(_) => {}
            None => errors.push(CpError::SymbolMissingValueError(
                "output",
                self.output.symbol().unwrap_or("?").to_owned(),
            )),
        };
        if let Some(model_fields) = self.model_fields.take() {
            let model = ModelConfig {
                label: "".to_string(),
                fields: model_fields,
            };
            match model.substitute_model_fields(context) {
                Ok(fields) => {
                    let _ = self.model_fields.insert(fields);
                }
                Err(e) => {
                    errors.push(CpError::ConfigError(
                        "Failed to substitute model with context",
                        format!("Could not substitute model in JsonSourceConfig `{:?}`: {}", self, e),
                    ));
                }
            }
        } else if let Some(model_name) = self.model.value() {
            match ctx.get_substituted_model_fields(model_name, context) {
                Ok(fields) => {
                    let _ = self.model_fields.insert(fields);
                }
                Err(e) => {
                    errors.push(CpError::ConfigError(
                        "Failed to substitute model with context",
                        format!("Could not substitute model `{}` in ModelRegistry", model_name),
                    ));
                }
            }
        }
        // check model and if it is valid
        errors
    }

    // TODO: Add model registry
    fn transform(&self, _ctx: Arc<DefaultPipelineContext>) -> Box<dyn Source> {
        // Ok to completely fail here at unwrap
        let schema = self.model_fields.as_ref().map(|x| {
            ModelConfig {
                label: "".to_string(),
                fields: x.clone(),
            }
            .schema()
            .expect("failed to build schema")
        });

        Box::new(JsonSource {
            filepath: self.filepath.value().expect("filepath").to_owned(),
            output: self.output.value().expect("output").to_owned(),
            schema,
        })
    }
}
