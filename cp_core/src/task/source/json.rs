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

impl JsonSource {
    pub fn new(filepath: &str, output: &str) -> Self {
        Self { filepath: filepath.to_owned(), output: output.to_owned(), schema: None }
    }

    pub fn and_schema(mut self, schema: Schema) -> Self {
        let _ = self.schema.insert(schema);
        self
    }
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
                        format!("Could not substitute model `{}` in ModelRegistry: {:?}", model_name, e),
                    ));
                }
            }
        }
        errors
    }

    fn transform(&self, _ctx: Arc<DefaultPipelineContext>) -> Box<dyn Source> {
        // By here the model_fields should be completely populated.
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use polars::{df, frame::DataFrame, io::SerWriter, prelude::{DataType, JsonWriter}};

    use crate::{context::model::ModelRegistry, model::common::{ModelConfig, ModelFieldInfo}, parser::{dtype::DType, keyword::{Keyword, ModelFieldKeyword, StrKeyword}}, pipeline::context::DefaultPipelineContext, task::source::{common::{Source, SourceConfig}, config::JsonSourceConfig}, util::tmp::TempFile};

    use super::JsonSource;

    fn example() -> DataFrame {
        df!(
            "a" => [-1, 1, 3, 5, 6],
            "b" => ["z", "a", "j", "i", "c"],
        ).unwrap()
    }

    fn example_model() -> ModelConfig {
        ModelConfig {
            label: "S".to_string(),
            fields: HashMap::from([
                (StrKeyword::with_value("a".to_owned()), ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::Int32)))),
                (StrKeyword::with_value("b".to_owned()), ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::String)))),
            ])
        }
    }

    #[test]
    fn valid_json_source() {
        let mut expected = example();
        let tmp = TempFile::default();
        let buffer = tmp.get_mut().unwrap();
        let mut writer = JsonWriter::new(buffer);
        writer.finish(&mut expected).unwrap();
        let model_schema = example_model().schema().unwrap();
        let json_source = JsonSource::new(&tmp.filepath, "_sample").and_schema(model_schema);
        let ctx = Arc::new(DefaultPipelineContext::new());
        let result = json_source.run(ctx).unwrap();
        // TODO: replace with helper method when yx impls it
        assert_eq!(result.select(&["a".into(), "b".into()]).collect().unwrap(), expected);
        assert_eq!(json_source.name(), "_sample");
        assert_eq!(json_source.connection_type(), "json");
        // TODO: test async
    }

    #[test]
    fn valid_json_source_config_to_json_source() {
        let mut expected = example();
        let tmp = TempFile::default();
        let buffer = tmp.get_mut().unwrap();
        let mut writer = JsonWriter::new(buffer);
        writer.finish(&mut expected).unwrap();
        let mut source_config = JsonSourceConfig {
            filepath: StrKeyword::with_value(tmp.filepath.clone()),
            output: StrKeyword::with_value("_sample".to_owned()),
            model_fields: None,
            model: StrKeyword::with_value("S".to_owned())
        };
        let mut model_reg = ModelRegistry::new();
        model_reg.insert(example_model());
        let ctx = Arc::new(DefaultPipelineContext::new().with_model_registry(model_reg));
        let mapping = serde_yaml_ng::Mapping::new();
        let errors = source_config.validate(ctx.clone(), &mapping);
        assert!(errors.is_empty());
        assert_eq!(source_config.model_fields.clone().unwrap(), example_model().fields);
        let actual_node = source_config.transform(ctx.clone());
        let result = actual_node.run(ctx.clone()).unwrap();
        // TODO: replace with helper method when yx impls it
        assert_eq!(result.select(&["a".into(), "b".into()]).collect().unwrap(), expected);
    }

}
