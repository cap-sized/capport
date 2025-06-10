use std::{path::PathBuf, str::FromStr, sync::Arc};

use async_trait::async_trait;
use polars::prelude::{LazyFileListReader, LazyFrame, LazyJsonLineReader, Schema};

use crate::{
    model::common::ModelConfig,
    model_emplace,
    parser::keyword::Keyword,
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    util::{
        common::get_full_path,
        error::{CpError, CpResult},
    },
    valid_or_insert_error,
};

use super::{
    common::{Source, SourceConfig},
    config::JsonSourceConfig,
};

pub struct JsonSource {
    filepath: PathBuf,
    output: String,
    schema: Option<Arc<Schema>>,
}

impl JsonSource {
    pub fn new(filepath: &str, output: &str) -> Self {
        Self {
            filepath: std::path::PathBuf::from_str(filepath).expect("bad filepath"),
            output: output.to_owned(),
            schema: None,
        }
    }

    pub fn and_schema(mut self, schema: Schema) -> Self {
        let _ = self.schema.insert(Arc::new(schema));
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
        self.run(ctx)
    }

    fn run(&self, _ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        // Reopens files every run
        if !self.filepath.exists() {
            return Err(CpError::ConfigError(
                "File not found",
                self.filepath.to_str().unwrap().to_owned(),
            ));
        }
        let reader = match &self.schema {
            Some(schema) => LazyJsonLineReader::new(&self.filepath).with_schema(Some(schema.clone())),
            None => LazyJsonLineReader::new(&self.filepath).with_infer_schema_length(None),
        };
        let lf = reader.finish()?;
        Ok(lf)
    }
}

impl SourceConfig for JsonSourceConfig {
    fn emplace(&mut self, ctx: &DefaultPipelineContext, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        self.json.filepath.insert_value_from_context(context)?;
        self.json.output.insert_value_from_context(context)?;
        model_emplace!(self.json, ctx, context);
        Ok(())
    }
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        valid_or_insert_error!(errors, self.json.filepath, "source[json].filepath");
        valid_or_insert_error!(errors, self.json.output, "source[json].output");
        if let Some(model_fields) = &self.json.model_fields {
            for (key_kw, field_kw) in model_fields {
                valid_or_insert_error!(errors, key_kw, "source[json].model.key");
                valid_or_insert_error!(errors, field_kw, "source[json].model.field");
            }
        }
        errors
    }

    fn transform(&self) -> Box<dyn Source> {
        // By here the model_fields should be completely populated.
        let schema = self.json.model_fields.as_ref().map(|x| {
            ModelConfig {
                label: "".to_string(),
                fields: x.clone(),
            }
            .schema()
            .expect("failed to build schema")
        });

        let filepath = get_full_path(self.json.filepath.value().expect("filepath"), true).expect("bad filepath");

        Box::new(JsonSource {
            filepath,
            output: self.json.output.value().expect("output").to_owned(),
            schema: schema.map(Arc::new),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use polars::{
        df,
        frame::DataFrame,
        io::SerWriter,
        prelude::{DataType, JsonWriter},
    };

    use crate::{
        async_st, context::model::ModelRegistry, model::common::{ModelConfig, ModelFieldInfo}, parser::{
            dtype::DType,
            keyword::{Keyword, ModelFieldKeyword, StrKeyword},
        }, pipeline::context::DefaultPipelineContext, task::source::{
            common::{Source, SourceConfig},
            config::{JsonSourceConfig, LocalFileSourceConfig},
        }, util::{test::assert_frame_equal, tmp::TempFile}
    };

    use super::JsonSource;

    fn example() -> DataFrame {
        df!(
            "a" => [-1, 1, 3, 5, 6],
            "b" => ["z", "a", "j", "i", "c"],
        )
        .unwrap()
    }

    fn example_model() -> ModelConfig {
        ModelConfig {
            label: "S".to_string(),
            fields: HashMap::from([
                (
                    StrKeyword::with_value("a".to_owned()),
                    ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::Int32))),
                ),
                (
                    StrKeyword::with_value("b".to_owned()),
                    ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::String))),
                ),
            ]),
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
        assert_frame_equal(result.collect().unwrap(), expected);
        assert_eq!(json_source.name(), "_sample");
        assert_eq!(json_source.connection_type(), "json");
    }

    #[test]
    fn valid_json_source_async() {
        let mut expected = example();
        let tmp = TempFile::default();
        let buffer = tmp.get_mut().unwrap();
        let mut writer = JsonWriter::new(buffer);
        writer.finish(&mut expected).unwrap();
        let model_schema = example_model().schema().unwrap();
        let json_source = JsonSource::new(&tmp.filepath, "_sample").and_schema(model_schema);
        let ctx = Arc::new(DefaultPipelineContext::new());
        async_st!(async || {
            let result = json_source.fetch(ctx).await.unwrap();
            assert_frame_equal(result.collect().unwrap(), expected);
            assert_eq!(json_source.name(), "_sample");
            assert_eq!(json_source.connection_type(), "json");
        });
    }

    #[test]
    fn valid_json_source_config_to_json_source() {
        let mut expected = example();
        let tmp = TempFile::default();
        let buffer = tmp.get_mut().unwrap();
        let mut writer = JsonWriter::new(buffer);
        writer.finish(&mut expected).unwrap();
        let mut source_config = JsonSourceConfig {
            json: LocalFileSourceConfig {
                filepath: StrKeyword::with_value(tmp.filepath.clone()),
                output: StrKeyword::with_value("_sample".to_owned()),
                model_fields: None,
                model: Some(StrKeyword::with_value("S".to_owned())),
            },
        };
        let mut model_reg = ModelRegistry::new();
        model_reg.insert(example_model());
        let ctx = Arc::new(DefaultPipelineContext::new().with_model_registry(model_reg));
        let mapping = serde_yaml_ng::Mapping::new();
        let _ = source_config.emplace(&ctx, &mapping);
        let errors = source_config.validate();
        assert!(errors.is_empty());
        assert_eq!(source_config.json.model_fields.clone().unwrap(), example_model().fields);
        let actual_node = source_config.transform();
        let result = actual_node.run(ctx.clone()).unwrap();
        assert_frame_equal(result.collect().unwrap(), expected);
    }
}
