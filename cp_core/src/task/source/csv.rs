use std::{fs::File, path::PathBuf, str::FromStr, sync::Arc};

use async_trait::async_trait;
use polars::prelude::{LazyCsvReader, LazyFileListReader, LazyFrame, PlSmallStr, Schema};

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
    config::CsvSourceConfig,
};

pub struct CsvSource {
    filepath: PathBuf,
    output: String,
    separator: u8,
    schema: Option<Arc<Schema>>,
}

fn read_headers_from_file(fp: &std::path::PathBuf, separator: u8) -> CpResult<Vec<String>> {
    let reader = LazyCsvReader::new(fp).with_separator(separator).with_n_rows(Some(0));
    let empty_frame = reader.finish()?.collect()?;
    Ok(empty_frame.get_columns().iter().map(|x| x.name().to_string()).collect())
}

impl CsvSource {
    pub fn new(filepath: &str, output: &str, separator: u8) -> Self {
        Self {
            filepath: std::path::PathBuf::from_str(filepath).expect("bad filepath"),
            output: output.to_owned(),
            separator,
            schema: None,
        }
    }

    pub fn and_schema(mut self, schema: Schema) -> Self {
        let _ = self.schema.insert(Arc::new(schema));
        self
    }
}

#[async_trait]
impl Source for CsvSource {
    fn connection_type(&self) -> &str {
        "csv"
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
        let reader = if let Some(schema) = &self.schema {
            let headers = read_headers_from_file(&self.filepath, self.separator)?;
            // IMPORTANT: schema does not read header. MUST be in order!
            let final_schema = Arc::new(schema.try_project(headers)?);
            LazyCsvReader::new(&self.filepath)
                .with_separator(self.separator)
                .with_schema(Some(final_schema))
        } else {
            LazyCsvReader::new(&self.filepath)
                .with_separator(self.separator)
                .with_try_parse_dates(true)
                .with_infer_schema_length(None)
        };
        let lf = reader.finish()?;

        Ok(lf)
    }
}

impl SourceConfig for CsvSourceConfig {
    fn emplace(&mut self, ctx: &DefaultPipelineContext, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        self.csv.filepath.insert_value_from_context(context)?;
        self.csv.output.insert_value_from_context(context)?;
        if let Some(mut separator) = self.csv.separator.take() {
            separator.insert_value_from_context(context)?;
            let _ = self.csv.separator.insert(separator);
        }
        model_emplace!(self.csv, ctx, context);
        Ok(())
    }
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        valid_or_insert_error!(errors, self.csv.filepath, "source[csv].filepath");
        valid_or_insert_error!(errors, self.csv.output, "source[csv].output");
        if let Some(separator) = self.csv.separator.as_ref() {
            valid_or_insert_error!(errors, separator, "source[csv].separator");
        }
        if let Some(model_fields) = &self.csv.model_fields {
            for (key_kw, field_kw) in model_fields {
                valid_or_insert_error!(errors, key_kw, "source[csv].model.key");
                valid_or_insert_error!(errors, field_kw, "source[csv].model.field");
            }
        }
        errors
    }

    fn transform(&self) -> Box<dyn Source> {
        // By here the model_fields should be completely populated.
        let schema = self.csv.model_fields.as_ref().map(|x| {
            ModelConfig {
                label: "".to_string(),
                fields: x.clone(),
            }
            .schema()
            .expect("failed to build schema")
        });
        let separator = if let Some(x) = &self.csv.separator {
            if let Some(sepstr) = x.value() {
                sepstr.as_bytes().first().map_or(b',', |b| b.to_owned())
            } else {
                b','
            }
        } else {
            b','
        };

        let filepath = get_full_path(self.csv.filepath.value().expect("filepath"), true).expect("bad filepath");

        Box::new(CsvSource {
            filepath,
            output: self.csv.output.value().expect("output").to_owned(),
            schema: schema.map(Arc::new),
            separator
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
        prelude::{CsvWriter, DataType, Field, Schema},
    };

    use crate::{
        context::model::ModelRegistry,
        model::common::{ModelConfig, ModelFieldInfo},
        parser::{
            dtype::DType,
            keyword::{Keyword, ModelFieldKeyword, StrKeyword},
        },
        pipeline::context::DefaultPipelineContext,
        task::source::{
            common::{Source, SourceConfig},
            config::{CsvSourceConfig, _CsvSourceConfig},
        },
        util::{test::assert_frame_equal, tmp::TempFile},
    };

    use super::CsvSource;

    fn example() -> DataFrame {
        df!(
            "a" => [-1, 1, 3, 5, 6],
            "b" => ["why", "doesn't", "this", "work", "consistently"],
        )
        .unwrap()
    }

    fn schema() -> Schema {
        let mut schema = Schema::with_capacity(2);
        schema.insert_at_index(0, "a".into(), DataType::Int32).unwrap();
        schema.insert_at_index(1, "b".into(), DataType::String).unwrap();
        schema
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
    fn valid_csv_source() {
        let mut expected = example();
        let tmp = TempFile::default();
        let buffer = tmp.get_mut().unwrap();
        let mut writer = CsvWriter::new(buffer);
        writer.finish(&mut expected).unwrap();
        let csv_source = CsvSource::new(&tmp.filepath, "_sample", b',').and_schema(schema());
        let ctx = Arc::new(DefaultPipelineContext::new());
        let result = csv_source.run(ctx).unwrap();
        assert_frame_equal(result.collect().unwrap(), expected);
        assert_eq!(csv_source.name(), "_sample");
        assert_eq!(csv_source.connection_type(), "csv");
    }

    #[test]
    fn valid_csv_source_async() {
        let mut expected = example();
        let tmp = TempFile::default();
        let buffer = tmp.get_mut().unwrap();
        let mut writer = CsvWriter::new(buffer);
        writer.finish(&mut expected).unwrap();
        let csv_source = CsvSource::new(&tmp.filepath, "_sample", b',').and_schema(schema());
        let ctx = Arc::new(DefaultPipelineContext::new());
        let mut rt_builder = tokio::runtime::Builder::new_current_thread();
        rt_builder.enable_all();
        let rt = rt_builder.build().unwrap();
        let event = async || {
            let result = csv_source.fetch(ctx).await.unwrap();
            assert_frame_equal(result.collect().unwrap(), expected);
            assert_eq!(csv_source.name(), "_sample");
            assert_eq!(csv_source.connection_type(), "csv");
        };
        rt.block_on(event());
    }

    #[test]
    fn valid_csv_source_config_to_csv_source() {
        let mut expected = example();
        let tmp = TempFile::default();
        let buffer = tmp.get_mut().unwrap();
        let mut writer = CsvWriter::new(buffer);
        writer.finish(&mut expected).unwrap();
        let mut source_config = CsvSourceConfig {
            csv: _CsvSourceConfig {
                filepath: StrKeyword::with_value(tmp.filepath.clone()),
                output: StrKeyword::with_value("_sample".to_owned()),
                model_fields: None,
                model: Some(StrKeyword::with_value("S".to_owned())),
                separator: None
            },
        };
        let mut model_reg = ModelRegistry::new();
        model_reg.insert(example_model());
        let ctx = Arc::new(DefaultPipelineContext::new().with_model_registry(model_reg));
        let mapping = serde_yaml_ng::Mapping::new();
        let _ = source_config.emplace(&ctx, &mapping);
        let errors = source_config.validate();
        assert!(errors.is_empty());
        assert_eq!(source_config.csv.model_fields.clone().unwrap(), example_model().fields);
        let actual_node = source_config.transform();
        let result = actual_node.run(ctx.clone()).unwrap();
        assert_frame_equal(result.collect().unwrap(), expected);
    }
}
