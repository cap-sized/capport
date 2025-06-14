use std::{fs::OpenOptions, path::PathBuf, str::FromStr, sync::Arc};

use async_trait::async_trait;
use polars::{
    frame::DataFrame,
    io::SerWriter,
    prelude::{Expr, IntoLazy, JsonWriter},
};

use crate::{
    model::common::ModelConfig,
    model_emplace,
    parser::{keyword::Keyword, merge_type::MergeTypeEnum},
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    util::{
        common::{get_full_path, get_utc_time_str_now, rng_str},
        error::{CpError, CpResult},
    },
};

use super::{
    common::{Sink, SinkConfig},
    config::JsonSinkConfig,
};

pub struct JsonSink {
    merge_type: MergeTypeEnum,
    filepath: PathBuf,
    schema: Option<Vec<Expr>>,
}

impl JsonSink {
    pub fn new(filepath: &str, merge_type: Option<MergeTypeEnum>) -> Self {
        Self {
            filepath: std::path::PathBuf::from_str(filepath).expect("bad filepath"),
            merge_type: merge_type.unwrap_or(MergeTypeEnum::Replace),
            schema: None,
        }
    }

    pub fn with_schema(mut self, columns: Vec<Expr>) -> Self {
        let _ = self.schema.insert(columns);
        self
    }
}

#[async_trait]
impl Sink for JsonSink {
    fn connection_type(&self) -> &str {
        "json"
    }

    async fn fetch(&self, dataframe: DataFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        self.run(dataframe, ctx)
    }

    fn run(&self, dataframe: DataFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        let filepath = match self.merge_type {
            MergeTypeEnum::Replace => OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&self.filepath)?,
            MergeTypeEnum::Insert => OpenOptions::new().append(true).truncate(false).open(&self.filepath)?,
            MergeTypeEnum::MakeNext => {
                let mut fp = self.filepath.clone();
                fp.set_file_name(format!(
                    "{}_{}.json",
                    fp.file_stem().expect("filename").to_str().unwrap_or("_"),
                    get_utc_time_str_now()
                ));
                if std::fs::exists(&fp)? {
                    fp.set_file_name(format!(
                        "{}_{}.json",
                        fp.file_stem().expect("filename").to_str().unwrap_or("_"),
                        rng_str(6)
                    ));
                }
                OpenOptions::new().write(true).create(true).truncate(true).open(fp)?
            }
        };
        let mut df_to_write = if let Some(schema) = &self.schema {
            dataframe.lazy().select(schema.clone()).collect()?
        } else {
            dataframe
        };
        if ctx.is_executing_sink() {
            let mut writer = JsonWriter::new(filepath).with_json_format(polars::prelude::JsonFormat::Json);
            writer.finish(&mut df_to_write)?;
        } else {
            let path_details = filepath.metadata()?;
            log::info!(
                "[no-execute-sink] Completed writing to {:?}: {:?}",
                path_details,
                df_to_write
            );
        }
        Ok(())
    }
}

impl SinkConfig for JsonSinkConfig {
    fn emplace(&mut self, ctx: &DefaultPipelineContext, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        self.json.filepath.insert_value_from_context(context)?;
        model_emplace!(self.json, ctx, context);
        Ok(())
    }

    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        match self.json.filepath.value() {
            Some(_) => {}
            None => errors.push(CpError::SymbolMissingValueError(
                "filepath",
                self.json.filepath.symbol().unwrap_or("?").to_owned(),
            )),
        }
        errors
    }

    fn transform(&self) -> Box<dyn Sink> {
        let fp = match get_full_path(self.json.filepath.value().expect("filepath"), true) {
            Ok(x) => x,
            Err(e) => panic!("bad filepath `{:?}`: {}", self.json.filepath.value(), e),
        };
        if self.json.merge_type == MergeTypeEnum::Insert {
            log::warn!("INSERT merge_type can be costly for json: {:?}", &fp);
        }
        let schema = self.json.model_fields.as_ref().map(|x| {
            ModelConfig {
                label: "".to_string(),
                fields: x.clone(),
            }
            .columns()
            .expect("failed to build schema")
        });
        Box::new(JsonSink {
            filepath: fp,
            merge_type: self.json.merge_type,
            schema,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Read, sync::Arc};

    use polars::{
        df,
        frame::DataFrame,
        io::SerReader,
        prelude::{DataType, IntoLazy, JsonReader, col},
    };

    use crate::{
        async_st,
        context::model::ModelRegistry,
        model::common::{ModelConfig, ModelFieldInfo, ModelFields},
        parser::{
            dtype::DType,
            keyword::{Keyword, ModelFieldKeyword, StrKeyword},
            merge_type::MergeTypeEnum,
        },
        pipeline::context::DefaultPipelineContext,
        task::sink::{
            common::{Sink, SinkConfig},
            config::{JsonSinkConfig, LocalFileSinkConfig},
        },
        util::{common::rng_str, test::assert_frame_equal, tmp::TempFile},
    };

    use super::JsonSink;

    fn example() -> DataFrame {
        df!(
            "a" => [-1, 1, 3, 5, 6],
            "b" => ["z", "a", "j", "i", "c"],
        )
        .unwrap()
    }

    fn example2() -> DataFrame {
        df!(
            "a" => [-1, 1, 3, 5, 6, -1, 1, 3, 5, 6],
            "b" => ["z", "a", "j", "i", "c", "z", "a", "j", "i", "c"],
        )
        .unwrap()
    }

    #[test]
    fn valid_json_sink() {
        let expected = example();
        let tmp = TempFile::default();
        let json_sink = JsonSink::new(&tmp.filepath, None);
        let ctx = Arc::new(DefaultPipelineContext::new().with_executing_sink(true));
        let _ = json_sink.run(expected.clone(), ctx);
        let buffer = tmp.get().unwrap();
        let reader = JsonReader::new(buffer);
        let actual = reader.finish().unwrap();
        assert_frame_equal(actual, expected);
        assert_eq!(json_sink.connection_type(), "json");
    }

    #[test]
    fn valid_json_sink_async() {
        let expected = example();
        let tmp = TempFile::default();
        let json_sink = JsonSink::new(&tmp.filepath, None);
        let ctx = Arc::new(DefaultPipelineContext::new().with_executing_sink(true));
        async_st!(async || {
            let _ = json_sink.fetch(expected.clone(), ctx).await;
            let buffer = tmp.get().unwrap();
            let reader = JsonReader::new(buffer);
            let actual = reader.finish().unwrap();
            assert_frame_equal(actual, expected);
        });
    }

    fn get_node(
        merge_type: MergeTypeEnum,
        tmp: TempFile,
    ) -> (
        Arc<DefaultPipelineContext>,
        serde_yaml_ng::Mapping,
        Box<dyn Sink>,
        TempFile,
    ) {
        let mut source_config = JsonSinkConfig {
            json: LocalFileSinkConfig {
                filepath: StrKeyword::with_symbol("sample"),
                merge_type,
                model: None,
                model_fields: None,
            },
        };
        let ctx = Arc::new(DefaultPipelineContext::new().with_executing_sink(true));
        let config = format!("sample: {}", &tmp.filepath);
        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>(config.as_str()).unwrap();
        let _ = source_config.emplace(&ctx, &context);
        let errors = source_config.validate();
        assert!(errors.is_empty());
        (ctx, context, source_config.transform(), tmp)
    }

    #[test]
    fn valid_json_sink_config_to_json_sink_model_fields() {
        let tmp = TempFile::default();
        let mut source_config = JsonSinkConfig {
            json: LocalFileSinkConfig {
                filepath: StrKeyword::with_value(tmp.filepath.clone()),
                merge_type: MergeTypeEnum::Replace,
                model: Some(StrKeyword::with_value("test".to_owned())),
                model_fields: None,
            },
        };
        let mut model_registry = ModelRegistry::new();
        model_registry.insert(ModelConfig {
            label: "test".to_string(),
            fields: ModelFields::from([(
                StrKeyword::with_value("a".to_owned()),
                ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::Int8))),
            )]),
        });
        let ctx = Arc::new(
            DefaultPipelineContext::new()
                .with_executing_sink(true)
                .with_model_registry(model_registry),
        );
        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>("{}").unwrap();
        let _ = source_config.emplace(&ctx, &context);
        let errors = source_config.validate();
        assert!(errors.is_empty());
        let actual_node = source_config.transform();
        actual_node.run(example(), ctx.clone()).unwrap();
        let buffer = tmp.get().unwrap();
        let reader = JsonReader::new(buffer);
        let actual = reader.finish().unwrap();
        assert_frame_equal(actual, example().lazy().select(&[col("a")]).collect().unwrap());
    }

    #[test]
    fn valid_json_sink_config_to_json_sink_exec_mode_off() {
        let tmp = TempFile::default();
        let mut source_config = JsonSinkConfig {
            json: LocalFileSinkConfig {
                filepath: StrKeyword::with_value(tmp.filepath.clone()),
                merge_type: MergeTypeEnum::Replace,
                model: None,
                model_fields: None,
            },
        };
        let ctx = Arc::new(DefaultPipelineContext::new().with_executing_sink(false));
        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>("{}").unwrap();
        let _ = source_config.emplace(&ctx, &context);
        let errors = source_config.validate();
        assert!(errors.is_empty());
        let actual_node = source_config.transform();
        actual_node.run(example(), ctx.clone()).unwrap();
        let mut actual_output = String::new();
        tmp.get().unwrap().read_to_string(&mut actual_output).unwrap();
        assert!(actual_output.is_empty())
    }

    #[test]
    fn valid_json_sink_config_to_json_sink_replace() {
        let expected = example();
        let tmp = TempFile::default();
        let (ctx, _, actual_node, tmp) = get_node(MergeTypeEnum::Replace, tmp);
        actual_node.run(expected.clone(), ctx.clone()).unwrap();
        let buffer = tmp.get().unwrap();
        let reader = JsonReader::new(buffer);
        let actual = reader.finish().unwrap();
        assert_frame_equal(actual, expected);
    }

    #[test]
    fn valid_json_sink_config_to_json_sink_insert() {
        let expected = example2();
        let tmp = TempFile::default();
        let (ctx, _, actual_node, tmp) = get_node(MergeTypeEnum::Insert, tmp);
        actual_node.run(expected.clone(), ctx.clone()).unwrap();
        let buffer = tmp.get().unwrap();
        let reader = JsonReader::new(buffer);
        let actual = reader.finish().unwrap();
        assert_frame_equal(actual, expected);
    }

    #[test]
    fn valid_json_sink_config_to_json_sink_make_next() {
        let expected = example();
        let dir = format!("/tmp/capport_testing/{}", rng_str(5));
        {
            std::fs::create_dir(&dir).unwrap();
            let tmp = TempFile::default_in_dir(&dir, "json").unwrap();
            let (ctx, _, actual_node, _) = get_node(MergeTypeEnum::MakeNext, tmp);
            let files_created = 3i32;
            for _ in 0..files_created {
                actual_node.run(expected.clone(), ctx.clone()).unwrap();
            }
            let count = std::fs::read_dir(&dir).unwrap().fold(0i32, |idx, file| {
                let buffer = file.unwrap().path();
                let reader = JsonReader::new(std::fs::File::open(buffer).unwrap());
                let actual = reader.finish().unwrap();
                assert_frame_equal(actual, expected.clone());
                idx + 1
            });
            assert_eq!(count, files_created);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
