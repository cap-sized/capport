use std::{sync::Arc};

use async_trait::async_trait;
use polars::{frame::DataFrame, prelude::{IntoLazy, LazyFrame}};
use polars_io::{json::JsonReader, SerReader};

use crate::{
    parser::keyword::Keyword,
    pipeline::context::{DefaultPipelineContext},
    util::{
        error::{CpError, CpResult},
        tmp::TempFile
    },
    valid_or_insert_error,
};

use super::{
    common::{Source, SourceConfig},
    config::{InlineDataSourceConfig},
};

pub struct InlineDataSource {
    dataframe: DataFrame,
    output: String,
}

impl InlineDataSource {
    pub fn new(output: &str) -> Self {
        Self {
            dataframe: DataFrame::empty(),
            output: output.to_owned(),
        }
    }
}

#[async_trait]
impl Source for InlineDataSource {
    fn connection_type(&self) -> &str {
        "inline"
    }

    fn name(&self) -> &str {
        self.output.as_str()
    }

    async fn fetch(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        self.run(ctx)
    }

    fn run(&self, _ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        // Reopens files every run
        let new_df = self.dataframe.clone();
        Ok(new_df.lazy())
    }
}

impl SourceConfig for InlineDataSourceConfig {
    fn emplace(&mut self, _ctx: &DefaultPipelineContext, _context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        Ok(())
    }
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        valid_or_insert_error!(errors, self.inline.output, "source[inline].output");
        errors
    }

    fn transform(&self) -> Box<dyn Source> {
        let data_str = serde_yaml_ng::to_string(&self.inline.data).map(|x| x.trim().to_owned()).expect("invalid yml");
        let de = serde_yaml_ng::Deserializer::from_str(&data_str);
        let tmp = TempFile::default(); 
        let writer = tmp.get_mut().unwrap();
        let mut se = serde_json::Serializer::new(writer);
        serde_transcode::transcode(de, &mut se).expect("Failed to transcode YML to JSON");
        let buffer = tmp.get().unwrap();
        let json_reader = JsonReader::new(buffer);
        let df = json_reader.finish().expect("Inalid dataframe from JSON from YML");
        Box::new(InlineDataSource {
            dataframe: df,
            output: self.inline.output.value().expect("output").to_owned()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use polars::{
        df,
        frame::DataFrame,
        prelude::{DataType},
    };

    use crate::{
        async_st,
        context::model::ModelRegistry,
        model::common::{ModelConfig, ModelFieldInfo, ModelFields},
        parser::{
            dtype::DType,
            keyword::{Keyword, ModelFieldKeyword, StrKeyword},
        },
        pipeline::context::DefaultPipelineContext,
        task::source::{
            common::{Source, SourceConfig},
            config::{InlineDataSourceConfig, RawValuesConfig},
        },
        util::{test::assert_frame_equal},
    };

    use super::InlineDataSource;

    fn example() -> DataFrame {
        df!(
            "a" => [-1, 1, 3, 5, 6],
            "b" => ["z", "a", "j", "i", "c"],
        )
        .unwrap()
    }

    fn fruits() -> DataFrame {
        df!(
            "apple" => [1],
            "orange" => [2],
            "pear" => [3],
            "fruit basket" => [19],
        )
        .unwrap()
    }

    fn example_model() -> ModelConfig {
        ModelConfig {
            label: "S".to_string(),
            fields: ModelFields::from([
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
    fn valid_inline_source() {
        let expected = example();
        let source = InlineDataSource { dataframe: example(), output: "S".to_string() };
        let ctx = Arc::new(DefaultPipelineContext::new());
        let result = source.run(ctx).unwrap();
        assert_frame_equal(result.collect().unwrap(), expected);
        assert_eq!(source.name(), "S");
        assert_eq!(source.connection_type(), "inline");
    }

    #[test]
    fn valid_inline_source_async() {
        let expected = example();
        let source = InlineDataSource { dataframe: example(), output: "S".to_string() };
        let ctx = Arc::new(DefaultPipelineContext::new());
        async_st!(async || {
            let result = source.fetch(ctx).await.unwrap();
            assert_frame_equal(result.collect().unwrap(), expected);
            assert_eq!(source.name(), "S");
            assert_eq!(source.connection_type(), "inline");
        });
    }

    #[test]
    fn valid_inline_source_config_to_inline_source_list() {
        let expected = example();
        let mut source_config = InlineDataSourceConfig {
            inline: RawValuesConfig {
                data: serde_yaml_ng::from_str::<serde_yaml_ng::Value>("
- a: -1
  b: z
- a: 1
  b: a
- a: 3
  b: j
- a: 5
  b: i
- a: 6
  b: c
                          ").unwrap(),
                output: StrKeyword::with_value("_sample".to_owned()),
            },
        };
        let mut model_reg = ModelRegistry::new();
        model_reg.insert(example_model());
        let ctx = Arc::new(DefaultPipelineContext::new().with_model_registry(model_reg));
        let mapping = serde_yaml_ng::Mapping::new();
        let _ = source_config.emplace(&ctx, &mapping);
        let errors = source_config.validate();
        assert!(errors.is_empty());
        let actual_node = source_config.transform();
        let result = actual_node.run(ctx.clone()).unwrap();
        assert_frame_equal(result.collect().unwrap(), expected);
    }

    #[test]
    fn valid_inline_source_config_to_inline_source_struct() {
        let expected = fruits();
        let mut source_config = InlineDataSourceConfig {
            inline: RawValuesConfig {
                data: serde_yaml_ng::from_str::<serde_yaml_ng::Value>("
apple: 1
orange: 2
pear: 3
fruit basket: 19
                          ").unwrap(),
                output: StrKeyword::with_value("_sample".to_owned()),
            },
        };
        let mut model_reg = ModelRegistry::new();
        model_reg.insert(example_model());
        let ctx = Arc::new(DefaultPipelineContext::new().with_model_registry(model_reg));
        let mapping = serde_yaml_ng::Mapping::new();
        let _ = source_config.emplace(&ctx, &mapping);
        let errors = source_config.validate();
        assert!(errors.is_empty());
        let actual_node = source_config.transform();
        let result = actual_node.run(ctx.clone()).unwrap();
        assert_frame_equal(result.collect().unwrap(), expected);
    }
}

