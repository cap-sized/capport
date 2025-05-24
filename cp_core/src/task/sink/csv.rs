use std::{fs::File, sync::Arc};

use async_trait::async_trait;
use polars::{frame::DataFrame, io::SerWriter, prelude::CsvWriter};

use crate::{
    parser::keyword::Keyword,
    pipeline::context::DefaultPipelineContext,
    util::error::{CpError, CpResult},
};

use super::{
    common::{Sink, SinkConfig},
    config::CsvSinkConfig,
};

pub struct CsvSink {
    filepath: String,
}

impl CsvSink {
    pub fn new(filepath: &str) -> Self {
        Self {
            filepath: filepath.to_owned(),
        }
    }
}

#[async_trait]
impl Sink for CsvSink {
    fn connection_type(&self) -> &str {
        "json"
    }

    async fn fetch(&self, dataframe: DataFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        self.run(dataframe, ctx)
    }

    fn run(&self, dataframe: DataFrame, _ctx: Arc<DefaultPipelineContext>) -> CpResult<()> {
        let filepath = File::create(&self.filepath)?;
        let mut writer = CsvWriter::new(filepath);
        let mut df_to_write = dataframe;
        writer.finish(&mut df_to_write)?;
        Ok(())
    }
}

impl SinkConfig for CsvSinkConfig {
    fn emplace(&mut self, _ctx: Arc<DefaultPipelineContext>, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        self.filepath.insert_value_from_context(context)
    }

    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        match self.filepath.value() {
            Some(_) => {}
            None => errors.push(CpError::SymbolMissingValueError(
                "filepath",
                self.filepath.symbol().unwrap_or("?").to_owned(),
            )),
        }
        errors
    }

    fn transform(&self) -> Box<dyn Sink> {
        Box::new(CsvSink {
            filepath: self.filepath.value().expect("filepath").to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use polars::{df, frame::DataFrame, io::SerReader, prelude::CsvReader};

    use crate::{
        parser::keyword::{Keyword, StrKeyword},
        pipeline::context::DefaultPipelineContext,
        task::sink::{
            common::{Sink, SinkConfig},
            config::CsvSinkConfig,
        },
        util::{test::assert_frame_equal, tmp::TempFile},
    };

    use super::CsvSink;

    fn example() -> DataFrame {
        df!(
            "a" => [-1, 1, 3, 5, 6],
            "b" => ["z", "a", "j", "i", "c"],
        )
        .unwrap()
    }

    #[test]
    fn valid_csv_sink() {
        let expected = example();
        let tmp = TempFile::default();
        let csv_sink = CsvSink::new(&tmp.filepath);
        let ctx = Arc::new(DefaultPipelineContext::new());
        let _ = csv_sink.run(expected.clone(), ctx);
        let buffer = tmp.get().unwrap();
        let reader = CsvReader::new(buffer);
        let actual = reader.finish().unwrap();
        assert_frame_equal(actual, expected);
    }

    #[test]
    fn valid_csv_sink_config_to_csv_sink() {
        let expected = example();
        let tmp = TempFile::default();
        let mut source_config = CsvSinkConfig {
            filepath: StrKeyword::with_symbol("sample"),
        };
        let ctx = Arc::new(DefaultPipelineContext::new());
        let config = format!("sample: {}", &tmp.filepath);
        let context = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>(config.as_str()).unwrap();
        let _ = source_config.emplace(ctx.clone(), &context);
        let errors = source_config.validate();
        assert!(errors.is_empty());
        let actual_node = source_config.transform();
        actual_node.run(expected.clone(), ctx.clone()).unwrap();
        let buffer = tmp.get().unwrap();
        let reader = CsvReader::new(buffer);
        let actual = reader.finish().unwrap();
        assert_frame_equal(actual, expected);
    }
}
