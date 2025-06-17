use crate::pipeline::context::DefaultPipelineContext;
use crate::task::transform::common::{Transform, TransformConfig};
use crate::util::error::{CpError, CpResult};
use polars::prelude::{Expr, LazyFrame, NonExistent, StrptimeOptions, TimeUnit, col, format_str};
use serde_yaml_ng::Mapping;
use std::sync::Arc;

use super::config::TimeConvertConfig;

pub struct TimeConvertTransform {
    cast: Vec<Expr>,
}

impl Transform for TimeConvertTransform {
    fn run(&self, main: LazyFrame, _ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        Ok(main.with_columns(self.cast.clone()))
    }
}

impl TransformConfig for TimeConvertConfig {
    fn emplace(&mut self, context: &Mapping) -> CpResult<()> {
        self.time.emplace(context)
    }

    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        self.time.validate(&mut errors);
        errors
    }

    fn transform(&self) -> Box<dyn Transform> {
        let include = self.time.get_include_str();
        let mut cast = vec![];
        let fmt = self.time.into.clone();
        let strptime_options = StrptimeOptions {
            format: Some(fmt.as_str().into()),
            ..Default::default()
        };
        let is_date = fmt.contains("%d");
        let is_time = fmt.contains("%M");
        match fmt.as_str() {
            "%Y-%m-%dT%H:%M:%SZ" => include.iter().for_each(|expr| {
                cast.push(col(expr).cast(polars::prelude::DataType::Datetime(
                    TimeUnit::Nanoseconds,
                    None,
                )).dt().replace_time_zone(Some("UTC".into()), col(expr), NonExistent::Null));
            }),
            "%M:%S" => include.iter().for_each(|expr| {
                cast.push(
                    format_str("00:{}", &[col(expr)])
                        .expect("bad time str: not str")
                        .str()
                        .to_time(StrptimeOptions {
                            format: Some("%H:%M:%S".into()),
                            ..Default::default()
                        })
                        .alias(expr),
                );
            }),
            _ => include.iter().for_each(|expr| {
                let options = strptime_options.clone();
                if is_date && is_time {
                    cast.push(col(expr).str().to_datetime(None, None, options, col(expr)));
                } else if is_time {
                    cast.push(col(expr).str().to_time(options));
                } else if is_date {
                    cast.push(col(expr).str().to_date(options));
                }
            }),
        }
        Box::new(TimeConvertTransform { cast })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use polars::{
        df,
        prelude::{col, DatetimeArgs, datetime, lit, IntoLazy, StrptimeOptions, TimeUnit},
    };

    use crate::{
        pipeline::context::DefaultPipelineContext,
        task::transform::{common::TransformConfig, config::TimeConvertConfig},
    };

    #[test]
    fn valid_time_convert() {
        let main = df!(
            "time" => ["10:09", "08:20", "19:33"],
        )
        .unwrap()
        .lazy();
        let config: TimeConvertConfig = serde_yaml_ng::from_str(r#"time: {include: [time], into: "%M:%S"}"#).unwrap();
        let errors = config.validate();
        assert!(errors.is_empty());
        let node = config.transform();
        let ctx = Arc::new(DefaultPipelineContext::new());
        let result = node.run(main, ctx);
        let actual = result.unwrap().collect().unwrap();
        let expected = df!(
            "time" => ["00:10:09", "00:08:20", "00:19:33"],
        )
        .unwrap()
        .lazy()
        .with_columns([col("time").str().to_time(StrptimeOptions::default())])
        .collect()
        .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_date_convert() {
        let main = df!(
            "datetime" => ["2025-04-06T10:20:40Z"],
        )
        .unwrap()
        .lazy();
        let config: TimeConvertConfig =
            serde_yaml_ng::from_str(r#"time: {include: [datetime], into: "%Y-%m-%dT%H:%M:%SZ"}"#).unwrap();
        let errors = config.validate();
        assert!(errors.is_empty());
        let node = config.transform();
        let ctx = Arc::new(DefaultPipelineContext::new());
        let result = node.run(main, ctx);
        let actual = result.unwrap().collect().unwrap();
        let expected = df!(
            "year" => [2025],
            "month" => [4],
            "day" => [6],
            "hr" => [10],
            "min" => [20],
            "sec" => [40],
            "tz" => ["UTC"],
        ).unwrap().lazy().select([
            datetime(DatetimeArgs {
                year: col("year"),
                month: col("month"),
                day: col("day"),
                hour: col("hr"),
                minute: col("min"),
                second: col("sec"),
                microsecond: lit(0),
                time_unit: TimeUnit::Nanoseconds,
                time_zone: Some("UTC".into()),
                ambiguous: col("year"),
            })
        ]).collect().unwrap();

        assert_eq!(actual, expected);
    }
}
