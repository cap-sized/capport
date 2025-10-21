use crate::parser::keyword::{Keyword};
use crate::pipeline::context::{DefaultPipelineContext, PipelineContext};
use crate::task::transform::common::{Transform, TransformConfig};
use crate::task::transform::config::SqlTransformConfig;
use crate::util::error::{CpError, CpResult};
use crate::valid_or_insert_error;
use polars::prelude::LazyFrame;
use polars::sql::SQLContext;
use serde_yaml_ng::Mapping;
use std::sync::Arc;

pub struct SqlTransform {
    sql: String,
    sql_context: Option<Vec<String>>,
}

impl Transform for SqlTransform {
    fn run(&self, main: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        let mut context = SQLContext::new();

        for df_name in self.sql_context.as_ref().unwrap_or(&vec![]) {
            let df = ctx.extract_result(df_name)?;
            context.register(df_name, df);
        }

        context.register("self", main);
        let new_frame = context
            .execute(&self.sql)
            .map_err(|e| CpError::ComponentError("polars_sql error", e.to_string()))?;

        Ok(new_frame)
    }
}

impl TransformConfig for SqlTransformConfig {
    fn emplace(&mut self, context: &Mapping) -> CpResult<()> {
        self.sql.insert_value_from_context(context)?;
        Ok(())
    }
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        valid_or_insert_error!(errors, self.sql, "source[sql].sql");
        if self.sql.value().is_some() && self.sql.value().unwrap().is_empty() {
            errors.push(CpError::ConfigError(
                "SqlTransformConfig parsing error",
                "Empty sql - sql cannot be empty".to_string(),
            ));
        }
        errors
    }

    fn transform(&self) -> Box<dyn Transform> {
        Box::new(SqlTransform {
            sql: self.sql.value().unwrap().clone(),
            sql_context: self.sql_context.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::keyword::{Keyword, StrKeyword};
    use crate::pipeline::context::{DefaultPipelineContext, PipelineContext};
    use crate::task::transform::common::TransformConfig;
    use crate::task::transform::config::SqlTransformConfig;
    use crate::util::test::assert_frame_equal;
    use polars::df;
    use polars::prelude::IntoLazy;
    use std::sync::Arc;

    #[test]
    fn valid_sql_transform_basic() {
        let config = SqlTransformConfig {
            sql: StrKeyword::with_value("select * from self join BASIC on self.col = BASIC.my_col".to_owned()),
            sql_context: Some(vec!["BASIC".to_owned()]),
        };

        assert!(config.validate().is_empty());
        let sql = config.transform();

        let ctx = Arc::new(DefaultPipelineContext::with_results(&["BASIC"], 1));
        let basic = df!(
            "my_col" => [1, 2, 3, 3],
            "data_1" => [1, 2, 3, 4]
        )
        .unwrap()
        .lazy();
        ctx.insert_result("BASIC", basic).unwrap();

        let main = df!(
            "col"       => [2, 2, 3, 3, 4, 4],
            "data_2"    => [1, 2, 3, 4, 5, 6]
        )
        .unwrap()
        .lazy();

        let actual = sql.run(main, ctx).unwrap();
        let expected = df!(
            "my_col"        => [2, 2, 3, 3, 3, 3],
            "data_1"        => [2, 2, 3, 4, 3, 4],
            "col"           => [2, 2, 3, 3, 3, 3],
            "data_2"        => [1, 2, 3, 3, 4, 4],
        );

        assert_frame_equal(actual.collect().unwrap(), expected.unwrap());
    }

    #[test]
    fn valid_sql_transform_emplace_basic() {
        let mut config = SqlTransformConfig {
            sql: StrKeyword::with_symbol("empty"),
            sql_context: None,
        };
        let mapping = serde_yaml_ng::from_str::<serde_yaml_ng::Mapping>("empty: select now()").unwrap();
        config.emplace(&mapping).unwrap();
        assert_eq!(config.validate().len(), 0);
    }

    #[test]
    fn invalid_sql_transform_basic() {
        let configs = vec![SqlTransformConfig {
            sql: StrKeyword::with_value("".to_owned()),
            sql_context: None,
        }, SqlTransformConfig {
            sql: StrKeyword::with_symbol("empty"),
            sql_context: None,
        }];
        for config in configs {
            assert_eq!(config.validate().len(), 1);
        }
    }
}
