use crate::pipeline::context::{DefaultPipelineContext, PipelineContext};
use crate::task::transform::common::{Transform, TransformConfig};
use crate::task::transform::config::SqlTransformConfig;
use crate::util::error::{CpError, CpResult};
use polars::prelude::LazyFrame;
use polars::sql::SQLContext;
use serde_yaml_ng::Mapping;
use std::sync::Arc;

pub struct SqlTransform {
    sql: String,
}

impl Transform for SqlTransform {
    fn run(&self, main: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        let mut context = SQLContext::new();

        for df_name in ctx.get_labels()? {
            let df = ctx.extract_result(&df_name)?;
            context.register(&df_name, df);
        }

        context.register("self", main);
        let new_frame = context
            .execute(&self.sql)
            .map_err(|e| CpError::ComponentError("polars_sql error", e.to_string()))?;

        Ok(new_frame)
    }
}

impl TransformConfig for SqlTransformConfig {
    fn emplace(&mut self, _context: &Mapping) -> CpResult<()> {
        Ok(())
    }
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        if self.sql.is_empty() {
            errors.push(CpError::ConfigError(
                "SqlTransformConfig parsing error",
                "Empty sql - sql cannot be empty".to_string(),
            ));
        }
        errors
    }

    fn transform(&self) -> Box<dyn Transform> {
        Box::new(SqlTransform { sql: self.sql.clone() })
    }
}

#[cfg(test)]
mod tests {
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
            sql: "select * from self join BASIC on self.col = BASIC.my_col".to_owned(),
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
    fn invalid_sql_transform_basic() {
        let config = SqlTransformConfig { sql: "".to_owned() };
        assert_eq!(config.validate().len(), 1);
    }
}
