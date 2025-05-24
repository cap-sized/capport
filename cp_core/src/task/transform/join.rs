use std::sync::Arc;

use polars::prelude::{Expr, IntoLazy, JoinArgs, LazyFrame, all, col};

use crate::{
    parser::keyword::Keyword,
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    util::error::{CpError, CpResult}, valid_or_insert_error,
};

use super::{
    common::{Transform, TransformConfig},
    config::JoinTransformConfig,
};

pub struct JoinTransform {
    right_label: String,
    left_prefix: Option<Vec<Expr>>,
    right_prefix: Option<Vec<Expr>>,
    /// selects after prefix is applied
    right_select: Vec<Expr>,
    left_on: Vec<Expr>,
    right_on: Vec<Expr>,
    join_args: JoinArgs,
}

impl Transform for JoinTransform {
    fn run(&self, main: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        let right_join = ctx.extract_clone_result(&self.right_label)?.lazy();
        let left: LazyFrame = self
            .left_prefix
            .as_ref()
            .map_or_else(|| main.clone(), |x| main.clone().with_columns(x));
        let right: LazyFrame = self
            .right_prefix
            .as_ref()
            .map_or_else(|| right_join.clone(), |x| right_join.clone().with_columns(x));
        let joined = left.join(
            if self.right_select.is_empty() {
                right
            } else {
                right.select(&self.right_select)
            },
            &self.left_on,
            &self.right_on,
            self.join_args.clone(),
        );
        Ok(joined)
    }
}

impl TransformConfig for JoinTransformConfig {
    fn emplace(&mut self, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        self.join.right_prefix.iter_mut().map(|sp| sp.insert_value_from_context(context));
        self.join.left_prefix.iter_mut().map(|sp| sp.insert_value_from_context(context));
        for on in &mut self.join.left_on {
            on.insert_value_from_context(context);
        }
        for on in &mut self.join.right_on {
            on.insert_value_from_context(context);
        }
        Ok(())
    }
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        if let Some(rp) = self.join.right_prefix.as_ref() {
            valid_or_insert_error!(errors, rp, "transform[join].right_prefix");
        }

        if let Some(lp) = self.join.left_prefix.as_ref() {
            valid_or_insert_error!(errors, lp, "transform[join].left_prefix");
        }

        for on in &self.join.left_on {
            valid_or_insert_error!(errors, on, "transform[join].left_on");
        }

        for on in &self.join.right_on {
            valid_or_insert_error!(errors, on, "transform[join].right_on");
        }

        valid_or_insert_error!(errors, self.join.right, "transform[join].right");
        errors
    }

    fn transform(&self) -> Box<dyn Transform> {
        let right_select = if let Some(selects) = &self.join.right_select {
            selects
                .iter()
                .map(|(alias_kw, expr_kw)| {
                    let alias = alias_kw.value().expect("alias").clone();
                    let expr = expr_kw.value().expect("expr").clone();
                    expr.alias(alias)
                })
                .collect()
        } else {
            vec![]
        };

        let right_label = self.join.right.value().expect("right").clone();
        let left_prefix = self.join.left_prefix.as_ref().map_or_else(
            || None,
            |x| Some(vec![all().name().prefix(x.value().expect("left_prefix"))]),
        );
        let right_prefix = self.join.right_prefix.as_ref().map_or_else(
            || None,
            |x| Some(vec![all().name().prefix(x.value().expect("right_prefix"))]),
        );

        let left_on = self
            .join
            .left_on
            .iter()
            .map(|x| col(x.value().expect("left_on")))
            .collect::<Vec<_>>();

        let right_on = self
            .join
            .right_on
            .iter()
            .map(|x| col(x.value().expect("right_on")))
            .collect::<Vec<_>>();

        let join_args = JoinArgs::new(self.join.how.clone().into());

        let join = JoinTransform {
            right_on,
            left_on,
            left_prefix,
            right_prefix,
            right_label,
            right_select,
            join_args,
        };
        Box::new(join)
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::jtype::JType;
    use crate::parser::keyword::{Keyword, PolarsExprKeyword, StrKeyword};
    use crate::pipeline::context::{DefaultPipelineContext, PipelineContext};
    use crate::task::transform::common::TransformConfig;
    use crate::task::transform::config::{_JoinTransformConfig, JoinTransformConfig};
    use crate::util::test::assert_frame_equal;
    use polars::df;
    use polars::prelude::{IntoLazy, JoinType, col};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn valid_join_transform_basic() {
        let test_cases = vec![
            (
                JoinType::Inner,
                df!(
                    "col"           => [2, 2, 3, 3, 3, 3],
                    "data_2"        => [1, 2, 3, 3, 4, 4],
                    "left_col"      => [2, 2, 3, 3, 3, 3],
                    "left_data_2"   => [1, 2, 3, 3, 4, 4],
                    "data_1"        => [2, 2, 3, 4, 3, 4],
                ),
            ),
            (
                JoinType::Left,
                df!(
                    "col"           => [2, 2, 3, 3, 3, 3, 4, 4],
                    "data_2"        => [1, 2, 3, 3, 4, 4, 5, 6],
                    "left_col"      => [2, 2, 3, 3, 3, 3, 4, 4],
                    "left_data_2"   => [1, 2, 3, 3, 4, 4, 5, 6],
                    "data_1"        => [Some(2), Some(2), Some(3), Some(4), Some(3), Some(4), None, None],
                ),
            ),
            (
                JoinType::Right,
                df!(
                    "col"           => [1, 2, 2, 3, 3, 3, 3],
                    "data_2"        => [None, Some(1), Some(2), Some(3), Some(4), Some(3), Some(4)],
                    "left_col"      => [None, Some(2), Some(2), Some(3), Some(3), Some(3), Some(3)],
                    "left_data_2"   => [None, Some(1), Some(2), Some(3), Some(4), Some(3), Some(4)],
                    "data_1"        => [1, 2, 2, 3, 3, 4, 4],
                ),
            ),
            (
                JoinType::Full,
                df!(
                    "col"           => [None, Some(2), Some(2), Some(3), Some(3), Some(3), Some(3), Some(4), Some(4)],
                    "col_right"     => [Some(1), Some(2), Some(2), Some(3), Some(3), Some(3), Some(3), None, None],
                    "data_1"        => [Some(1), Some(2), Some(2), Some(3), Some(3), Some(4), Some(4), None, None],
                    "data_2"        => [None, Some(1), Some(2), Some(3), Some(4), Some(3), Some(4), Some(5), Some(6)],
                    "left_col"      => [None, Some(2), Some(2), Some(3), Some(3), Some(3), Some(3), Some(4), Some(4)],
                    "left_data_2"   => [None, Some(1), Some(2), Some(3), Some(4), Some(3), Some(4), Some(5), Some(6)],
                ),
            ),
        ];

        for (join_type, expected) in test_cases {
            let config = JoinTransformConfig {
                join: _JoinTransformConfig {
                    right: StrKeyword::with_value("BASIC".to_owned()),
                    left_on: vec![StrKeyword::with_value("col".to_owned())],
                    right_on: vec![StrKeyword::with_value("col".to_owned())],
                    left_prefix: Some(StrKeyword::with_value("left_".to_owned())),
                    right_prefix: None,
                    right_select: Some(HashMap::from([
                        (
                            StrKeyword::with_value("col".to_owned()),
                            PolarsExprKeyword::with_value(col("my_col")),
                        ),
                        (
                            StrKeyword::with_value("data_1".to_owned()),
                            PolarsExprKeyword::with_value(col("data_1")),
                        ),
                    ])),
                    how: JType(join_type),
                },
            };

            assert!(config.validate().is_empty());

            let join = config.transform();
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

            let actual = join.run(main, ctx).unwrap();
            assert_frame_equal(actual.collect().unwrap(), expected.unwrap());
        }
    }
    #[test]
    fn invalid_join_transform_basic() {
        let config = JoinTransformConfig {
            join: _JoinTransformConfig {
                right: StrKeyword::with_value("BASIC".to_owned()),
                left_on: vec![StrKeyword::with_symbol("col")],
                right_on: vec![StrKeyword::with_symbol("col")],
                left_prefix: Some(StrKeyword::with_value("left_".to_owned())),
                right_prefix: None,
                right_select: Some(HashMap::from([
                    (
                        StrKeyword::with_value("col".to_owned()),
                        PolarsExprKeyword::with_value(col("my_col")),
                    ),
                    (
                        StrKeyword::with_value("data_1".to_owned()),
                        PolarsExprKeyword::with_value(col("data_1")),
                    ),
                ])),
                how: JType(JoinType::Left),
            },
        };

        assert_eq!(config.validate().len(), 2);
    }
}
