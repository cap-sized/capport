use std::sync::Arc;

use polars::prelude::{Expr, LazyFrame};

use crate::{
    parser::keyword::Keyword,
    pipeline::context::DefaultPipelineContext,
    util::error::{CpError, CpResult},
};

use super::{
    common::{TransformConfig, Transform},
    config::SelectTransformConfig,
};

pub struct SelectTransform {
    select: Vec<Expr>,
}

impl Transform for SelectTransform {
    fn run(&self, main: LazyFrame, _ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        Ok(main.select(&self.select))
    }
}

impl TransformConfig for SelectTransformConfig {
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        for (alias_kw, expr_kw) in &self.select {
            match alias_kw.value() {
                Some(_) => {}
                None => errors.push(CpError::SymbolMissingValueError(
                    "alias",
                    alias_kw.symbol().unwrap_or("?").to_owned(),
                )),
            }
            match expr_kw.value() {
                Some(_) => {}
                None => errors.push(CpError::SymbolMissingValueError(
                    "expr",
                    expr_kw.symbol().unwrap_or("?").to_owned(),
                )),
            };
        }
        errors
    }
    fn transform(self) -> Box<dyn Transform> {
        let mut select = vec![];
        for (alias_kw, expr_kw) in self.select {
            let alias = alias_kw.value().expect("alias").clone();
            let expr = expr_kw.value().expect("expr").clone();
            select.push(expr.alias(alias));
        }
        Box::new(SelectTransform { select })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use polars::{
        df,
        prelude::{Expr, IntoLazy, col},
    };

    use crate::{
        parser::keyword::{Keyword, PolarsExprKeyword, StrKeyword},
        pipeline::context::DefaultPipelineContext,
        task::transform::{common::TransformConfig, config::SelectTransformConfig},
    };

    fn create_select_good_config(map: HashMap<&'static str, Expr>) -> HashMap<StrKeyword, PolarsExprKeyword> {
        map.into_iter()
            .map(|(alias, expr)| {
                (
                    StrKeyword::with_value(alias.to_owned()),
                    PolarsExprKeyword::with_value(expr),
                )
            })
            .collect()
    }

    fn create_select_bad_config(
        good: HashMap<&'static str, Expr>,
        bad: HashMap<StrKeyword, PolarsExprKeyword>,
    ) -> HashMap<StrKeyword, PolarsExprKeyword> {
        let mut good_stuff = create_select_good_config(good);
        bad.into_iter().for_each(|(alias_kw, expr_kw)| {
            good_stuff.insert(alias_kw, expr_kw);
        });
        good_stuff
    }

    #[test]
    fn valid_select_transform_basic() {
        let config = SelectTransformConfig {
            select: create_select_good_config(HashMap::from([("one", col("two")), ("three", col("two"))])),
        };
        assert!(config.validate().is_empty());
        let select = config.transform();
        let ctx = Arc::new(DefaultPipelineContext::new());
        let main = df!(
            "two" => [1, 2, 3]
        )
        .unwrap()
        .lazy();
        let actual = select.run(main, ctx).unwrap();
        let expected = df!(
            "one" => [1, 2, 3],
            "three" => [1, 2, 3],
        )
        .unwrap();
        assert_eq!(actual.collect().unwrap(), expected);
    }

    #[test]
    fn invalid_select_transform_basic() {
        let config = SelectTransformConfig {
            select: create_select_bad_config(
                HashMap::from([("one", col("two")), ("three", col("two"))]),
                HashMap::from([
                    (StrKeyword::with_symbol("not"), PolarsExprKeyword::with_value(col("ok"))),
                    (
                        StrKeyword::with_value("ok".to_owned()),
                        PolarsExprKeyword::with_symbol("not"),
                    ),
                ]),
            ),
        };
        assert_eq!(config.validate().len(), 2);
    }
}
