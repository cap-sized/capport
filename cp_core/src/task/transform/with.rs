use std::{collections::HashMap, sync::Arc};

use polars::prelude::{Expr, LazyFrame, Null, coalesce, col, lit};

use crate::{
    parser::keyword::Keyword,
    pipeline::context::DefaultPipelineContext,
    util::error::{CpError, CpResult},
};

use super::{
    common::{Transform, TransformConfig},
    config::WithColTransformConfig,
};

pub struct WithColTransform {
    with_columns: Vec<Expr>,
}

impl Transform for WithColTransform {
    fn run(&self, main: LazyFrame, _ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        log::debug!(
            "WithColTransform: \n\t{}",
            self.with_columns
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(",\n\t")
        );
        Ok(main.with_columns(&self.with_columns))
    }
}

impl TransformConfig for WithColTransformConfig {
    fn emplace(&mut self, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        let mut fields = HashMap::new();
        log::debug!("original model: {:?}", self);
        for (alias_kw, expr_kw) in &self.with_columns {
            let mut alias = alias_kw.clone();
            alias.insert_value_from_context(context)?;
            let mut expr = expr_kw.clone();
            expr.insert_value_from_context(context)?;
            fields.insert(alias, expr);
        }
        self.with_columns = fields;
        Ok(())
    }
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        for (alias_kw, expr_kw) in &self.with_columns {
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
    fn transform(&self) -> Box<dyn Transform> {
        let mut with_columns = vec![];
        for (alias_kw, expr_kw) in &self.with_columns {
            let alias = alias_kw.value().expect("alias").clone();
            let expr = expr_kw.value().expect("expr").clone();
            with_columns.push(coalesce(&[expr, col(format!("^{}$", alias)), lit(Null {})]).alias(alias));
        }
        Box::new(WithColTransform { with_columns })
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
        task::transform::{common::TransformConfig, config::WithColTransformConfig},
    };

    fn create_with_columns_good_config(map: HashMap<&'static str, Expr>) -> HashMap<StrKeyword, PolarsExprKeyword> {
        map.into_iter()
            .map(|(alias, expr)| {
                (
                    StrKeyword::with_value(alias.to_owned()),
                    PolarsExprKeyword::with_value(expr),
                )
            })
            .collect()
    }

    fn create_with_columns_bad_config(
        good: HashMap<&'static str, Expr>,
        bad: HashMap<StrKeyword, PolarsExprKeyword>,
    ) -> HashMap<StrKeyword, PolarsExprKeyword> {
        let mut good_stuff = create_with_columns_good_config(good);
        bad.into_iter().for_each(|(alias_kw, expr_kw)| {
            good_stuff.insert(alias_kw, expr_kw);
        });
        good_stuff
    }

    #[test]
    fn valid_with_columns_transform_basic() {
        let config = WithColTransformConfig {
            with_columns: create_with_columns_good_config(HashMap::from([("one", col("two")), ("three", col("two"))])),
        };
        assert!(config.validate().is_empty());
        let with_columns = config.transform();
        let ctx = Arc::new(DefaultPipelineContext::new());
        let main = df!(
            "two" => [1, 2, 3]
        )
        .unwrap()
        .lazy();
        let actual = with_columns.run(main, ctx).unwrap();
        let expected = df!(
            "one" => [1, 2, 3],
            "two" => [1, 2, 3],
            "three" => [1, 2, 3],
        )
        .unwrap();
        assert_eq!(actual.collect().unwrap(), expected);
    }

    #[test]
    fn invalid_with_columns_transform_basic() {
        let config = WithColTransformConfig {
            with_columns: create_with_columns_bad_config(
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
