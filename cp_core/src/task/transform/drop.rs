use crate::parser::keyword::Keyword;
use crate::pipeline::context::DefaultPipelineContext;
use crate::task::transform::common::{Transform, TransformConfig};
use crate::task::transform::config::DropTransformConfig;
use crate::util::error::{CpError, CpResult};
use crate::valid_or_insert_error;
use polars::prelude::{Expr, LazyFrame};
use serde_yaml_ng::Mapping;
use std::sync::Arc;

pub struct DropTransform {
    drop: Vec<Expr>,
}

impl Transform for DropTransform {
    fn run(&self, main: LazyFrame, _ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        Ok(main.drop(self.drop.clone()))
    }
}

impl TransformConfig for DropTransformConfig {
    fn emplace(&mut self, context: &Mapping) -> CpResult<()> {
        let mut drop = vec![];
        for expr_kw in &self.drop {
            let mut expr = expr_kw.clone();
            expr.insert_value_from_context(context)?;
            drop.push(expr);
        }
        self.drop = drop;
        Ok(())
    }

    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        for expr_kw in &self.drop {
            valid_or_insert_error!(errors, expr_kw, "drop_expr");
        }
        errors
    }

    fn transform(&self) -> Box<dyn Transform> {
        let mut drop = vec![];
        for expr_kw in &self.drop {
            let expr = expr_kw.value().expect("expr").clone();
            drop.push(expr);
        }
        Box::new(DropTransform { drop })
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::keyword::{Keyword, PolarsExprKeyword};
    use crate::pipeline::context::DefaultPipelineContext;
    use crate::task::transform::common::TransformConfig;
    use crate::task::transform::config::DropTransformConfig;
    use polars::df;
    use polars::prelude::{IntoLazy, col};
    use std::sync::Arc;

    #[test]
    fn valid_drop_transform_basic() {
        let config = DropTransformConfig {
            drop: vec![PolarsExprKeyword::with_value(col("Price"))],
        };
        assert!(config.validate().is_empty());
        let drop = config.transform();
        let ctx = Arc::new(DefaultPipelineContext::new());
        let main = df!(
            "Price" => [2.3, 102.023, 19.88],
            "Instr" => ["ABAB", "TORO", "PKJT"],
        )
        .unwrap()
        .lazy();
        let actual = drop.run(main, ctx).unwrap();
        let expected = df!(
            "Instr" => ["ABAB", "TORO", "PKJT"],
        )
        .unwrap();
        assert_eq!(actual.collect().unwrap(), expected);
    }

    #[test]
    fn invalid_drop_transform_basic() {
        let config = DropTransformConfig {
            drop: vec![PolarsExprKeyword::with_symbol("price")],
        };
        assert_eq!(config.validate().len(), 1);
    }
}
