use std::sync::Arc;

use polars::prelude::{Expr, IntoLazy, JoinArgs, LazyFrame, all, col};

use crate::{
    parser::keyword::Keyword,
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    util::error::{CpError, CpResult},
};

use super::{
    common::{TransformConfig, Transform},
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
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        if let Some(rp) = self.join.right_prefix.as_ref() {
            match rp.value() {
                Some(_) => {}
                None => errors.push(CpError::SymbolMissingValueError(
                    "right_prefix",
                    rp.symbol().unwrap_or("?").to_owned(),
                )),
            }
        }

        if let Some(lp) = self.join.left_prefix.as_ref() {
            match lp.value() {
                Some(_) => {}
                None => errors.push(CpError::SymbolMissingValueError(
                    "left_prefix",
                    lp.symbol().unwrap_or("?").to_owned(),
                )),
            }
        }

        for on in &self.join.left_on {
            match on.value() {
                Some(_) => {}
                None => errors.push(CpError::SymbolMissingValueError(
                    "left_on",
                    on.symbol().unwrap_or("?").to_owned(),
                )),
            }
        }

        for on in &self.join.right_on {
            match on.value() {
                Some(_) => {}
                None => errors.push(CpError::SymbolMissingValueError(
                    "right_on",
                    on.symbol().unwrap_or("?").to_owned(),
                )),
            }
        }

        match self.join.right.value() {
            Some(_) => {}
            None => errors.push(CpError::SymbolMissingValueError(
                "right",
                self.join.right.symbol().unwrap_or("?").to_owned(),
            )),
        }

        errors
    }

    fn transform(self) -> Box<dyn Transform> {
        let right_select = if let Some(selects) = self.join.right_select {
            let mut rselect = vec![];
            for (alias_kw, expr_kw) in selects {
                let alias = alias_kw.value().expect("alias").clone();
                let expr = expr_kw.value().expect("expr").clone();
                rselect.push(expr.alias(alias));
            }
            rselect
        } else {
            vec![]
        };

        let right_label = self.join.right.value().expect("right").to_owned();
        let left_prefix = self.join.left_prefix.map_or_else(
            || None,
            |x| Some(vec![all().name().prefix(x.value().expect("left_prefix"))]),
        );
        let right_prefix = self.join.right_prefix.map_or_else(
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

        let join_args = JoinArgs::new(self.join.how.into());

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
    // YX TODO: implement tests
}
