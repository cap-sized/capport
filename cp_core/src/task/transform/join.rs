use std::sync::Arc;

use polars::prelude::{all, Expr, IntoLazy, JoinArgs, LazyFrame};

use crate::{parser::keyword::Keyword, pipeline::context::{DefaultPipelineContext, PipelineContext}, util::error::{CpError, CpResult}};

use super::{common::{SubTransformConfig, Transform}, config::SelectTransformConfig};

pub struct JoinTransform {
    right_label: String,
    left_prefix: Option<Expr>,
    right_prefix: Option<Expr>,
    /// selects after prefix is applied
    select_right: Vec<Expr>,
    left_on: Vec<Expr>,
    right_on: Vec<Expr>,
    join_args: JoinArgs
}

impl Transform for JoinTransform {
    fn run(&self, main: LazyFrame, ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        let right_join = ctx.extract_clone_result(&self.right_label)?.lazy();
        let left: LazyFrame = self.left_prefix.as_ref().map_or_else(|| main.clone(), |x| main.clone().with_columns(&[x.clone()]));
        let right: LazyFrame = self.right_prefix.as_ref().map_or_else(|| right_join.clone(), |x| right_join.clone().with_columns(&[x.clone()]));
        let joined = left.join(if self.select_right.is_empty() {
            right
        } else {
            right.select(&self.select_right)
        }, &self.left_on, &self.right_on, self.join_args.clone());
        Ok(joined)
    }
}


#[cfg(test)]
mod tests {
}
