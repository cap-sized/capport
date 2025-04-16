use polars::prelude::*;
use polars_lazy::prelude::*;

use crate::{config::parser::join::parse_jointype, pipeline::results::PipelineResults, util::error::SubResult};

use super::{
    common::Transform,
    select::{SelectField, SelectTransform},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinTransform {
    pub join: String,
    pub right_select: Vec<SelectField>,
    pub how: JoinType,
    pub left_on: Vec<String>,
    pub right_on: Vec<String>,
}

impl JoinTransform {
    pub const fn keyword() -> &'static str {
        "join"
    }
    pub fn new(join: &str, left: &str, right: &str, right_select: &[SelectField], how: JoinType) -> JoinTransform {
        JoinTransform {
            join: join.to_owned(),
            left_on: left.split(',').map(|x| x.to_owned()).collect(),
            right_on: right.split(',').map(|x| x.to_owned()).collect(),
            right_select: right_select.to_vec(),
            how,
        }
    }
}

impl Transform for JoinTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", &self)
    }
    fn run(&self, curr: LazyFrame, results: &PipelineResults) -> SubResult<LazyFrame> {
        let right = match results.get_unchecked(&self.join) {
            Some(x) => x,
            None => return Err(format!("Dataframe named {} not found in results", &self.join)),
        };
        let mut select_cols: Vec<Expr> = vec![];
        for select in &self.right_select {
            match select.expr() {
                Ok(valid_expr) => select_cols.push(valid_expr),
                Err(err_msg) => return Err(format!("JoinTransform: {}", err_msg)),
            }
        }
        let left_on = self.left_on.iter().map(col).collect::<Vec<_>>();
        let right_on = self.right_on.iter().map(col).collect::<Vec<_>>();
        Ok(curr.join(
            right.select(select_cols),
            left_on,
            right_on,
            JoinArgs::new(self.how.clone()),
        ))
    }
}
