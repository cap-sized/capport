use polars::prelude::*;
use polars_lazy::prelude::*;

use crate::{config::parser::join::parse_jointype, util::error::SubResult};

use super::{
    common::Transform,
    select::{SelectField, SelectTransform},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinTransform {
    pub right_select: Vec<SelectField>,
    pub how: JoinType, // TODO: Enum
    pub left_on: Vec<String>,
    pub right_on: Vec<String>,
}

// TODO: implement join transform.
impl JoinTransform {
    pub const fn keyword() -> &'static str {
        "join"
    }
    pub fn new(left: &str, right: &str, right_select: Vec<SelectField>, how: JoinType) -> JoinTransform {
        JoinTransform {
            left_on: left.split(',').map(|x| x.to_owned()).collect(),
            right_on: right.split(',').map(|x| x.to_owned()).collect(),
            right_select,
            how,
        }
    }
}

impl Transform for JoinTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", &self)
    }
    fn lazy_unary(&self, df: DataFrame) -> SubResult<LazyFrame> {
        self.unary(df.lazy())
    }
    fn binary(&self, left: LazyFrame, right: LazyFrame) -> SubResult<LazyFrame> {
        let mut select_cols: Vec<Expr> = vec![];
        for select in &self.right_select {
            match select.expr() {
                Ok(valid_expr) => select_cols.push(valid_expr),
                Err(err_msg) => return Err(format!("JoinTransform: {}", err_msg)),
            }
        }
        let left_on = self.left_on.iter().map(col).collect::<Vec<_>>();
        let right_on = self.right_on.iter().map(col).collect::<Vec<_>>();
        Ok(left.join(
            right.select(select_cols),
            left_on,
            right_on,
            JoinArgs::new(self.how.clone()),
        ))
    }

    fn unary(&self, df: LazyFrame) -> SubResult<LazyFrame> {
        Err("Unary op not implemented for SelectTransform".to_owned())
    }
}
