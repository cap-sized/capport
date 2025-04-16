use polars::prelude::*;
use polars_lazy::prelude::*;

use crate::{pipeline::results::PipelineResults, util::error::SubResult};

use super::{common::Transform, expr::parse_str_to_col_expr, select::SelectField};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropField {
    pub target: String,
    pub delete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTransform {
    pub deletes: Vec<DropField>,
}

impl DropTransform {
    pub const fn keyword() -> &'static str {
        "drop"
    }
    pub fn new(deletes: Vec<DropField>) -> DropTransform {
        DropTransform { deletes }
    }
}

impl Transform for DropTransform {
    fn run(&self, curr: LazyFrame, results: &PipelineResults) -> SubResult<LazyFrame> {
        let mut drop_cols: Vec<Expr> = vec![];
        for delete in &self.deletes {
            match delete.expr() {
                Ok(valid_expr) => drop_cols.push(valid_expr),
                Err(err_msg) => return Err(format!("DropTransform: {}", err_msg)),
            }
        }
        Ok(curr.drop(drop_cols))
    }
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", &self.deletes)
    }
}

impl DropField {
    pub fn new(target: &str) -> DropField {
        DropField {
            target: target.to_string(),
            delete: true,
        }
    }

    pub fn expr(&self) -> SubResult<Expr> {
        // Currently only supports top level column deletion, structs cannot be modified
        Ok(col(&self.target))
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::PlSmallStr;
    use polars::{df, docs::lazy};
    use polars_lazy::prelude::Expr;
    use polars_lazy::{dsl::col, frame::IntoLazy};

    use crate::pipeline::results::PipelineResults;

    use super::{DropField, DropTransform, Transform};

    #[test]
    fn valid_drop_basic() {
        let sample_df = df![
            "Price" => [2.3, 102.023, 19.88],
            "Instr" => ["ABAB", "TORO", "PKJT"],
        ]
        .unwrap()
        .lazy();
        let transform = DropTransform::new(vec![DropField::new("Price")]);
        let results = PipelineResults::new();
        let actual_df = transform.run(sample_df, &results).unwrap().collect().unwrap();
        assert_eq!(
            actual_df,
            df![
                "instrument" => ["ABAB", "TORO", "PKJT"],
            ]
            .unwrap()
        );
    }
}
