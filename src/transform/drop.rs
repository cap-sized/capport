use std::sync::RwLock;

use polars::prelude::*;

use crate::{
    pipeline::results::PipelineResults,
    util::error::{CpError, CpResult, SubResult},
};

use super::common::Transform;

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
    pub fn new(deletes: &[DropField]) -> DropTransform {
        DropTransform {
            deletes: deletes.to_vec(),
        }
    }
}

impl Transform for DropTransform {
    fn run_lazy(&self, curr: LazyFrame, _results: Arc<RwLock<PipelineResults<LazyFrame>>>) -> CpResult<LazyFrame> {
        let mut drop_cols: Vec<Expr> = vec![];
        for delete in &self.deletes {
            if !delete.delete {
                continue;
            }
            match delete.expr() {
                Ok(valid_expr) => drop_cols.push(valid_expr),
                Err(err_msg) => {
                    return Err(CpError::PipelineError("DropTransform failed", err_msg));
                }
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
    use std::sync::{Arc, RwLock};

    use polars::df;
    use polars::prelude::LazyFrame;

    use polars_lazy::frame::IntoLazy;

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
        let transform = DropTransform::new([DropField::new("Price")].as_slice());
        let results = Arc::new(RwLock::new(PipelineResults::<LazyFrame>::default()));
        let actual_df = transform.run_lazy(sample_df, results).unwrap().collect().unwrap();
        assert_eq!(
            actual_df,
            df![
                "instrument" => ["ABAB", "TORO", "PKJT"],
            ]
            .unwrap()
        );
    }
}
