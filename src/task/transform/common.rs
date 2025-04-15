use std::fmt;

use polars::prelude::*;
use polars_lazy::prelude::*;
use yaml_rust2::Yaml;

use crate::util::error::{CpResult, PlResult, SubResult};

pub trait Transform {
    // TODO: Handle multiple joins
    fn binary(&self, left: LazyFrame, right: LazyFrame) -> SubResult<LazyFrame>;
    fn lazy_unary(&self, df: DataFrame) -> SubResult<LazyFrame>;
    fn unary(&self, df: LazyFrame) -> SubResult<LazyFrame>;
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result;
}

pub struct RootTransform {
    label: String,
    stages: Vec<Box<dyn Transform>>,
}

impl RootTransform {
    pub fn new(label: &str, stages: Vec<Box<dyn Transform>>) -> RootTransform {
        RootTransform {
            label: label.to_string(),
            stages: stages,
        }
    }
}

impl fmt::Debug for RootTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _ = write!(f, "{} [ ", &self.label);
        self.stages.iter().for_each(|transform| {
            let _ = transform.as_ref().fmt(f).unwrap();
            let _ = write!(f, ", ");
        });
        write!(f, " ]")
    }
}
