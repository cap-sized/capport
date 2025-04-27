use std::{fmt, sync::RwLock};

use polars::prelude::*;

use crate::{pipeline::results::PipelineResults, util::error::CpResult};

pub trait Transform {
    fn run_lazy(&self, curr: LazyFrame, results: Arc<RwLock<PipelineResults<LazyFrame>>>) -> CpResult<LazyFrame>;
    // fn run_eager(&self, curr: DataFrame, results: &PipelineResults<DataFrame>) -> SubResult<DataFrame>;
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result;
}

pub struct RootTransform {
    pub label: String,
    stages: Vec<Box<dyn Transform>>,
}

impl RootTransform {
    pub fn new(label: &str, stages: Vec<Box<dyn Transform>>) -> RootTransform {
        RootTransform {
            label: label.to_string(),
            stages,
        }
    }
}

impl Transform for RootTransform {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let _ = write!(f, "{} [ ", &self.label);
        self.stages.iter().for_each(|transform| {
            transform.as_ref().fmt(f).unwrap();
            let _ = write!(f, ", ");
        });
        write!(f, " ]")
    }

    fn run_lazy(&self, curr: LazyFrame, results: Arc<RwLock<PipelineResults<LazyFrame>>>) -> CpResult<LazyFrame> {
        let mut next = curr;
        for stage in &self.stages {
            next = stage.as_ref().run_lazy(next, results.clone())?
        }
        Ok(next)
    }
}
