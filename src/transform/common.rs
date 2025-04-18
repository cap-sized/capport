use std::fmt;

use polars::prelude::*;
use polars_lazy::prelude::*;
use yaml_rust2::Yaml;

use crate::{
    pipeline::results::PipelineResults,
    util::error::{CpResult, PlResult, SubResult},
};

pub trait Transform {
    fn run(&self, curr: LazyFrame, results: &PipelineResults) -> SubResult<LazyFrame>;
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

    fn run(&self, curr: LazyFrame, results: &PipelineResults) -> SubResult<LazyFrame> {
        let mut next = curr;
        for stage in &self.stages {
            next = stage.as_ref().run(next, results)?
        }
        Ok(next)
    }
}
