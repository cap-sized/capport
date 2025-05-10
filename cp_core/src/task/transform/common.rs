use std::{fmt, sync::RwLock};

use polars::prelude::*;

use crate::util::error::CpResult;

pub trait Transform {
    fn run(&self) -> CpResult<()>;
    // global inputs/outputs
    fn named_inputs(&self) -> &[String];
    fn named_outputs(&self) -> &[String];
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
