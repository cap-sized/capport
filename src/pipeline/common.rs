use crate::{
    model::common::Model,
    transform::common::{RootTransform, Transform},
    util::{
        common::yaml_from_str,
        error::{CpResult, SubResult},
    },
};

use super::{context::Context, results::PipelineResults};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pipeline {
    pub label: String,
    pub stages: Vec<PipelineStage>,
}

#[derive(Clone, Debug, Eq)]
pub struct PipelineStage {
    pub label: String,
    pub task: PipelineTask,
    pub args_yaml_str: String,
}

impl PartialEq for PipelineStage {
    fn eq(&self, other: &Self) -> bool {
        let this_args = match yaml_from_str(&self.args_yaml_str) {
            Some(x) => x,
            None => return false,
        };
        let other_args = match yaml_from_str(&other.args_yaml_str) {
            Some(x) => x,
            None => return false,
        };
        self.label == other.label && this_args == other_args
    }
}

pub trait RunTask {
    fn task(&self) -> SubResult<PipelineTask>;
}

// Eventually we will need to make live stages which acculumate their own results over time.
// They will extend the current functionality of PipelineStage.
pub trait LoopJobStage {
    fn poll();
    fn push();
}

pub type PipelineTask = fn(Box<Context>) -> CpResult<PipelineResults>;
