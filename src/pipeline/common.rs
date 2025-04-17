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
    pub task: PipelineOnceTask,
    pub args_yaml_str: String,
}

impl Pipeline {
    pub fn new(label: &str, stages: &[PipelineStage]) -> Self {
        Pipeline {
            label: label.to_string(),
            stages: stages.to_vec(),
        }
    }
}

impl PipelineStage {
    pub fn new(label: &str, task: PipelineOnceTask, args_yaml_str: &str) -> Self {
        PipelineStage {
            label: label.to_string(),
            task,
            args_yaml_str: args_yaml_str.to_string(),
        }
    }
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

pub trait HasTask {
    fn task(&self) -> SubResult<PipelineOnceTask>;
}

// Eventually we will need to make live stages which acculumate their own results over time.
// They will extend the current functionality of PipelineStage.
pub trait LoopJobStage {
    fn poll();
    fn push();
}

pub type PipelineOnceTask = fn(&Context, &str) -> CpResult<()>;
