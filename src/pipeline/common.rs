use std::fmt;

use yaml_rust2::Yaml;

use crate::{
    model::common::Model,
    transform::common::{RootTransform, Transform},
    util::{
        common::yaml_from_str,
        error::{CpResult, SubResult},
    },
};

use super::{context::DefaultContext, results::PipelineResults};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pipeline {
    pub label: String,
    pub stages: Vec<PipelineStage>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipelineStage {
    pub label: String,
    pub task_name: String,
    pub args_node: Yaml, // pub task: PipelineOnceTask,
                         // pub args_yaml_str: String,
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
    pub fn new(label: &str, task_name: &str, args_node: &Yaml) -> Self {
        PipelineStage {
            label: label.to_string(),
            task_name: task_name.to_string(),
            args_node: args_node.clone(),
        }
    }
}

pub trait HasTask {
    fn task(args: &Yaml) -> CpResult<PipelineOnceTask>;
}

// Eventually we will need to make live stages which acculumate their own results over time.
// They will extend the current functionality of PipelineStage.
pub trait LoopJobStage {
    fn poll();
    fn push();
}

pub type PipelineOnceTask = Box<dyn Fn(&mut DefaultContext) -> CpResult<()>>;
