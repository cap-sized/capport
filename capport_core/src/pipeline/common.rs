use std::sync::Arc;

use polars::prelude::LazyFrame;
use serde::{Deserialize, Serialize};

use crate::util::error::CpResult;

use super::context::PipelineContext;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pipeline {
    pub label: String,
    pub stages: Vec<PipelineStage>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineStage {
    pub label: String,
    pub task: String,
    pub args: serde_yaml_ng::Value,
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
    pub fn new(label: &str, task_name: &str, args_node: &serde_yaml_ng::Value) -> Self {
        PipelineStage {
            label: label.to_string(),
            task: task_name.to_string(),
            args: args_node.clone(),
        }
    }
}

pub trait HasTask {
    fn lazy_task<SvcDistributor>(args: &serde_yaml_ng::Value) -> CpResult<PipelineTask<LazyFrame, SvcDistributor>>;
}

// Eventually we will need to make live stages which acculumate their own results over time.
// They will extend the current functionality of PipelineStage.
pub trait LoopJobStage {
    fn poll();
    fn push();
}

pub type PipelineTask<ResultType, SvcDistributor> =
    Box<dyn Fn(Arc<dyn PipelineContext<ResultType, SvcDistributor>>) -> CpResult<()>>;
