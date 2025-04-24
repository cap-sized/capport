use std::sync::Arc;

use polars::prelude::LazyFrame;
use yaml_rust2::Yaml;

use crate::util::error::CpResult;

use super::context::PipelineContext;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pipeline {
    pub label: String,
    pub stages: Vec<PipelineStage>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipelineStage {
    pub label: String,
    pub task_name: String,
    pub args_node: Yaml,
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

pub trait HasTask<SvcDistributor> {
    fn lazy_task(args: &Yaml) -> CpResult<PipelineTask<LazyFrame, SvcDistributor>>;
}

pub type PipelineTask<ResultType, SvcDistributor> =
    Box<dyn Fn(Arc<dyn PipelineContext<ResultType, SvcDistributor>>) -> CpResult<()>>;
