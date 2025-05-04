use std::sync::Arc;

use polars::prelude::LazyFrame;
use serde::{Deserialize, Serialize};

use crate::util::error::CpResult;

use super::{context::PipelineContext, runner::RunMethodType};

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

pub trait HasTask<SvcDistributor> {
    fn lazy_task(args: &serde_yaml_ng::Value) -> CpResult<PipelineTask<LazyFrame, SvcDistributor>>;
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct RunnerConfig {
    pub label: String,
    pub logger: String,
    pub run_method: RunMethodType,
    pub schedule: Option<String>,
}

impl RunnerConfig {
    pub fn new(label: &str, logger: &str, run_method: RunMethodType, schedule: Option<&str>) -> Self {
        Self {
            label: label.to_owned(),
            logger: logger.to_owned(),
            run_method,
            schedule: schedule.map(|x| x.to_owned()),
        }
    }
}

pub type PipelineTask<ResultType, SvcDistributor> =
    Box<dyn Fn(Arc<dyn PipelineContext<ResultType, SvcDistributor>>) -> CpResult<()>>;
