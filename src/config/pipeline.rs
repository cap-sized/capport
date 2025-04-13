use crate::config::common::Configurable;
use crate::util::error::CpResult;
use std::fs;

pub struct PipelineRegistry;

impl Configurable for PipelineRegistry {
    fn get_node_name() -> &'static str {
        "pipeline"
    }
}
