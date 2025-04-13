use crate::config::common::Configurable;
use crate::util::error::CpResult;
use std::fs;

pub struct ModelRegistry;

impl Configurable for ModelRegistry {
    fn get_node_name() -> &'static str {
        "model"
    }
}
