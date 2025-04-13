use crate::config::common::Configurable;
use crate::util::error::CpResult;
use std::fs;

pub struct TransformRegistry;

impl Configurable for TransformRegistry {
    fn get_node_name() -> &'static str {
        "transform"
    }
}
