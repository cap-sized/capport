use crate::config::common::Configurable;
use crate::util::error::CpResult;
use std::collections::HashMap;
use std::fs;

pub struct TransformRegistry;

impl Configurable for TransformRegistry {
    fn get_node_name() -> &'static str {
        "transform"
    }
    fn extract_parse_config(
        &mut self,
        config_pack: &mut std::collections::HashMap<String, HashMap<String, yaml_rust2::Yaml>>,
    ) -> CpResult<()> {
        Ok(())
    }
}
