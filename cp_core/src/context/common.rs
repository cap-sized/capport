use std::collections::HashMap;

use crate::util::error::CpResult;

pub trait Configurable {
    fn get_node_name() -> &'static str;
    fn extract_parse_config(
        &mut self,
        config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>,
    ) -> CpResult<()>;
}
