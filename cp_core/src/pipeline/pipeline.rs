use serde::Deserialize;

use crate::task::stage::StageConfig;


#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PipelineConfig {
    pub label: String,
    pub stages: Vec<StageConfig>
}
