use serde::Deserialize;

use crate::{
    pipeline::context::{DefaultPipelineContext},
    util::error::CpResult,
};

use super::{merge_type::MergeTypeEnum};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SqlConnection {
    pub merge_type: Option<MergeTypeEnum>,
}

impl SqlConnection {
    pub fn emplace(
        &mut self,
        _ctx: &DefaultPipelineContext,
        _context: &serde_yaml_ng::Mapping,
        _url_prefix: &str,
    ) -> CpResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
}
