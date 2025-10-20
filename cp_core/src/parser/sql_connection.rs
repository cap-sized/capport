use connectorx::prelude::CXQuery;
use polars::prelude::{Expr, Schema};
use serde::Deserialize;

use crate::{
    db_url_emplace,
    model::common::{ModelConfig, ModelFields},
    model_emplace,
    parser::keyword::Keyword,
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    util::error::CpResult,
};

use super::{keyword::StrKeyword, merge_type::MergeTypeEnum};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SqlConnection {
    pub merge_type: Option<MergeTypeEnum>,
}

impl SqlConnection {
    pub fn emplace(
        &mut self,
        ctx: &DefaultPipelineContext,
        context: &serde_yaml_ng::Mapping,
        url_prefix: &str,
    ) -> CpResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
}
