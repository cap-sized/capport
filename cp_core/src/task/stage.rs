use std::{collections::HashMap, sync::Arc};

use serde::Deserialize;

use crate::{pipeline::context::DefaultPipelineContext, util::error::CpResult};

pub trait Stage {
    /// Executes the default pipeline context with subtransforms executed in linear order.
    fn linear(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()>;

    /// Executes the default pipeline context with concurrent subtransforms.
    fn sync_exec(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<()>;

    /// Runs a polling loop while not killed. Since there are only 3 types of subtransforms
    /// we will allow the async_fn_in_trait. Returns the number of iterations made.
    #[allow(async_fn_in_trait)]
    async fn async_exec(&self, ctx: Arc<DefaultPipelineContext>) -> CpResult<u64>;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct StageConfig {
    pub label: String,
    pub task: String,
    pub emplace: HashMap<String, serde_yaml_ng::Value>,
}
