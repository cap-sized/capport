use crate::{
    context::{model::ModelRegistry, pipeline::PipelineRegistry, transform::TransformRegistry},
    util::error::{CpError, CpResult},
};

use super::{common::Pipeline, results::PipelineResults};

pub struct Context {
    model_registry: ModelRegistry,
    transform_registry: TransformRegistry,
    results: PipelineResults,
}

impl Context {
    pub fn new(model_registry: ModelRegistry, transform_registry: TransformRegistry) -> Self {
        Context {
            model_registry,
            transform_registry,
            results: PipelineResults::new(),
        }
    }
    pub fn clone_results(&self) -> PipelineResults {
        self.results.clone()
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new(ModelRegistry::new(), TransformRegistry::new())
    }
}
