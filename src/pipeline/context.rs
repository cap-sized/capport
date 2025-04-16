use crate::config::{model::ModelRegistry, pipeline::PipelineRegistry, transform::TransformRegistry};

pub struct Context {
    model_registry: ModelRegistry,
    transform_registry: TransformRegistry,
    pipeline_registry: PipelineRegistry,
}

impl Context {
    pub fn new(
        model_registry: ModelRegistry,
        transform_registry: TransformRegistry,
        pipeline_registry: PipelineRegistry,
    ) -> Self {
        Context {
            model_registry,
            transform_registry,
            pipeline_registry,
        }
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new(ModelRegistry::new(), TransformRegistry::new(), PipelineRegistry::new())
    }
}
