use crate::{
    context::transform::TransformRegistry,
    pipeline::common::{PipelineOnceTask, HasTask},
    transform::common::RootTransform,
};

pub struct TransformTask {
    pub name: String,
    pub input: String, // input df
    pub save_df: String,
}
