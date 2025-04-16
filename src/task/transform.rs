use crate::{
    config::transform::TransformRegistry,
    pipeline::common::{PipelineTask, RunTask},
    transform::common::RootTransform,
};

pub struct TransformTask {
    pub name: String,
    pub input: String, // input df
    pub save_df: String,
}
