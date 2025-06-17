use std::sync::Arc;

use polars::prelude::{Expr, LazyFrame};

use crate::{pipeline::context::DefaultPipelineContext, util::error::CpResult};

use super::common::Source;

/// add this back to sink/mod.rs when ready
pub struct MongoSource {
    uri: String,
    find: mongodb::bson::Document,
    projection: mongodb::bson::Document,
    output: String,
    strict: bool,
    columns: Option<Vec<Expr>>,
}
