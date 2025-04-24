use capport_core::{
    context::{model::ModelRegistry, task::TaskDictionary, transform::TransformRegistry},
    pipeline::context::DefaultContext,
};
use polars::prelude::LazyFrame;

use crate::{context::service::DefaultSvcDistributor, util::common::SvcDefault};

impl SvcDefault for DefaultContext<LazyFrame, DefaultSvcDistributor> {
    fn default_with_svc() -> Self {
        Self::new(
            ModelRegistry::new(),
            TransformRegistry::new(),
            TaskDictionary::default_with_svc(),
            DefaultSvcDistributor::new(),
        )
    }
}
