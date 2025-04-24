use std::collections::HashMap;

use capport_core::{
    context::{
        model::ModelRegistry,
        task::{TaskDictionary, generate_lazy_task},
        transform::TransformRegistry,
    },
    pipeline::context::DefaultContext,
    task::{
        loadstore::csv::{CsvModelLoadTask, CsvModelSaveTask},
        noop::NoopTask,
        requests::http::HttpRequestTask,
        transform::TransformTask,
    },
};
use polars::prelude::LazyFrame;

use crate::{services::common::DefaultSvcDistributor, util::common::SvcDefault};

impl SvcDefault for TaskDictionary<LazyFrame, DefaultSvcDistributor> {
    fn default_with_svc() -> Self {
        Self {
            tasks: HashMap::from([
                (
                    "noop".to_string(),
                    generate_lazy_task::<NoopTask, DefaultSvcDistributor>(),
                ),
                (
                    "load_csv".to_string(),
                    generate_lazy_task::<CsvModelLoadTask, DefaultSvcDistributor>(),
                ),
                (
                    "save_csv".to_string(),
                    generate_lazy_task::<CsvModelSaveTask, DefaultSvcDistributor>(),
                ),
                (
                    "transform".to_string(),
                    generate_lazy_task::<TransformTask, DefaultSvcDistributor>(),
                ),
                (
                    "http_request".to_string(),
                    generate_lazy_task::<HttpRequestTask, DefaultSvcDistributor>(),
                ),
            ]),
        }
    }

    // TODO:
    fn default_with_svc_config(_args: &yaml_rust2::Yaml) -> Self {
        Self::default_with_svc()
    }
}

impl SvcDefault for DefaultContext<LazyFrame, DefaultSvcDistributor> {
    fn default_with_svc() -> Self {
        Self::new(
            ModelRegistry::new(),
            TransformRegistry::new(),
            TaskDictionary::default_with_svc(),
            DefaultSvcDistributor::new(),
        )
    }

    // TODO:
    fn default_with_svc_config(_args: &yaml_rust2::Yaml) -> Self {
        Self::default_with_svc()
    }
}
