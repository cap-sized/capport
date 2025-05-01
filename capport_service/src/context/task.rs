use std::collections::HashMap;

use capport_core::{
    context::task::{TaskDictionary, generate_lazy_task},
    task::{
        loadstore::csv::{CsvModelLoadTask, CsvModelSaveTask},
        noop::NoopTask,
        requests::http::HttpBatchRequestTask,
        transform::TransformTask,
    },
};
use polars::prelude::LazyFrame;

use crate::{context::service::DefaultSvcDistributor, util::common::SvcDefault};

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
                    generate_lazy_task::<HttpBatchRequestTask, DefaultSvcDistributor>(),
                ),
            ]),
        }
    }
}
