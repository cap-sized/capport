use std::collections::HashMap;

use bson::doc;
use capport_core::{
    context::common::Configurable,
    pipeline::context::{DefaultContext, PipelineContext},
    util::common::yaml_from_str,
};
use capport_service::{
    context::service::DefaultSvcDistributor,
    service::mongo::{HasMongoClient, MongoClientConfig},
    util::common::SvcDefault,
};
use polars::prelude::LazyFrame;
use serde::{Deserialize, Serialize};
use yaml_rust2::Yaml;

const MONGO_URI: &str = "mongodb://localhost:27017";
const MONGO_DEFAULT_DB: &str = "csdb";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TestPerson {
    name: String,
    id: u8,
    desc: String,
}

fn get_config_pack() -> HashMap<String, HashMap<String, Yaml>> {
    let config = yaml_from_str(
        format!(
            "
---
uri: {}
default_db: {}
",
            MONGO_URI, MONGO_DEFAULT_DB
        )
        .as_str(),
    )
    .unwrap();
    HashMap::from([(
        DefaultSvcDistributor::get_node_name().to_owned(),
        HashMap::from([("mongo".to_owned(), config)]),
    )])
}

#[test]
fn valid_identical_config() {
    let mut config_pack = get_config_pack();
    let mut ctx = DefaultContext::<LazyFrame, DefaultSvcDistributor>::default_with_svc();
    let svc = ctx.mut_svc();
    svc.extract_parse_config(&mut config_pack).unwrap();
    svc.setup(&["mongo"]).unwrap();
    let mc = svc.get_mongo_client(None).unwrap();
    assert_eq!(mc.config.clone(), MongoClientConfig::new(MONGO_URI, MONGO_DEFAULT_DB));
}
