use std::collections::HashMap;

use capport_core::{
    context::common::Configurable,
    pipeline::context::{DefaultContext, PipelineContext},
    util::common::yaml_from_str,
};
use capport_service::{
    context::service::DefaultSvcDistributor, service::mongo::HasMongoClient, util::common::SvcDefault,
};
use polars::prelude::LazyFrame;
use yaml_rust2::Yaml;

fn get_config_pack() -> HashMap<String, HashMap<String, Yaml>> {
    let config = yaml_from_str("
---
uri: mongodb+srv://capsizedhockey:QMQphdQSkSvayFXQ@capsized-data.krs2e.mongodb.net/?retryWrites=true&w=majority&appName=capsized-data
default_db: csdb
").unwrap();
    HashMap::from([(
        DefaultSvcDistributor::get_node_name().to_owned(),
        HashMap::from([("mongo".to_owned(), config)]),
    )])
}

#[test]
fn valid_ctx() {
    let mut config_pack = get_config_pack();
    let ctx = DefaultContext::<LazyFrame, DefaultSvcDistributor>::default_with_svc();
}

#[test]
fn valid_default_config() {
    let mut config_pack = get_config_pack();
    let mut svc = DefaultSvcDistributor::new();
    svc.extract_parse_config(&mut config_pack).unwrap();
    svc.setup(&["mongo"]).unwrap();

    // get the default
    let mc = svc.get_mongo_client(None).unwrap();
    let msc = mc.sync_client.unwrap();
    let db = svc.get_db_sync(None).unwrap();
}
