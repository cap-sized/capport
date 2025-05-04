use std::{collections::HashMap, sync::Arc};

use bson::doc;
use capport_core::{
    context::common::Configurable,
    parser::config::pack_configurables,
    pipeline::{
        common::HasTask,
        context::{DefaultContext, PipelineContext},
    },
    util::common::yaml_from_str,
};
use capport_service::{
    context::service::{DefaultSvcConfig, DefaultSvcDistributor},
    service::mongo::{HasMongoClient, MongoClientConfig},
    task::mongo::MongoFindTask,
    util::common::SvcDefault,
};
use polars::prelude::LazyFrame;
use serde::{Deserialize, Serialize};

const MONGO_URI: &str = "mongodb://localhost:27017";
const MONGO_DEFAULT_DB: &str = "csdb";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TestPerson {
    name: String,
    id: u8,
    desc: String,
}

#[test]
fn valid_default_config_mongo() {
    let mut config_pack = HashMap::new();
    let yaml_root = yaml_from_str(
        format!(
            "
service: 
    mongo:
        uri: {}
        default_db: {}
# pipeline:
#     mongo_test:
#         - label: basic_find_op
#           task: mongo_find
#           args:
#             collection: persons
#             df_name: PEOPLE
#             query: 
#                 name: foo
    ",
            MONGO_URI, MONGO_DEFAULT_DB
        )
        .as_ref(),
    )
    .unwrap();
    pack_configurables(&mut config_pack, yaml_root).unwrap();
    let mut ctx_base: DefaultContext<LazyFrame, DefaultSvcDistributor> = DefaultContext::default_with_svc();
    ctx_base.mut_svc().extract_parse_config(&mut config_pack).unwrap();
    ctx_base.mut_svc().setup(&["mongo"]).unwrap();
    let ctx = Arc::new(ctx_base);
    let db = ctx.svc().get_mongo_syncdb(Some(MONGO_DEFAULT_DB)).unwrap();
    db.collection("persons")
        .insert_many([
            doc! { "name": "foo", "id": 1, "desc": "bar" },
            doc! { "name": "dee", "id": 2, "desc": "bee" },
        ])
        .run()
        .unwrap();
    {
        let find_task_cfg = yaml_from_str(
            "
collection: persons
df_name: PEOPLE
query:
    name: foo
        ",
        )
        .unwrap();
        let t = MongoFindTask::lazy_task(&find_task_cfg).unwrap();
        t(ctx.clone()).unwrap();
        let actual = ctx.clone_result("PEOPLE").unwrap().collect().unwrap();
        println!("{:?}", actual);
    }
    let cursor = ctx
        .svc()
        .get_mongo_client(None)
        .as_ref()
        .unwrap()
        .sync_client
        .as_ref()
        .unwrap()
        .database(MONGO_DEFAULT_DB)
        .collection::<TestPerson>("persons")
        .find(doc! {})
        .run()
        .unwrap();
    let actual = cursor.map(|x| x.unwrap()).collect::<Vec<_>>();
    let expected = vec![
        TestPerson {
            name: "foo".to_owned(),
            id: 1,
            desc: "bar".to_owned(),
        },
        TestPerson {
            name: "dee".to_owned(),
            id: 2,
            desc: "bee".to_owned(),
        },
    ];
    assert_eq!(&actual, &expected);
    {
        let mut svc = DefaultSvcDistributor {
            config: DefaultSvcConfig {
                mongo: Some(MongoClientConfig::new(MONGO_URI, MONGO_DEFAULT_DB)),
            },
            mongo_client: None,
        };

        svc.setup(&["mongo"]).unwrap();

        let db = svc.get_mongo_syncdb(None).unwrap();
        let cursor = db.collection::<TestPerson>("persons").find(doc! {}).run().unwrap();
        let actual = cursor.map(|x| x.unwrap()).collect::<Vec<_>>();
        assert_eq!(&actual, &expected);

        let collection = db.collection::<mongodb::bson::Document>("persons");
        collection.drop().run().unwrap();
        let final_cursor = db.collection::<TestPerson>("persons").find(doc! {}).run().unwrap();
        let final_actual = final_cursor.map(|x| x.unwrap()).collect::<Vec<_>>();
        assert!(&final_actual.is_empty());
    }
}
