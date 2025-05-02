use bson::doc;
use capport_service::{
    context::service::{DefaultSvcConfig, DefaultSvcDistributor},
    service::mongo::{HasMongoClient, MongoClient, MongoClientConfig},
};
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
    let mc_config = MongoClientConfig::new(MONGO_URI, MONGO_DEFAULT_DB);
    let mc = MongoClient::new(mc_config.clone()).unwrap();
    let msc = mc.sync_client.unwrap();
    let db = msc.database(MONGO_DEFAULT_DB);
    db.collection("persons")
        .insert_many([
            doc! { "name": "foo", "id": 1, "desc": "bar" },
            doc! { "name": "dee", "id": 2, "desc": "bee" },
        ])
        .run()
        .unwrap();
    let cursor = msc
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
                mongo: Some(mc_config.clone()),
                sql: None,
            },
            mongo_client: None,
            sql_client: None,
        };

        svc.setup(&["mongo"]).unwrap();

        let db = svc.get_db_sync(None).unwrap();
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
