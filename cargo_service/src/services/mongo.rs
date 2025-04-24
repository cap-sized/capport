use std::sync::Arc;

use bson::Bson;
use capport_core::{
    pipeline::{
        common::{HasTask, PipelineTask},
        context::PipelineContext,
    },
    task::common::{deserialize_arg_str, yaml_to_task_arg_str},
    util::error::CpResult,
};
use mongodb::bson::Document;
use polars::{
    io::SerReader,
    prelude::{IntoLazy, JsonReader, LazyFrame},
};
use serde::{Deserialize, Serialize};

use crate::services::common::JsonTranscodable;

use super::error::{CpSvcError, CpSvcResult};

#[derive(Clone)]
pub struct MongoClient {
    pub uri: String,
    pub client: Option<mongodb::Client>,
    pub sync_client: Option<mongodb::sync::Client>,
    pub default_db: String,
}
pub trait HasMongoClient {
    fn get_mongo_client(&self, name: Option<&str>) -> Option<MongoClient>;
}

impl MongoClient {
    pub async fn new(uri: &str, default_db: &str) -> CpSvcResult<MongoClient> {
        Ok(MongoClient {
            uri: uri.to_owned(),
            client: Some(mongodb::Client::with_uri_str(uri).await?),
            sync_client: Some(mongodb::sync::Client::with_uri_str(uri)?),
            default_db: default_db.to_owned(),
        })
    }
    pub fn new_sync(uri: &str, default_db: &str) -> CpSvcResult<MongoClient> {
        Ok(MongoClient {
            uri: uri.to_owned(),
            client: None,
            sync_client: Some(mongodb::sync::Client::with_uri_str(uri)?),
            default_db: default_db.to_owned(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MongoFindTask {
    pub db: Option<String>,
    pub collection: String,
    pub find: String, // JSON string
    // TODO: implement these
    pub sort: Option<String>, // JSON string
    pub limit: Option<usize>,
    pub df_name: String,
}

pub fn json_str_to_doc(json_str: &str) -> CpSvcResult<bson::Document> {
    // https://docs.rs/bson/2.14.0/bson/document/struct.Document.html#method.from_reader
    let json_val: serde_json::Value = serde_json::from_str(json_str)?;
    let trans_bson: Bson = json_val.try_into()?;
    match trans_bson.as_document() {
        Some(x) => Ok(x.to_owned()),
        None => Err(CpSvcError::JsonToBsonError(format!(
            "Json returned no bson: {}",
            json_str
        ))),
    }
}

pub fn doc_cursor_to_df(cursor: mongodb::sync::Cursor<Document>) -> CpResult<polars::frame::DataFrame> {
    let inner = cursor
        .map(|document| match document {
            Ok(d) => {
                let deser: serde_json::Value = bson::Bson::Document(d).clone().into_json();
                deser.to_string()
            }
            Err(e) => panic!("{:?}", e),
        })
        .collect::<Vec<String>>()
        .join(", ");

    let json_to_parse = format!("[{}]", &inner.as_str());
    // https://stackoverflow.com/questions/51982893/is-there-a-better-way-to-directly-convert-a-rust-bson-document-to-json

    let restr = JsonReader::new(std::io::Cursor::new(json_to_parse));
    let df = restr.finish()?;
    Ok(df)
}

fn find_in_collection<S: HasMongoClient>(
    ctx: Arc<dyn PipelineContext<LazyFrame, S>>,
    task: &MongoFindTask,
) -> CpSvcResult<mongodb::sync::Cursor<Document>> {
    let filter = json_str_to_doc(&task.find)?;
    let mc = match ctx.svc().get_mongo_client(None) {
        Some(client) => client,
        None => return Err(CpSvcError::MongoError("MongoClient not initialized".to_string()).into()),
    };
    let cur: mongodb::sync::Cursor<Document> = match &mc.client {
        Some(async_client) => {
            let db = task.db.clone().unwrap_or(mc.default_db.clone());
            let collection: mongodb::Collection<Document> = async_client.database(&db).collection(&task.collection);
            let cur: mongodb::action::Find<'_, Document> = collection.find(filter);
            cur.run()?
        }
        None => match &mc.sync_client {
            Some(sync_client) => {
                let db = task.db.clone().unwrap_or(mc.default_db.clone());
                let collection: mongodb::sync::Collection<Document> =
                    sync_client.database(&db).collection(&task.collection);
                let cur: mongodb::action::Find<'_, Document> = collection.find(filter);
                cur.run()?
            }
            None => {
                return Err(CpSvcError::CoreError(
                    "MongoClient initialization unsuccessful".to_string(),
                ));
            }
        },
    };
    Ok(cur)
}

pub fn run_find<S: HasMongoClient>(ctx: Arc<dyn PipelineContext<LazyFrame, S>>, task: &MongoFindTask) -> CpResult<()> {
    let res = find_in_collection(ctx.clone(), task)?;
    let df = doc_cursor_to_df(res)?;
    let lf = df.lazy();
    ctx.insert_result(&task.df_name, lf)?;
    Ok(())
}

impl<S: HasMongoClient> HasTask<S> for MongoFindTask {
    fn lazy_task(args: &yaml_rust2::Yaml) -> CpResult<PipelineTask<LazyFrame, S>> {
        let arg_str = yaml_to_task_arg_str(args, "MongoFindTask")?;
        let task: MongoFindTask = deserialize_arg_str(&arg_str, "MongoFindTask")?;
        Ok(Box::new(move |ctx| run_find(ctx, &task)))
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use crate::services::mongo::json_str_to_doc;

    #[test]
    fn valid_find() {
        let doc = doc! { "x" : 1, "y": 86, "j": "k", "ok": ["c", "u", "h"] };
        let json_str = "{ \"x\" : 1, \"y\": 86, \"j\": \"k\", \"ok\": [\"c\", \"u\", \"h\"] }";
        let trans_doc = json_str_to_doc(json_str).unwrap();
        assert_eq!(doc, trans_doc);
    }

    #[test]
    fn valid_connection() {
        // let uri = "mongodb+srv://capsizedhockey:QMQphdQSkSvayFXQ@capsized-data.krs2e.mongodb.net/?retryWrites=true&w=majority&appName=capsized-data";
        // let mongoc = MongoClient::new_sync(uri, "csdb").unwrap();
        //         let ctx = Arc::new(DefaultContext::default_with_svc(svcd));
        //         let config = "
        // ---
        // db: csdb
        // collection: seasons
        // find: \"{ }\"
        // df_name: SEASONS
        // ";
        //         let args = yaml_from_str(&config).unwrap();
        //         let t = MongoFindTask::lazy_task(&args).unwrap();
        //         t(ctx.clone()).unwrap();
        //         let actual = ctx.clone_result("SEASONS").unwrap();
        //         println!("{}", actual.collect().unwrap());
        //         assert!(false);
        // run_find(ctx, task)
    }
}
