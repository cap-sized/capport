use std::sync::Arc;

use bson::Bson;
use capport_core::pipeline::context::PipelineContext;
use mongodb::bson::Document;
use polars::{
    io::SerReader,
    prelude::{IntoLazy, JsonReader, LazyFrame},
};
use serde::{Deserialize, Serialize};

use crate::services::common::JsonTranscodable;

use super::error::{CpError, CpResult};

#[derive(Clone)]
pub struct MongoClient {
    pub uri: String,
    pub client: mongodb::Client,
    pub default_db: String,
}

pub trait HasMongoClient {
    fn get_mongo_client(&self, name: Option<&str>) -> Option<MongoClient>;
}

impl MongoClient {
    pub async fn new(uri: &str, default_db: &str) -> CpResult<MongoClient> {
        let client = mongodb::Client::with_uri_str(uri).await?;
        Ok(MongoClient {
            uri: uri.to_owned(),
            client,
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

pub fn json_str_to_doc(json_str: &str) -> CpResult<bson::Document> {
    // https://docs.rs/bson/2.14.0/bson/document/struct.Document.html#method.from_reader
    let json_val: serde_json::Value = serde_json::from_str(json_str)?;
    let trans_bson: Bson = json_val.try_into()?;
    match trans_bson.as_document() {
        Some(x) => Ok(x.to_owned()),
        None => Err(CpError::JsonToBsonError(format!("Json returned no bson: {}", json_str))),
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

    println!("{}", &inner);
    let restr = JsonReader::new(std::io::Cursor::new(json_to_parse));
    let df = restr.finish()?;
    Ok(df)
}

pub fn run_find<S: HasMongoClient>(ctx: Arc<dyn PipelineContext<LazyFrame, S>>, task: &MongoFindTask) -> CpResult<()> {
    let mc = match ctx.svc().get_mongo_client(None) {
        Some(client) => client,
        None => return Err(CpError::MongoError("MongoClient not initialized".to_string())),
    };
    let db = task.db.clone().unwrap_or(mc.default_db.clone());
    let filter = json_str_to_doc(&task.find)?;

    let collection: mongodb::Collection<Document> = mc.client.database(&db).collection(&task.collection);
    let cur: mongodb::action::Find<'_, Document> = collection.find(filter);
    let res: mongodb::sync::Cursor<Document> = cur.run()?;
    let df = doc_cursor_to_df(res)?;
    let lf = df.lazy();
    ctx.insert_result(&task.df_name, lf)?;
    Ok(())
}

// impl HasTask for MongoFindTask {
//     fn lazy_task<SvcDistributor>(args: &Yaml) -> CpResult<PipelineTask<LazyFrame, SvcDistributor>> {
//         // let arg_str = yaml_to_task_arg_str(args, "MongoFindTask")?;
//     }
// }

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
}
