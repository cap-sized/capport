use std::sync::Arc;

use capport_core::{
    pipeline::{
        common::{HasTask, PipelineTask},
        context::PipelineContext,
    },
    util::error::{CpError, CpResult},
};
use polars::prelude::{IntoLazy, LazyFrame};
use polars_bson::BsonReader;
use serde::{Deserialize, Serialize};

use crate::service::mongo::HasMongoClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MongoFindTask {
    pub database: Option<String>,
    pub collection: String,
    pub df_name: String,
    pub query: serde_yaml_ng::Value,
}

impl MongoFindTask {
    pub fn new(database: Option<&str>, collection: &str, df_name: &str, query: serde_yaml_ng::Value) -> Self {
        Self {
            database: database.map(|x| x.to_owned()),
            collection: collection.to_owned(),
            df_name: df_name.to_owned(),
            query,
        }
    }
}

pub fn run_find<S>(ctx: Arc<dyn PipelineContext<LazyFrame, S>>, mongo_task: &MongoFindTask) -> CpResult<()>
where
    S: HasMongoClient,
{
    let dbname = mongo_task.database.as_deref();
    let db = match ctx.svc().get_mongo_syncdb(dbname) {
        Some(x) => x,
        None => {
            return Err(CpError::TaskError(
                "Database not found",
                format!("Database `{}` not found in connection", dbname.unwrap_or("<default>")),
            ));
        }
    };
    let collection = db.collection(&mongo_task.collection);
    let find = match bson::to_document(&mongo_task.query) {
        Ok(x) => x,
        Err(e) => {
            return Err(CpError::TaskError(
                "Invalid `find` bson document",
                format!(
                    "Could not create bson document for `find` query from {:?}: {:?}",
                    &mongo_task.query, e
                ),
            ));
        }
    };
    let bson = BsonReader::new(collection, find);
    let result = bson.finish()?.lazy();
    ctx.insert_result(&mongo_task.df_name, result)?;
    Ok(())
}

impl<S: HasMongoClient> HasTask<S> for MongoFindTask {
    fn lazy_task(args: &serde_yaml_ng::Value) -> CpResult<PipelineTask<LazyFrame, S>> {
        let mongo_task: MongoFindTask = serde_yaml_ng::from_value::<MongoFindTask>(args.to_owned())?;
        Ok(Box::new(move |ctx| run_find::<S>(ctx, &mongo_task)))
    }
}
