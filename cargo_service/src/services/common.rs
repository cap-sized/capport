use super::mongo::{HasMongoClient, MongoClient};

pub struct DefaultSvcDistributor {
    pub mongo: Option<MongoClient>,
}

impl HasMongoClient for DefaultSvcDistributor {
    fn get_mongo_client(&self, _name: Option<&str>) -> Option<MongoClient> {
        self.mongo.clone()
    }
}
