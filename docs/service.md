# Services

### Todo summary

- [ ] MongoDB Service
    - [ ] `find` task
- [ ] SQL connection Service
    - [ ] `create_table` task
    - [ ] `insert` task
    - [ ] `insert_batched` task (from lazyframe)
    - [ ] `select` task
    - [ ] `execute` task (takes in a SQL string query and runs it directly on the database)
- [ ] Websockets connection Service

## Overview

A `ServiceDistributor` in `PipelineContext` is a custom implemented struct that contains and handles the 
Rust clients for external services, like database connections, gRPC clients etc.

This design allows us to attach an arbitrary number of services to a distributor.

### Service Paradigm

Tasks that require services should require the service distributor to implement their own trait. Example:

```rs

pub trait HasMongoClient {
    fn get_mongo_client(&self, name: Option<&str>) -> Option<MongoClient>;
}

pub fn run_find<S: HasMongoClient>(ctx: Arc<dyn PipelineContext<LazyFrame, S>>, task: &MongoFindTask) -> CpResult<()> {
    ...
}

impl HasTask for MongoFindTask {
    fn lazy_task<SvcDistributor: HasMongoClient>(args: &Yaml) -> CpResult<PipelineTask<LazyFrame, SvcDistributor>> {
        ...
        Ok(|ctx, task| run_find<SvcDistributor>(ctx, task))
    }
}
```
