# Services

### Todo summary

- [x] MongoDB Service
    - [ ] `find` task
    - [ ] `bulk_write` task
- [ ] SQL connection Service
    - [ ] `create_table` task
    - [ ] `insert` task
    - [ ] `insert_batched` task (from lazyframe)
    - [ ] `select` task
    - [ ] `execute` task (takes in a SQL string query and runs it directly on the database)

## Overview

A `ServiceDistributor` in `PipelineContext` is a custom implemented struct that contains and handles the 
Rust clients for external services, like database connections, gRPC clients etc.

This design allows us to attach an arbitrary number of services to a distributor.

### `DefaultSvcDistributor`

Acts as the default registry provided by capport, implements `Configurable`.
It will include the ability to hold a single instance of every single client that service
provides. However it only instantiates those the `required_svcs` that are passed to the 
`setup` method.

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
    fn lazy_task<SvcDistributor: HasMongoClient>(args: &serde_yaml_ng::Value) -> CpResult<PipelineTask<LazyFrame, SvcDistributor>> {
        ...
        Ok(|ctx, task| run_find<SvcDistributor>(ctx, task))
    }
}
```

## Services to support

- MongoDB `MongoClient`
- SQLx `SQLClient`