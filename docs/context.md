# Context

### Todo summary

- [ ] Parse results config
- [ ] Design connection to on-disk store for pipeline results, should be optional
  - Crash if out of memory and no on-disk store configured
  - Otherwise should transparently connect to on-disk store.

## Overview

The pipeline's context consists of all the reusable information a stage needs for its execution.
Different pipelines require different contexts, so `PipelineContext` is an interface rather than a
concrete implementation.

PipelineContexts are generic across `ResultType` (which is the datatype that `PipelineResults` holds)
and `ServiceDistributor` (which is the implementation of the provider of service clients to the individual
tasks).

Currently the default context includes a

- `ModelRegistry`
- `TransformRegistry`
- `TaskDictionary`
- `PipelineResults`
- any Service Distributor (see service.md for details)

Tasks can use this interface with the `Arc<PipelineContext<R,S>>` provided to them as the first argument.

```rs

pub trait PipelineContext<ResultType, ServiceDistributor> {

    // Clones (deep copies) the entire key-value store of results in PipelineResults currently
    fn clone_results(&self) -> CpResult<PipelineResults<ResultType>>;

    // Clones (deep copies) a single named result from PipelineResults currently
    fn clone_result(&self, key: &str) -> CpResult<ResultType>;

    // Returns a lock-wrapped handler to the actual list of results
    fn get_results(&self) -> Arc<RwLock<PipelineResults<ResultType>>>;

    // Inserts a keyed value into the PipelineResults
    fn insert_result(&self, key: &str, result: ResultType) -> CpResult<Option<ResultType>>;

    // Fetch a model from model registry
    fn get_model(&self, key: &str) -> CpResult<Model>;

    // Fetch a task from the task dictionary
    fn get_task(&self, key: &str, args: &serde_yaml_ng::Value) -> CpResult<PipelineTask<ResultType, ServiceDistributor>>;

    // Fetch a transform from transform registry
    fn get_transform(&self, key: &str) -> CpResult<&RootTransform>;

    // Fetch a handler to the service distributor
    fn svc(&self) -> Arc<ServiceDistributor>;
}

```

### ModelRegistry, TransformRegistry

Simply a key-value store of models/transforms. Follows the same pattern

```rs
    pub fn new() -> Registry<T>;
    pub fn insert(&mut self, model: T) -> Option<T>;
    pub fn from(config_pack: &mut HashMap<String, HashMap<String, serde_yaml_ng::Value>>) -> CpResult<Registry<T>> {
        let mut reg = Registry<T>::new();
        reg.extract_parse_config(config_pack)?;
        Ok(reg)
    }
    pub fn get_model(&self, model_name: &str) -> Option<T>;
```

### TaskDictionary

These are also registries of tasks, but do not use the same pattern as `TaskDictionary` requires the 
template parameters `ResultType` and `ServiceDistributor` to be fully defined. Hence every unique pipeline 
`<ResultType, SvcDistributor>` combination will require their own `TaskDictionary<ResultType, SvcDistributor>`.

In this project, the helper function `generate_lazy_task<TaskType: HasTask, SvcDistributor>` allows us to 
create synchronous lazy tasks to be added to the task dictionary as can be seen in the example below

```rs

pub fn generate_lazy_task<T: HasTask, S>() -> TaskGenerator<LazyFrame, S> {
    |yaml| T::lazy_task::<S>(yaml)
}

impl Default for TaskDictionary<LazyFrame, ()> {
    fn default() -> Self {
        Self {
            tasks: HashMap::from([
                ("noop".to_string(), generate_lazy_task::<NoopTask, ()>()),
                ("load_csv".to_string(), generate_lazy_task::<CsvModelLoadTask, ()>()),
                ("save_csv".to_string(), generate_lazy_task::<CsvModelSaveTask, ()>()),
                ("transform".to_string(), generate_lazy_task::<TransformTask, ()>()),
                ("http_request".to_string(), generate_lazy_task::<HttpRequestTask, ()>()),
            ]),
        }
    }
}
```

### PipelineResults

This is an in-memory managed store of the current results. It is simply a key-value store of name to DataFrame
or LazyFrame. Not threadsafe on its own.

Implements the following

```rs
impl<T: Clone> PipelineResults<T> {
    pub fn get_unchecked(&self, key: &str) -> Option<T>;
    pub fn insert(&mut self, key: &str, lf: T) -> Option<T>;
    pub fn clone_all(&self) -> HashMap<String, T>;
}
```

TODO: Dataframes can get way too huge for memory. We will need an option to connect to a local 
on-disk store.


## Config examples

- [puckdata, players](https://github.com/cap-sized/capport/blob/main/config/example/pipeline.yml)
- [mass_load_player](https://github.com/cap-sized/capport/blob/main/config/pipeline.yml)