# Data and communication

Each task reads from/writes to a `LazyFrame` (lazily computed dataframe) which is consumed by the sink subtransforms.
Nothing else is communicated between subtransforms.

To reduce the amount of dependency on the actual implementation of LazyFrame wherever possible
in the pipeline, the result type `LazyFrame` is encapsulated behind the `PipelineFrame`
interface which additionally provides communication mechanisms for tasks.

(This is so that if we want to replace the LazyFrame implementation from polars in the future to
something else so our custom implementation, we do not have to make as much of a change in the actual pipeline 
framework, though we will probably still have to rewrite every task.)

## PipelineFrame

`PipelineFrame`s of any type of frame `FrameType` can: 

1. hold a lazyframe with a reader/writer lock (using `RwLock`) on it
2. produce broadcast (sender) channels allowing subtransforms to *publish* changes
3. produce listener (receiver) channels allowing subtransforms to *receive* changes

Internally, the `broadcast` method waits for the listeners to release the reader lock 
on the internal lazyframe before writing and publishing/broadcasting the update. 

> Q: Why?

MOST IMPORTANTLY: **Transform tasks may require more than one frame to run**.
In which case, I must be able to recall/extract other frames that may not have been updated 
immediately when the main dependent is updated.

e.g. `JoinTransform`. Listen to main frame A, but want to join on auxiliary frame B. B gets
updated only once in 5 minutes, but A gets updated every 10s. The join transform must run everytime
A is updated, not every time both A and B are updated.

Other misc reasons:

- I prioritize the ability to take a global snapshot
    - much easier with a central data structure
- Don't have ultra low latency requirements
- Reader bias, but without writer starvation 
    - because readers are always fast, much faster than writers (i.e. source tasks)
    - but readers should not read when there is nothing to do

> Q: Why RwLock instead of something faster?

- give me something faster in std rust and I will consider.

> Q: What is the `df_dirty` atomic attribute?

- It is just an atomic flag to indicate if the cached dataframe is out of date (i.e. dirty)
- Currently there is no cached dataframe (it is commented out)
- Eventually will enable it, for debugging and visualizing

```rs

fn source_task(ctx: Arc<DefaultPipelineContext>) {
    let mut broadcast = ctx.get_broadcast("orig", "source").unwrap();
    let myresult: DataFrame = do_something();
    /// source_task broadcasts my_result to all listeners.
    broadcast.broadcast(myresult.lazy()).unwrap();
}

fn my_transform_task(ctx: Arc<DefaultPipelineContext>) {
    let mut listener = ctx.get_listener("orig", "my_transform_task").unwrap();
    let mut broadcast = ctx.get_broadcast("next", "my_transform_task").unwrap();
    /// my_transform_tasks listens to messages before 
    let update = listener.listen().unwrap();
    let lf = update.frame.read().unwrap();
    let myresult: LazyFrame = do_another_thing(lf);
    broadcast.broadcast(myresult.lazy()).unwrap();
}

fn main() {
    /// ...
    let ctx = Arc::new(DefaultPipelineContext::new());
    pipeline.run_once(ctx).unwrap();
}
```

## `PipelineResults`

`PipelineResults` is built at the config parsing stage.
The entres in `PipelineResults` should not be modified after its insertion into `PipelineContext`.
