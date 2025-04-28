# Pipeline

### Todo summary

- [ ] Implement `run_eager`
- [ ] Implement async pipeline validator
- [ ] Implement async pipeline executor
- [ ] Implement `async_lazy`
- [ ] Implement `async_eager`
- [ ] Parse runner config
- [ ] Design `PipelineScheduler`

## Overview

A **pipeline** consists of a list of stages to be executed, in a dictionary keyed by the pipeline name.

A pipeline **stage** consist of its *label*, the *task* it executes and the *arguments* to be passed into 
the task.

This is the structure of a stage:

```yml
- label: XXX
  task: method
  args:
    - listitemA
    - listitemB
  # or: 
  # args: { itemA: valA, itemB: valB }
```

A pipeline **task** is a single unit of action in a pipeline. You can think of them as methods
that your configured arguments are passed into.

### Example

- [config/pipeline.yml](https://github.com/cap-sized/capport/blob/main/config/pipeline.yml)

## Pipeline Execution

`PipelineRunner` is the default pipeline executor that implements the pipeline execution methods.

Currently there is only one mode of execution: naively running the pipeline stages in the configured list
order synchronously, with the results as LazyFrames: via 

-  `run_lazy` (synchronous, LazyFrames, linear execution)

TODO: Implement

-  `run_eager` (synchronous, DataFrames, linear execution)
-  `async_eager` (asynchronous, DataFrames, execute on dependency fulfilment)
-  `async_lazy` (asynchronous, LazyFrames, execute on dependency fulfilment)

### TODO: Dependencies

Currently there are no stages that has explicit dependencies (in fact it's not a recognized key in a stage).

But they can be deduced based on the logic of the stage's task. 
e.g. In the following config, `prune_to_b` depends on `enrich_a`.

```yml
    - label: enrich_a
      task: transform
      args: 
        name: enrich_with_details
        input: TABLE_A
        save_df: TABLE_A_plus

    - label: prune_to_b
      task: transform
      args: 
        name: select_criteria
        input: TABLE_A_plus # this is produced by `enrich_a`
        save_df: TABLE_FINAL
```

Obviously dependencies can get complicated and are very much task dependent, especially since transforms 
can have join operations. 

But if a *dependency graph* can be formed, we can validate the pipeline for missing dependencies,
and run the pipeline in async (where tasks wait for their dependencies to complete in order to unblock and
execute).

This also means we need a pipeline validator for this execution

#### TODO: Scheduler

When this is more mature, we should set up a scheduler that allows the pipeline runner to persist and 
execute runs according to a schedule.

Example proposed config:

```yml
runner:
  logger: default
  run_with: async_eager # or async_lazy or sync_eager or sync_lazy
  schedule:
    timezone: UTC
    # 1d or 1w or 10s or 2M (month) or 1Y or 10m30s
    every: Monday at 15:00 # internally parse to .every(Monday).at("15:00")
```

Can use [clockwerk](https://docs.rs/clokwerk/latest/clokwerk/index.html) for this.

The correct pipeline should be passed in via command line args.