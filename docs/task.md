# Task

### Todo summary

- [ ] Task traits
    - [ ] rename `HasTask` to `HasSyncLazyTask`
    - [ ] `HasAsyncLazyTask`
    - [ ] `HasSyncEagerTask`
    - [ ] `HasAsyncEagerTask`
- [ ] HttpRequestTask enhancements
    - [ ] Convert existing `HttpRequestTask` to `HttpBatchRequestTask` 
    - [ ] Create`HttpSingleRequestTask`

## Overview

A task attaches a method to its context and returns it as a closure for execution.
Tasks are stored in a dictionary `TaskDictionary<R,S>` and must all share the same context 
type as input `PipelineContext<R,S>`.

Currently `HasTask` only requires the method `lazy_task`.

TODO: rename `HasTask` to `HasSyncLazyTask`.

TODO: add interfaces for `HasSyncEagerTask`, `HasAsyncLazyTask`, `HasAsyncEagerTask`

TODO: Get rid of loop job stage

## Types of tasks

### Service independent tasks

1. `TransformTask`
2. `CsvModelLoadTask`
3. `CsvModelStoreTask`

#### TransformTask

A simple wrapper around transforms. Example args:

```yml
---
args:
    # name of the transform
    name: player_data_to_full_name_id
    # load from PipelineResults[key] 
    input: PLAYER_DATA
    # save to PipelineResults[key] 
    save_df: ID_NAME_MAP
    # TODO: variable names to replace
    kwargs: 
        state_province_table_name: STATE_PROVINCE
```

#### CsvModelLoadTask

Load one or more csvs, optional reshape them to their models after loading. Example args:

```yml
---
args:
    # source filepath
    - filepath: /absolute/filepath/A.csv
    # df to save to
      df_name: MAP_A
    # optional model to reshape with after loading
      model: modelA
    - filepath: /absolute/filepath/B.csv
      df_name: MAP_B # model is optional
```

#### CsvModelLoadTask

Save one or more csvs, optional reshape them to their models before saving. Example args:

```yml
---
args:
    # destination filepath
    - filepath: /absolute/filepath/A.csv
    # df to load from
      df_name: MAP_A
    # optional model to reshape with before saving
      model: modelA
    - filepath: /absolute/filepath/B.csv
      df_name: MAP_B # model is optional
```

### Request based tasks

1. `HttpRequestTask`
    - `HttpBatchRequestTask`
    - `HttpSingleRequestTask`

#### HttpRequestTask

Currently only supports `GET` request methods and `JSON` format types.

Also will attempts to concurrently request for all endpoints without attempting to batch them or retry 
on timeout. These things should be configurable.

Also is currently missing a way to convert a list of ids into a list of query parameters on a single endpoint.
Currently the RequestTask is only able to make many separate requests for them.

TODO: 

- Split HttpRequestTask into `HttpBatchRequestTask` and `HttpSingleRequestTask`
    - collapse parameters into one http request
    - batch url request fetching
- handle POST, UPDATE, DELETE http methods
- handle attaching payload to request
- handle other response format types not just `application/json`

```yml
---
args:
    df_from: URLS
    df_to: DATA
    url_column: url
    method: GET # optional, defaults to GET
    format: JSON # optional: default to JSON
```