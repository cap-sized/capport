# Capport [latest:pre-v.0.0.1]

## Overview

A Rust framework for data manipulation.

Basically takes in a folder of configs and extracts/load/transforms/saves data through subtransforms in 
pipelines. 

**UPDATE** (2025-05-08): 
We are drastically changing the structure of capport to support

- easily extensible ingestion (from external dest.)/writing (to external dest.) subtransforms
- templating of variables in reusable Tasks
- async pipeline model
- schedules on subtransforms OR pipelines

## Links


## Roadmap

- [x] LoggerRegistry
- [x] EnvironmentVariablesRegistry
- [x] PipelineFrame
    - [x] PolarsPipelineFrame
- [x] PipelineResults
- [x] PipelineContext
    - [x] Results
    - [ ] ConnectionRegistry
    - [ ] TransformTaskDictionary
    - [ ] ModelRegistry
    - [ ] SourceTaskDictionary
    - [ ] SinkTaskDictionary
    - [ ] Runner
    - [ ] Pipeline
- [x] Trait with execution modes
    - [x] Linear execution
    - [x] Sync concurrent execution
    - [x] Async concurrent execution
- [x] Config utils and parsing
    - [x] deserialize field `StrKeyword` (provides `.symbol()` from "$varname" and `.value(): String` from "litname")
    - [x] deserialize field `PolarsExprKeyword` (provides `.symbol()` from "$varname" and `.value(): PolarsExpr` from "litname.selector.1")
        - [x] Concat
        - [x] Format
        - [x] Literals (u64, i64, str)
    - [x] deserialize field `DTypeKeyword` (provides `.symbol()` from "$varname" and `.value(): PolarsDtype` from "uint64")
    - [x] deserialize field `JTypeKeyword` (provides `.symbol()` from "$varname" and `.value(): PolarsJoinType` from "left")
- [ ] RootTransform and sub-transforms (config and impl)
    - [x] SelectStatement
    - [x] SelectTransform
    - [x] JoinTransform
    - [x] DropTransform
    - [ ] FilterTransform
        - [ ] deserialize field `FilterStmt` { col_keyword_a: { OP : col_keyword_b } } which produces a PolarsStmt ("a OP b", or "OP a")
    - [ ] OrderTransform
    - [ ] SortTransform
    - [ ] SqlTransform (for all other operations)
- [ ] RootSource, method of pulling data, and types
    - [ ] JsonSource
    - [ ] CsvSource
    - [ ] BsonSource (cp_ext)
    - [ ] PostgresSource (cp_ext)
    - [ ] ClickhouseSource (cp_ext)
    - [ ] FromArrowAdapter
- [ ] RootSink, method of writing data, and types
    - [ ] JsonSink
    - [ ] CsvSink
    - [ ] BsonSink (cp_ext)
    - [ ] PostgresSink (cp_ext)
    - [ ] ClickhouseSink (cp_ext)
    - [ ] ToArrowAdapter
- [ ] PipelineRegistry
- [ ] RunnerRegistry
- [ ] Synchronous schedule (per-pipeline)
- [ ] Asynchronous schedule (per-stage)

## Project structure

1. `cp-core` for implementations of:
  - Pipeline
  - Runner
  - Model
  - RootTransform task (and all transform subtasks)
  - RootSource
  - RootSink
  - Logger
  - EnvironmentVariablesRegistry
  - ConnectionTemplatesRegistry
  - LocalVariables
2. `cp-ext` for implementations of features for:
  - postgres
  - mongo
  - clickhouse
  - http
  - others (lower priority): mysql, sqlite, mariadb, websockets
3. `cp-demo` for examples

## Concepts (start here)

### Config

Configuration files fully describe the scope of what the pipeline runner should do.
Currently the config parser only recognizes YAML (.yaml/.yml) files.

The following are `Configurable` traits: 

1. `pipeline`
2. `model`
3. `transform`
4. `connection`
5. `source`
6. `sink`
7. `logger`
8. `runner`

#### Config loading

To extract the config files and pack them into a map of configurables:

```rs
let args: RunPipelineArgs = argh::from_env();
let config_files = read_configs(&args.config_dir, &["yml", "yaml"]).unwrap();
let mut pack = pack_configs_from_files(&config_files).unwrap();
```

#### Config layout

Each config YAML file must be a map at its root level. Each key at the root represents a configurable, 
identified by the `Configurable` in the `context/` directory by their `get_node_name()`.

All configurables must be parsable into a map of named `HashMap<String, serde_yaml_ng::Value>`.
`parse_configs_from_files` collapses each configurable across different files into one map of named configs.

e.g. config/data_pipelines_A.yml
```yml
pipeline:
    a_fetch_update:
        - label: ...
        ...
    a_daily_increment:
        - label: ...

transform: 
    player_stats_clean:
        - ... 
    increment_games_played:
        - ... 
model:
    player:
        ...
```

e.g. config/data_pipelines_B.yml
```yml
pipeline:
    b_fetch_update:
        - label: ...
        ...
    b_daily_increment:
        - label: ...

transform: 
    teams_calculate_daily:
        - ... 
model:
    team:
        ...
```

When both configs are read via `pack_configs_from_files`, the configurables' map becomes equivalent to:

```
pipeline: 
    a_fetch_update: ... 
    a_daily_increment: ... 
transform: 
    player_stats_clean: ...
    increment_games_played: ...
    teams_calculate_daily: ...
model:
    player: ...
    team: ...
```

Clashing names should result in error.

### Program arguments (`RunPipelineArgs`)

To execute a pipeline in the command line, you need:

```sh
./capport_exec \
  -c config_dir/ \    # directory to read input configuration ymls from
  -o output_dir/ \    # directory to put logged outputs and store local writes in
  -p pipeline \       #
  -r runner \         # 
  -e execute \        # optional, when omitted runs in dryrun mode (no write to ext. dbs)
  -d ref_date \       # optional, use is up to exec impl
  -t ref_datetime \   # optional, using this overrides the use of the other
```

#### Environment Variables

Some of these program arguments are registered as privately visible environment variables, 
except pipeline and runner.
`ref_datetime` creates three variables as shown below

| flag | variable | 
| ---- | -------- | 
| `config_dir` | DEFAULT_CONFIG_DIR | 
| `output_dir` | DEFAULT_OUTPUT_DIR | 
| `pipeline` | - | 
| `runner` | - | 
| `execute` | EXECUTE_MODE | 
| `ref_date` | DEFAULT_REF_DATE | 
| `ref_datetime` | DEFAULT_REF_DATE, DEFAULT_REF_DATETIME, DEFAULT_REF_TIMEZONE | 

See `EnvironmentVariablesRegistry` and `envvar.md` in the `docs/` folder for more information.

### Pipeline

Pipelines consist of labelled **subtransforms** (either **source/sink/transform**). 
Each stage performs a **task**. 
Each task loads/transforms/saves tabular **data**. 
See the annotated sample pipeline below

```yml
pipeline:                           
  mass_load_player:                 # Named pipeline
    - label: load_state_province    # Labelled stage
      stage: source                 # Uses the `source` stage's Root task (RootSource) 
      every: 5m                     # [Optional] Runs this stage on loop per this schedule
      task: load_csv                # Task name in `TaskDictionary`
      args: ...                     # Args to be subbed in by task

    - label: fetch_player_data
      stage: source                 
      task: player_mongo
      args: ...

    - label: player_urls            # pipeline can consist of any number of tasks
      stage: transform              # Uses the `transform` stage's Root task (RootTransform) 
      task: transform
      args: ...
```

#### Pipeline Runner

```yml
runner:
  one_shot_runner:
    logger: default
    run: sync
    schedule:
      timezone: UTC
      # 1d or 1w or 10s or 2M (month) or 1Y or 10m30s
      every: Monday at 15:00 # internally parse to .every(Monday).at("15:00")

  interval_runner:
    logger: discord
    run: async
    schedule:
      timezone: UTC
      every: Monday at 15:00 repeating every 30m times 6
```

Launches in the scheduler an async runner for the pipeline.

### Scheduler

There should never be more than one pipeline being run at once. i.e. when another run
starts, the other must have ended or be terminated.

As such if a schedule is found for the internal pipeline, the external pipeline schedule is
invalid, it's presence will result in error.

### Dataframes and dataframe universe

The whole pipeline is based on the manipulation of data with the [Polars](https://docs.rs/polars/latest/polars/) 
library. 

Tasks write dataframes into the pool of results (dataframe universe) readable 
by all tasks in the pipeline.

We are using polars, so as an optimization technique we provide an interface for both
the `LazyFrame` extraction and `DataFrame`. 
In order to read from/write to a dataframe, the appropriate lock (R/W) must be acquired.

In both long running (async) and one-shot (sync) pipelines, tasks are alerted of when to read from any of its dataframe dependencies via the **message broker**.

### Message channels

Allows subtransforms to broadcast or selectively inform tasks of updates. When writing to a 
dataframe, an update sugnal is either broadcasted or sent with a target from the 
producer to the consumers (listeners).

![Message broker and Dataframes](https://github.com/cap-sized/capport/raw/main/docs/img/dataframe-msg-system.png )

In a looping runmode the roots of the pipeline (i.e. subtransforms with no dependencies) 
should always have the longest duration per cycle (loop) in order to prevent writer starvation.

This is not a concern for the one-shot runmode.

### Context

Many tasks (for load/save subtransforms) depend on configured clients for services or previous results. 
Hence the environment of collected results, services and other configured information forms the **context**.

Contexts are highly configurable, as they may vary greatly between different use cases. e.g. They may differ in

- source/sink/transform task 
  - generators
  - connection templates (for source/sink)
  - registries (for the concrete configurations)
- model registry
- runner registry
- logger registry

### subtransforms

A pipeline **stage** consist of its *label*, the *task* it executes and the *arguments* to be 
passed into the task.
The `args` mapping provides a list of scoped local variables the task can use to replace 
any templatable keywords (see example in Transform)

Example:
```yml
rebuild_player_data:

  # source stage:
  - label: get_db_players
    task: mongo_players
    args:
      fields: { id: 1, full_name: 1 }
      output: PLAYERS

  # transform stage:
  - label: build_player_landing_urls
    task: build_from_str_template
    args:
      input: PLAYERS
      template: https://api-web.nhle.com/v1/player/{}/landing
      input_column: id
      output_column: player_url
      urls: PLAYER_URLS


  # another source stage:
  - label: http_get_player_urls
    task: http_get_batch_request
    args:
      input: PLAYER_URLS
      columns: player_url
      output: NHL_PLAYER_DATA

  # another transform stage:
  - label: build_player
    task: unpack_build_player
    args:
      input: NHL_PLAYER_DATA
      output: CS_PLAYER_DATA
  
  # sink stage
  - label: update_players_postgres
    task: update_postgres
    args:
      model: player
      table: player
      input: CS_PLAYER_DATA
```


**WARNING**: A task MAY deadlock if input/output are the same df. 
Eventually we want to have a validator that checks for this.

### Connection templates

Used by source tasks to share client connection details. 

Note: unlike in the previous design, the clients should no longer be shared between subtransforms

```yml

connection:
  default_postgres:
    user_env_var: CS_PG_USER
    password_env_var: CS_PG_PW
    uri_env_var: CS_PG_URI
    default_db: csdb

  prod_mongo:
    uri_env_var: CS_MONGO_URI_PROD
    default_db: csdb

  dev_mongo:
    uri_env_var: CS_MONGO_URI_DEV
    default_db: csdb
```

### Source tasks

Source and sink tasks both consist of 

```yml

source:
  mongo_players: 
    base: mongo           # base task, must be present in implementation TaskDictionary
    use: prod_mongo       # optional name of connection setup
    args:
      db: capsized          # db override
      collection: players
      query: {}
      projection: $fields   # defaults to none, but can override by subbing the
      output: $output

# http_get_batch_request: 
#   base: http_get_batch_request
#   args:
#     input: $input
#     columns: $columns
#     output: $output

# IMPT: in the above example, the base is exactly the same as the source node name,
# and the fields have exactly the same name as the arguments to be sourced from the pipeline stage
# in which case, the following is sufficient and transparently decodes to the above config.

  http_get_batch_request: default
```

### Sink tasks

```yml
sink:
  update_postgres: 
    base: postgres
    use: default_postgres
    args:
      model: $model
      table: $table
      input: $input
```

### Transform tasks

Transform tasks always consist of a list of sub-transforms. They can also have substituted variables

Transform tasks must always start with an `input` sub-transform and can contain one or more `output` sub-transforms.

All they do is simply obtain the read/write lock respectively to read from or write to the dataframe universe.

Transform tasks tend to be more verbose as they will contain a lot of business logic.

```yml
transform:
  cs_player_to_nhl_player_url:
    - input: $input
    - select:
        nhl_url: 
          format: 
            template: "https://api-web.nhle.com/v1/player/{}/landing"
            cols: [ "player_ids.NHL" ]
    - output: $output

  # Get fresh NHL data
  # example: https://api-web.nhle.com/v1/player/8476453/landing
  nhl_player_to_person:
    - input: NHL_PLAYER_DF # example of "hard coded" input df name value
    - select:
        id: csid # from the previous step
        first_name: firstName.default # handle subpaths
        last_name: lastName.default # handle subpaths
        full_name: 
          # action type "concat"
          concat: 
            cols: [ "firstName.default", "lastName.default" ] 
            separator : " "
        birthdate: birthDate
        birth_city: birthCity.default
        birth_state_province_name: birthStateProvince.default
        birth_state_country_code: birthCountry.default
        is_nhl: { lit: true } # action type "lit" aka literal
    - join:
        right: STATE_PROVINCE #  << named df
        right_select: 
          birth_state_province_code: code
          birth_state_province_name: name
        left_on: birth_state_province_name
        right_on: birth_state_province_name
        how: left
    - drop:
        birth_state_province_name: True
    - output: $output
```

### Models

Models describe the shape and specification of a SQL like table (relation) that the data in the Data/Lazyframe
has to adhere to before inserting into dictionaries. 

i.e. Models not only define the schema, but also the constraints of each column, what the primary/unique/foreign
keys are etc. *These constaints are only used for validation in sink subtransforms*.

Example:

```yml
model:
  person:                       # name of relation/table
    id:                         # column label
      dtype: uint64             # datatype
      constraints: [primary]    # vector of constraints as strings
    full_name: str              # shortened [column label : dtype]
    first_name: str
    last_name: str
    birthdate: date
    deathdate: date
    birth_city: str
    birth_country_code: str
    birth_state_province_code: str

pipeline:
    example:
        - label: load_persons
          task: load_csv
          args:
            - filepath: file.csv
              df_name: PERSONS
              model: person     # optional argument model to ensure PERSONS conforms to schema.
        - label: save_persons
          task: save_csv
          args:
            - filepath: another_file.csv
              df_name: PERSONS
              model: person     # optional argument model to ensure PERSONS conforms to schema.

```

The full list of dtype mappings can be found in `capport_core::parser::model::parse_dtype`.

### Logger

by default, writes to `"${DEFAULT_OUTPUT_DIR}/{output_path_prefix}{pipeline_name}_YYYYmmdd_HHMMSS.log"`.

the directory with prefix `"${DEFAULT_OUTPUT_DIR}/{output_path_prefix}` can be overwritten with the `output_path_prefix` 
if it results in an absolute path.

```yml
logger:
  default:
    level: trace
    output_path_prefix: "capport_"
    # alternative:
    # output_path_prefix: "/tmp/non-default/capport_"
```

## For Contributors

### Setup

1. [Optional] If you are not an organization member, fork this repo and clone the project locally

2. Clone the project locally

```sh
git clone https://github.com/<your_user>/capport.git
```

3. If you have never setup up Rust working environment before:

```sh
curl https://sh.rustup.rs -sSf | sh # install nightly
```

4. Restart your terminal and run `cargo build` in the project root directory to check if it works

5. [IMPORTANT] Install the following crates:

```sh
rustup component add clippy
cargo +stable install cargo-llvm-cov --locked
```

### Contribution guidelines

1. All code must be linted

2. All code additiions must pass the **function** and **line** coverage tests by at least 80% 
(if there are existing files with coverage below 80%, it is a todo to fix them)

![Coverage Report](https://github.com/cap-sized/capport/raw/main/docs/img/readme_coverage.png )

3. All tests for requests with external endpoints must be mocked (e.g. httpmock). See task/requests/http.rs 

### Development

```sh
cargo build --verbose
cargo test --verbose
cargo run -- --help # print help
cargo run -- -c config/example -o output/ -p puckdata # example run
cargo clippy --tests -- -Dclippy::all --fix # linter
cargo llvm-cov --open # coverage report
```
