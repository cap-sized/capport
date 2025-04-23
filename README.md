# Capport [latest:pre-v.0.0.1]

## Overview

A Rust framework for data manipulation.

Basically takes in a folder of configs and extracts/load/transforms/saves data through stages in 
pipelines. 

[Work in progress] Pipelines are designed to be customizable in terms of the 
- async or sync
- when and how it's scheduled: one-shot or on-loop
- configurable transform stages
- configurable pre-built request clients (e.g. HTTP, or database connections e.g. MongoDB/SQL databases)

## Roadmap

- [ ] 

## Project structure

| crate | description | confirmed | 
| ----- | ----------- | --------- |
| `capport_core` | Contains implementation for transforms, models, interfaces for pipelines common pipelines, common tasks | [x] |
| `capport_service` | Provide service interface and tasks specifically requiring services | [x] |
| `capport_default` | Example of how to use the pipeline from `capport_core` adding your own service distributor, and switching between two different dataframe types | [x] |
| `capport_recon` | [Unconfirmed] Recon pipeline implementation | [ ] |
| `capport_live` | [Unconfirmed] Live loop pipeline implementation | [ ] |

## Concepts (start here)

### Config

Configuration files fully describe the scope of what the pipeline runner should do.
Currently the config parser only recognizes YAML (.yaml/.yml) files.

There are 3 currently kinds of configurables which implement the `Configurable` trait: 

1. Pipeline (lists of tasks)
2. Model
3. Transform

TODO: Eventually we should add the following as configurables too

1. Different Service Distributors
2. Logger
3. Pipeline Run Scheduler/Executor 
    - select pipeline runner, choice of logger, choice of service distributor

The example [main.rs](https://github.com/cap-sized/capport/blob/main/capport_default/src/main.rs) 
in capport_default shows how you can use the framework to handle any sort of pipelines fed through the
configs in the folder `config/`.

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

All configurables must be parsable into a map of named `HashMap<String, Yaml>`.
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
    a_daily_increment: ... 
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

### Pipeline

Pipelines consist of labelled **stages**. Each stage performs a **task**. Each task loads/transforms/saves 
tabular **data**. See the annotated sample pipeline below

```yml
pipeline:                           # CONFIGURABLE 
  mass_load_player:                 # Named pipeline
    - label: load_state_province    # Labelled stage
      task: load_csv                # Task name in `TaskDictionary`
      args: ...                     # Args to be parsed by task

    - label: fetch_player_data
      task: mongo
      args: ...

    - label: player_urls            # pipeline can consist of any number of tasks
      task: transform
      args: ...
```

#### Pipeline Runner

Stages are currently run linearly with `PipelineRunner`. Soon this will either be called 
`LinearSyncPipelineRunner` or it will come to support async and not chance its name. 

Eventually this runner should be configurable as well, with the pipeline run scheduler/executor.

### Dataframes

The whole pipeline is based on the manipulation of data with the [Polars](https://docs.rs/polars/latest/polars/) 
library. Tasks load from and store **manipulated Dataframes** or their lazily computed version, 
**Lazyframes**.

NOTE: Though `PipelineResults<T>` can be configured to store any data type `T`, support for the most 
commonly used data manipulation tasks are for `LazyFrame` and `DataFrame` types specifically.

### Context

Many tasks (for load/save stages) depend on configured clients for services or previous results. 
Hence the environment of collected results, services and other configured information forms the **context**.

Contexts are highly configurable, as they may vary greatly between different use cases. e.g. They may differ in

- which set of services are required
- result type for pipeline results
- does it support concurrent access/is it threadsafe?
    - NOTE: Crucially the current implementations are single threaded and have synchronous execution of tasks
    but the plan is to introduce MT async execution recurring pipelines. This requires `PipelineContext` to be
    threadsafe, but the current `DefaultContext<FrameType, ()>` is currently insufficient

Each context must implement the `PipelineContext<ResultType, ServiceDistributor>` trait. This allows 
the provided `PipelineRunner`s to interact with the context 

TODO: We need a monitor/metrics manager added to the context interface as well, with a `NoopMonitor` provided in 
the default implementation of `DefaultContext`.

### Stages and tasks

A pipeline **stage** consist of its *label*, the *task* it executes and the *arguments* to be passed into 
the task.

Tasks are single actions that do something based on the context and the task parameters (configured via `args`).

Example:
```yml
    - label: load_puckdata
      task: load_csv
      args: 
        - filepath: "$PROJECT_ROOT/puckdata.csv"
          df_name: PUCKDATA
        - filepath: "$PROJECT_ROOT/team.csv"
          df_name: TEAM
        - filepath: "$PROJECT_ROOT/player.csv"
          df_name: PLAYER
```

The stage `load_puckdata` passes the list of files to load into the task `load_csv`.

> Q: What's the difference between `PipelineTask` and any struct implementing `HasTask`?
> 
> A: The struct implementing `HasTask` parses the args and selects/enriches the correct `PipelineTask` to run
> with the provided `PipelineContext`, returning it as its own closure.

### Models

Models describe the shape and specification of a SQL like table (relation) that the data in the Data/Lazyframe
has to adhere to before inserting into dictionaries. 

i.e. Models not only define the schema, but also the constraints of each column, what the primary/unique/foreign
keys are etc. *However there is currently no implementation requiring or utilizing these constraints*.

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

Currently only used by `load_csv` and `save_csv`.

### Transform

Transforms are basically wrappers over typical Polars/Pandas manipulation methods.

Currently supports the following column operations:

- Select
    - Format (action)
    - Concat (action)
- Join
- Drop

To support in the future

- Filter
- Sort
- Limit
- Full SQL operations

#### `SelectField` (and `DropField`) syntax

#### Transform actions in `SelectField`

Sometimes support for more complicated data manipulation options e.g. concat/format are required.

In `capport_core::transform::action` we have implementations for the above two expressions.
Importantly they are created from parsing their yaml_str and producing the `expr()` they hold.

#### Implementing new transforms operations

Refer to `capport_core::transform::drop` as reference.

### Services

Every task that interacts with a service must access the `ServiceDistributor` from the context via the 
`ctx.svc()` method from the PipelineContext. Similar to the PipelineContext this must implement the unsafe
traits `Send` and `Sync` (see capport_core/src/pipeline/)

Services will go into a separate crate `capport_service`. 

## Important dependencies

See the full lists of dependencies from the Cargo.toml files.

### capport_core

- [serde_yaml_ng](https://github.com/acatton/serde-yaml-ng)
  - Since we use serde_yaml_ng which only parses YAML specification 1.1, our configs are also necessarily 
  only following YAML 1.1
- [yaml_rust2](https://docs.rs/yaml-rust2/latest/yaml_rust2/)
  - for dynamic traversal of YAML configs
- [polars](https://github.com/pola-rs/polars)
- [argh](https://github.com/google/argh)
  - argument parsing
- [reqwest](https://github.com/seanmonstar/reqwest)
  - HTTP client
- [tokio](https://github.com/tokio-rs/tokio)
  - async runtime

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
(if there are existing files with coverage below 80% e.g. parser/join.rs or parser/model.rs, 
it is a todo to fix them)

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