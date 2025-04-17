use std::collections::HashMap;

use yaml_rust2::{Yaml, YamlEmitter};

use crate::{
    pipeline::{
        common::{HasTask, Pipeline, PipelineOnceTask, PipelineStage},
        context::Context,
    },
    task::noop::NoopTask,
    util::{
        common::yaml_to_str,
        error::{CpError, CpResult, SubResult},
    },
};

use super::common::{YamlMapRead, YamlRead};

const LABEL_KEYWORD: &str = "label";
const TASK_KEYWORD: &str = "task";
const ARGS_KEYWORD: &str = "args";

pub fn parse_task(task_key: &str) -> SubResult<PipelineOnceTask> {
    match task_key {
        "__noop" => NoopTask.task(),
        _ => Err(format!("Parser did not recognize task key {}", task_key)),
    }
}

pub fn parse_pipeline_stage(node: &Yaml) -> SubResult<PipelineStage> {
    let nodemap = node.to_map(format!("PipelineStage is not a map: {:?}", node))?;
    let label = nodemap.get_str(
        LABEL_KEYWORD,
        format!("PipelineStage requires `{}`: {:?}", LABEL_KEYWORD, &node),
    )?;
    let task_key = nodemap.get_str(
        TASK_KEYWORD,
        format!("PipelineStage requires `{}`: {:?}", TASK_KEYWORD, &node),
    )?;
    let task = parse_task(&task_key)?;
    let args_yaml_str = match nodemap.get(ARGS_KEYWORD) {
        Some(x) => yaml_to_str(x)?,
        None => return Err(format!("PipelineStage requires `{}`: {:?}", ARGS_KEYWORD, &node)),
    };
    Ok(PipelineStage {
        label,
        task,
        args_yaml_str,
    })
}

pub fn parse_pipeline(name: &str, node: &Yaml) -> SubResult<Pipeline> {
    let stage_configs = node.to_list(format!("Pipeline is not a list of PipelineStage configs: {:?}", &node))?;
    let stages: Vec<PipelineStage> = vec![];

    Ok(Pipeline {
        label: name.to_string(),
        stages,
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        pipeline::common::{HasTask, PipelineOnceTask, PipelineStage},
        task::noop::NoopTask,
        util::common::yaml_from_str,
    };
    use serde::{Deserialize, Serialize};

    use super::parse_pipeline_stage;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    struct TestArgs {
        database: String,
        table: String,
        find: String,
        save_df: String,
    }
    impl TestArgs {
        fn new(database: &str, table: &str, find: &str, save_df: &str) -> TestArgs {
            TestArgs {
                database: database.to_string(),
                table: table.to_string(),
                find: find.to_string(),
                save_df: save_df.to_string(),
            }
        }
    }

    fn noop(label: &str, args_yaml_str: &str) -> PipelineStage {
        PipelineStage {
            label: label.to_string(),
            task: NoopTask.task().unwrap(),
            args_yaml_str: args_yaml_str.to_owned(),
        }
    }

    #[test]
    fn valid_basic_pipeline_stage() {
        let config = yaml_from_str(
            "
label: fetch_cs_player_data
task: __noop
args: 
    database: csdb
    table: players
    find: '{ player.id : {$in: [1, 2, 3]} }'
    save_df: CS_PLAYER_DATA
",
        )
        .unwrap();
        let actual = parse_pipeline_stage(&config).unwrap();
        let expected = noop(
            "fetch_cs_player_data",
            "
database: csdb
table: players
find: '{ player.id : {$in: [1, 2, 3]} }'
save_df: CS_PLAYER_DATA
",
        );
        assert_eq!(actual, expected);

        let actual_args: TestArgs = serde_yaml_ng::from_str(&actual.args_yaml_str).unwrap();
        let expected_args: TestArgs =
            TestArgs::new("csdb", "players", "{ player.id : {$in: [1, 2, 3]} }", "CS_PLAYER_DATA");
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_empty_args() {
        let config = yaml_from_str(
            "
label: fetch_cs_player_data
task: __noop
args: 
",
        )
        .unwrap();
        let actual = parse_pipeline_stage(&config).unwrap();
        assert_eq!(actual, noop("fetch_cs_player_data", "---",));
    }

    #[test]
    fn check_yaml_str_maintain_arg_integrity() {
        let config = yaml_from_str(
            "
label: fetch_cs_player_data
task: __noop
args: 
    database: csdb
    table: players
    find: '{ player.id : {$in: [1, 2, 3]} }'
    save_df: CS_PLAYER_DATA
",
        )
        .unwrap();
        let actual = parse_pipeline_stage(&config).unwrap();
        assert_ne!(actual, noop("fetch_cs_player_data", "---",));
        assert_ne!(
            actual,
            noop(
                "fetch_cs_player_data",
                "
database: csdb
table: players
find: '{ player.id:{$in: [1, 2, 3]} }'
save_df: CS_PLAYER_DATA
"
            )
        );
    }
}
