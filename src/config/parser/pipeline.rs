use std::collections::HashMap;

use yaml_rust2::{Yaml, YamlEmitter};

use crate::{
    pipeline::common::{Pipeline, PipelineStage, PipelineTask},
    util::{common::yaml_to_str, error::SubResult},
};

use super::common::{YamlMapRead, YamlRead};

const LABEL_KEYWORD: &str = "label";
const TASK_KEYWORD: &str = "task";
const ARGS_KEYWORD: &str = "args";

pub fn parse_task(task_key: &str) -> Option<PipelineTask> {
    match task_key {
        "__noop" => Some(Ok),
        _ => None,
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
    let task = match parse_task(&task_key) {
        Some(x) => x,
        None => return Err(format!("Task not recognized: {}", &task_key)),
    };
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
    use crate::{pipeline::common::PipelineStage, util::common::yaml_from_str};

    use super::parse_pipeline_stage;

    #[test]
    fn valid_basic_pipeline_stage() {
        let config = yaml_from_str(
            "
label: fetch_cs_player_data
task: __noop
args: 
    database: csdb
    table: players
    action: match
    match: {}
    save_df: CS_PLAYER_DATA
",
        )
        .unwrap();
        let actual = parse_pipeline_stage(&config).unwrap();
        let expected = PipelineStage::noop(
            "fetch_cs_player_data",
            "
database: csdb
table: players
action: match
match: {}
save_df: CS_PLAYER_DATA
",
        );
        // println!("{}", &actual.args_yaml_str);
        // println!("{}", &expected.args_yaml_str);
        assert_eq!(actual, expected);
    }
}
