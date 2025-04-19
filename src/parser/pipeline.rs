use std::collections::{HashMap, HashSet};

use yaml_rust2::{Yaml, YamlEmitter};

use crate::{
    pipeline::{
        common::{HasTask, Pipeline, PipelineStage, PipelineTask},
        context::DefaultContext,
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

pub fn parse_pipeline_stage(node: &Yaml) -> SubResult<PipelineStage> {
    let nodemap = node.to_map(format!("PipelineStage is not a map: {:?}", node))?;
    let label = nodemap.get_str(
        LABEL_KEYWORD,
        format!("PipelineStage requires `{}`: {:?}", LABEL_KEYWORD, &node),
    )?;
    let task_name = nodemap.get_str(
        TASK_KEYWORD,
        format!("PipelineStage requires `{}`: {:?}", TASK_KEYWORD, &node),
    )?;
    let args_node = match nodemap.get(ARGS_KEYWORD) {
        Some(&x) => x.clone(),
        None => return Err(format!("PipelineStage requires `{}`: {:?}", ARGS_KEYWORD, &node)),
    };
    Ok(PipelineStage {
        label,
        task_name,
        args_node,
    })
}

pub fn parse_pipeline(name: &str, node: &Yaml) -> SubResult<Pipeline> {
    let stage_configs = node.to_list(format!("Pipeline is not a list of PipelineStage configs: {:?}", &node))?;
    let mut stages: Vec<PipelineStage> = vec![];
    let mut seen_label: HashSet<String> = HashSet::new();
    for config in stage_configs {
        match parse_pipeline_stage(config) {
            Ok(x) => {
                if seen_label.contains(&x.label) {
                    return Err(format!("Duplicate stage label `{}` in pipeline", &x.label));
                }
                seen_label.insert(x.label.clone());
                stages.push(x);
            }
            Err(e) => return Err(e),
        }
    }

    Ok(Pipeline {
        label: name.to_string(),
        stages,
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        pipeline::common::{HasTask, PipelineStage, PipelineTask},
        task::noop::NoopTask,
        util::common::yaml_from_str,
    };
    use serde::{Deserialize, Serialize};

    use super::parse_pipeline_stage;

    #[test]
    fn valid_basic_pipeline_stage() {
        let config = yaml_from_str(
            "
label: fetch_cs_player_data
task: mongo_find
args: 
    database: csdb
    table: players
    find: '{ player.id : {$in: [1, 2, 3]} }'
    save_df: CS_PLAYER_DATA
",
        )
        .unwrap();
        let actual = parse_pipeline_stage(&config).unwrap();
        let expected = PipelineStage::new(
            "fetch_cs_player_data",
            "mongo_find",
            &yaml_from_str(
                "
database: csdb
table: players
find: '{ player.id : {$in: [1, 2, 3]} }'
save_df: CS_PLAYER_DATA
",
            )
            .unwrap(),
        );
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
        assert_eq!(
            actual,
            PipelineStage::new("fetch_cs_player_data", "__noop", &yaml_from_str("---").unwrap(),)
        );
    }

    #[test]
    fn check_yaml_str_maintain_arg_integrity() {
        let config = yaml_from_str(
            "
label: fetch_cs_player_data
task: mongo_find
args: 
    database: csdb
    table: players
    find: '{ player.id : {$in: [1, 2, 3]} }'
    save_df: CS_PLAYER_DATA
",
        )
        .unwrap();
        let actual = parse_pipeline_stage(&config).unwrap();
        assert_ne!(
            actual,
            PipelineStage::new("fetch_cs_player_data", "mongo_find", &yaml_from_str("---").unwrap())
        );
        assert_ne!(
            actual,
            PipelineStage::new(
                "fetch_cs_player_data",
                "mongo_find",
                &yaml_from_str(
                    "
database: csdb
table: players
find: '{ player.id:{$in: [1, 2, 3]} }'
save_df: CS_PLAYER_DATA
"
                )
                .unwrap()
            )
        );
    }
}
