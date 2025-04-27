use crate::{
    pipeline::common::{Pipeline, PipelineStage},
    util::error::CpResult,
};

use super::common::YamlRead;

pub fn parse_pipeline(name: &str, value: serde_yaml_ng::Value) -> CpResult<Pipeline> {
    let raw_stages = value.to_val_vec(true)?;
    let mut stages = vec![];
    for stage in raw_stages {
        stages.push(serde_yaml_ng::from_value::<PipelineStage>(stage)?);
    }
    Ok(Pipeline {
        label: name.to_owned(),
        stages,
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        parser::pipeline::parse_pipeline,
        pipeline::common::{Pipeline, PipelineStage},
        util::common::yaml_from_str,
    };

    const DUMMY_PIPELINE_NAME: &str = "mypipe";

    #[test]
    fn valid_basic_pipeline_stage() {
        let config = yaml_from_str(
            "
- label: fetch_cs_player_data
  task: find
  args: 
      database: csdb
      table: players
      find: \"{ player.id : {$in: [1, 2, 3]} }\"
      save_df: CS_PLAYER_DATA
",
        )
        .unwrap();
        let actual = parse_pipeline(DUMMY_PIPELINE_NAME, config).unwrap();
        let expected = Pipeline::new(
            "mypipe",
            &[PipelineStage::new(
                "fetch_cs_player_data",
                "find",
                &yaml_from_str(
                    "
database: csdb
table: players
find: \"{ player.id : {$in: [1, 2, 3]} }\"
save_df: CS_PLAYER_DATA
",
                )
                .unwrap(),
            )],
        );
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_empty_args() {
        let config = yaml_from_str(
            "
- label: fetch_cs_player_data
  task: __noop
  args: 
",
        )
        .unwrap();
        let actual = parse_pipeline(DUMMY_PIPELINE_NAME, config).unwrap();
        assert_eq!(
            actual,
            Pipeline::new(
                "mypipe",
                &[PipelineStage::new(
                    "fetch_cs_player_data",
                    "__noop",
                    &yaml_from_str("---").unwrap(),
                )]
            )
        );
    }
}
