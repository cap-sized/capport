use crate::{
    transform::{
        common::{RootTransform, Transform},
        drop::DropTransform,
        join::JoinTransform,
        select::SelectTransform,
    },
    util::error::{CpError, CpResult},
};

use super::{common::YamlRead, drop::parse_drop_transform, join::parse_join_transform, select::parse_select_transform};

const SELECT: &str = SelectTransform::keyword();
const JOIN: &str = JoinTransform::keyword();
const DROP: &str = DropTransform::keyword();

const ALLOWED_NODES: [&str; 3] = [SELECT, JOIN, DROP];

pub fn parse_root_transform(name: &str, node: serde_yaml_ng::Value) -> CpResult<RootTransform> {
    let stages_configs = node.to_val_vec(true)?;

    let mut stages: Vec<Box<dyn Transform>> = vec![];
    for raw_config in stages_configs {
        let mut config = raw_config.to_str_val_vec()?;
        if config.len() != 1 {
            return Err(CpError::ConfigError(
                "Transform subnodes can only have one key each",
                format!(
                    "Stage of transform node {} does not have exactly one key of {:?} (config: {:?})",
                    name, ALLOWED_NODES, &config,
                ),
            ));
        }
        let (nodetype, args) = config.pop().unwrap();
        match nodetype.as_str() {
            SELECT => {
                stages.push(Box::new(match parse_select_transform(args) {
                    Ok(x) => x,
                    Err(e) => return Err(CpError::ConfigError("Error parsing Select", e.to_string())),
                }));
            }
            JOIN => {
                stages.push(Box::new(match parse_join_transform(args) {
                    Ok(x) => x,
                    Err(e) => return Err(CpError::ConfigError("Error parsing Join", e.to_string())),
                }));
            }
            DROP => {
                stages.push(Box::new(match parse_drop_transform(args) {
                    Ok(x) => x,
                    Err(e) => return Err(CpError::ConfigError("Error parsing Drop", e.to_string())),
                }));
            }
            x => {
                return Err(CpError::ConfigError(
                    "Transform subnodes not recognized",
                    format!(
                        "Stage of transform node {} is not one of {:?}: {}",
                        name, ALLOWED_NODES, x,
                    ),
                ));
            }
        };
    }
    Ok(RootTransform::new(name, stages))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use polars::{df, prelude::LazyFrame};
    use polars_lazy::dsl::col;

    use crate::{
        pipeline::results::PipelineResults,
        transform::{common::Transform, drop::DropTransform, join::JoinTransform, select::SelectTransform},
        util::common::{DummyData, yaml_from_str},
    };

    use super::{ALLOWED_NODES, parse_root_transform};

    #[test]
    fn check_valid_transform_types() {
        assert_eq!(
            ALLOWED_NODES,
            [
                SelectTransform::keyword(),
                JoinTransform::keyword(),
                DropTransform::keyword(),
            ]
        );
    }

    fn create_results() -> Arc<RwLock<PipelineResults<LazyFrame>>> {
        let res_before = Arc::new(RwLock::new(PipelineResults::<LazyFrame>::default()));
        let to_return = res_before.clone();
        let mut writer = res_before.write().unwrap();
        writer.insert("STATE_CODE", DummyData::state_code());
        to_return
    }

    #[test]
    fn valid_root_transform_select_only() {
        let config = yaml_from_str(
            "
- select:
    person_id: csid # from the previous step
    player_id: playerId
    shoots_catches: shootsCatches
    first_name: name.first
    last_name: name.last
",
        )
        .unwrap();
        let root = parse_root_transform("player", config).unwrap();
        let res_before = create_results();
        let actual_df = root
            .run_lazy(DummyData::player_data(), res_before)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(
            actual_df,
            df![
                "person_id" => [8872631, 82938842, 86543102],
                "player_id" => ["abcd", "88ef", "1988"],
                "shoots_catches" => ["L", "R", "L"],
                "first_name" => ["Darren", "Hunter", "Varya"],
                "last_name" => ["Hutnaby", "O'Connor", "Zeb"],
            ]
            .unwrap()
        );
    }

    #[test]
    fn valid_root_transform_join_only() {
        let res_before = create_results();
        let config = yaml_from_str(
            "
- join:
    join: STATE_CODE
    right_select:
        state_name: state
        state_code: state_code
    left_on: state
    right_on: state_code
    how: left
",
        )
        .unwrap();
        let root = parse_root_transform("player", config).unwrap();
        let actual_df = root
            .run_lazy(DummyData::player_data(), res_before)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(
            actual_df,
            DummyData::player_data()
                .left_join(DummyData::state_code().drop([col("tin")]), "state", "state_code")
                .collect()
                .unwrap()
        );
    }

    #[test]
    fn valid_root_transform_drop_only() {
        let config = yaml_from_str(
            "
- drop:
    name: True
",
        )
        .unwrap();
        let root = parse_root_transform("player", config).unwrap();
        let res_before = create_results();
        let actual_df = root
            .run_lazy(DummyData::player_data(), res_before)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(
            actual_df,
            df![
                "csid" => [8872631, 82938842, 86543102],
                "playerId" => ["abcd", "88ef", "1988"],
                "shootsCatches" => ["L", "R", "L"],
                "state" => ["TN", "DL", "GA"],
            ]
            .unwrap()
        );
    }

    #[test]
    fn valid_root_transform_select_join() {
        let config = yaml_from_str(
            "
- select:
    player_id: playerId
    state: state
    first_name: name.first
    last_name: name.last
- join:
    join: STATE_CODE
    right_select:
        state: state_code
        tin: tin
    left_on: state
    right_on: state
    how: left
",
        )
        .unwrap();
        let root = parse_root_transform("player", config).unwrap();
        let res_before = create_results();
        let actual_df = root
            .run_lazy(DummyData::player_data(), res_before)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(
            actual_df,
            df![
                "player_id" => ["abcd", "88ef", "1988"],
                "state" => ["TN", "DL", "GA"],
                "first_name" => ["Darren", "Hunter", "Varya"],
                "last_name" => ["Hutnaby", "O'Connor", "Zeb"],
                "tin" => [33, 7, 30],
            ]
            .unwrap()
        );
    }

    #[test]
    fn valid_root_transform_join_drop() {
        let config = yaml_from_str(
            "
- join:
    join: STATE_CODE
    right_select:
        state: state_code
        tin: tin
    left_on: state
    right_on: state
    how: left
- drop:
    shootsCatches: True
    name: True
    playerId: True
",
        )
        .unwrap();
        let root = parse_root_transform("player", config).unwrap();
        let res_before = create_results();
        let actual_df = root
            .run_lazy(DummyData::player_data(), res_before)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(
            actual_df,
            df![
                "csid" => [8872631, 82938842, 86543102],
                "state" => ["TN", "DL", "GA"],
                "tin" => [33, 7, 30],
            ]
            .unwrap()
        );
    }

    #[test]
    fn valid_root_transform_join_select() {
        let config = yaml_from_str(
            "
- join:
    join: STATE_CODE
    right_select:
        state: state_code
        tin: tin
    left_on: state
    right_on: state
    how: left
- select:
    player_id: playerId
    state: state
    tin: tin
    first_name: name.first
    last_name: name.last
",
        )
        .unwrap();
        let root = parse_root_transform("player", config).unwrap();
        let res_before = create_results();
        let actual_df = root
            .run_lazy(DummyData::player_data(), res_before)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(
            actual_df,
            df![
                "player_id" => ["abcd", "88ef", "1988"],
                "state" => ["TN", "DL", "GA"],
                "tin" => [33, 7, 30],
                "first_name" => ["Darren", "Hunter", "Varya"],
                "last_name" => ["Hutnaby", "O'Connor", "Zeb"],
            ]
            .unwrap()
        );
    }
}
