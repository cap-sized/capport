use yaml_rust2::Yaml;

use crate::{
    task::transform::{
        common::{RootTransform, Transform},
        drop::DropTransform,
        join::JoinTransform,
        select::SelectTransform,
    },
    util::error::SubResult,
};

use super::{common::YamlRead, drop::parse_drop_transform, join::parse_join_transform, select::parse_select_transform};

const ALLOWED_NODES: [&str; 3] = [
    SelectTransform::keyword(),
    JoinTransform::keyword(),
    DropTransform::keyword(),
];

pub fn parse_root_transform(name: &str, node: &Yaml) -> SubResult<RootTransform> {
    let stages_configs = node.to_list(format!(
        "Child of transform node {} is not a list of select/join/drop nodes, invalid: {:?}",
        name, node
    ))?;

    let mut stages: Vec<Box<dyn Transform>> = vec![];
    for raw_config in stages_configs {
        let config = raw_config.to_map(format!(
            "Stage of transform node {} is not a map/join/drop node: {:?}",
            name, raw_config,
        ))?;
        if config.len() != 1 {
            return Err(format!(
                "Stage of transform node {} does not have exactly one key of {:?} (keys: {:?}, len={})",
                name,
                ALLOWED_NODES,
                &config.keys(),
                &config.len()
            ));
        }
        if config.contains_key(SelectTransform::keyword()) {
            match parse_select_transform(config.get(SelectTransform::keyword()).unwrap()) {
                Ok(x) => stages.push(Box::new(x)),
                Err(e) => return Err(e),
            };
        } else if config.contains_key(JoinTransform::keyword()) {
            match parse_join_transform(config.get(JoinTransform::keyword()).unwrap()) {
                Ok(x) => stages.push(Box::new(x)),
                Err(e) => return Err(e),
            };
        } else if config.contains_key(DropTransform::keyword()) {
            match parse_drop_transform(config.get(DropTransform::keyword()).unwrap()) {
                Ok(x) => stages.push(Box::new(x)),
                Err(e) => return Err(e),
            };
        } else {
            return Err(format!(
                "Stage of transform node {} does not have exactly one key of {:?} (keys: {:?})",
                name,
                ALLOWED_NODES,
                &config.keys()
            ));
        }
    }
    Ok(RootTransform::new(name, stages))
}

#[cfg(test)]
mod tests {
    use polars::{
        df,
        prelude::{DataFrameJoinOps, PlSmallStr},
    };
    use polars_lazy::{
        dsl::col,
        frame::{IntoLazy, LazyFrame},
    };

    use crate::{
        pipeline::results::PipelineResults,
        task::transform::common::{RootTransform, Transform},
        util::common::yaml_from_str,
    };

    use super::parse_root_transform;

    fn state_code() -> LazyFrame {
        // https://ddvat.gov.in/docs/List%20of%20State%20Code.pdf
        df! [
            "state" => ["Karnataka", "Goa", "Tamil Nadu", "Delhi"],
            "state_code" => ["KA", "GA", "TN", "DL"],
            "tin" => [29, 30, 33, 7],
        ]
        .unwrap()
        .lazy()
    }

    fn lf() -> LazyFrame {
        df![
            "csid" => [8872631, 82938842, 86543102],
            "playerId" => ["abcd", "88ef", "1988"],
            "shootsCatches" => ["L", "R", "L"],
            "state" => ["TN", "DL", "GA"],
            "name" => df![
                "first" => ["Darren", "Hunter", "Varya"],
                "last" => ["Hutnaby", "O'Connor", "Zeb"],
            ].unwrap().into_struct(PlSmallStr::from_str("name")),
        ]
        .unwrap()
        .lazy()
    }

    fn valid_root_transform_select_only() {
        let config = yaml_from_str(
            "
- select:
    person_id: csid # from the previous step
    player_id: playerId
    shoots_catches: shootsCatches
    first_name: first.name
    last_name: last.name
",
        )
        .unwrap();
        let root = parse_root_transform("player", &config).unwrap();
        let res_before = PipelineResults::new();
        let actual_df = root.run(lf(), &res_before).unwrap().collect().unwrap();
        assert_eq!(
            actual_df,
            df![
                "person_id" => ["abcd", "88ef", "1988"],
                "player_id" => [8872631, 82938842, 86543102],
                "shoots_catches" => ["L", "R", "L"],
                "first_name" => ["Darren", "Hunter", "Varya"],
                "last_name" => ["Hutnaby", "O'Connor", "Zeb"],
            ]
            .unwrap()
        );
    }
    fn valid_root_transform_join_only() {
        let mut res_before = PipelineResults::new();
        res_before.insert("STATE_CODE", state_code());
        let config = yaml_from_str(
            "
- join:
    right: STATE_CODE
    right_select:
        state_name: state
        state_code: state_code
    left_on: state
    right_on: state_code
    how: left
",
        )
        .unwrap();
        let root = parse_root_transform("player", &config).unwrap();
        let actual_df = root.run(lf(), &res_before).unwrap().collect().unwrap();
        assert_eq!(
            actual_df,
            lf().left_join(state_code().drop(vec![col("tin")]), "state", "state")
                .collect()
                .unwrap()
        );
    }
    fn valid_root_transform_drop_only() {
        let config = yaml_from_str(
            "
- drop:
    name: True
",
        )
        .unwrap();
        let root = parse_root_transform("player", &config).unwrap();
        let res_before = PipelineResults::new();
        let actual_df = root.run(lf(), &res_before).unwrap().collect().unwrap();
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
    // fn valid_root_transform_select_join() {}
    // fn valid_root_transform_join_drop() {}
    // fn valid_root_transform_join_select() {}
}
