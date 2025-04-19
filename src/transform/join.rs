use std::sync::RwLock;

use polars::prelude::*;
use polars_lazy::prelude::*;

use crate::{parser::join::parse_jointype, pipeline::results::PipelineResults, util::error::SubResult};

use super::{
    common::Transform,
    select::{SelectField, SelectTransform},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinTransform {
    pub join: String,
    pub right_select: Vec<SelectField>,
    pub how: JoinType,
    pub left_on: Vec<String>,
    pub right_on: Vec<String>,
}

impl JoinTransform {
    pub const fn keyword() -> &'static str {
        "join"
    }
    pub fn new(join: &str, left: &str, right: &str, right_select: &[SelectField], how: JoinType) -> JoinTransform {
        JoinTransform {
            join: join.to_owned(),
            left_on: left.split(',').map(|x| x.to_owned()).collect(),
            right_on: right.split(',').map(|x| x.to_owned()).collect(),
            right_select: right_select.to_vec(),
            how,
        }
    }
}

impl Transform for JoinTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", &self)
    }
    fn run_lazy(&self, curr: LazyFrame, results: Arc<RwLock<PipelineResults<LazyFrame>>>) -> SubResult<LazyFrame> {
        let b = results.read().unwrap();
        let right = match b.get_unchecked(&self.join) {
            Some(x) => x,
            None => return Err(format!("Dataframe named {} not found in results", &self.join)),
        };
        let mut select_cols: Vec<Expr> = vec![];
        for select in &self.right_select {
            match select.expr() {
                Ok(valid_expr) => select_cols.push(valid_expr),
                Err(err_msg) => return Err(format!("JoinTransform: {}", err_msg)),
            }
        }
        let left_on = self.left_on.iter().map(col).collect::<Vec<_>>();
        let right_on = self.right_on.iter().map(col).collect::<Vec<_>>();
        Ok(curr.join(
            if select_cols.is_empty() {
                right
            } else {
                right.select(select_cols)
            },
            left_on,
            right_on,
            JoinArgs::new(self.how.clone()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::RwLock};

    use polars::{df, prelude::*};
    use polars_lazy::prelude::*;

    use crate::{
        pipeline::results::PipelineResults,
        transform::{common::Transform, select::SelectField},
        util::common::DummyData,
    };

    use super::JoinTransform;

    #[test]
    fn valid_basic_left_join_transform() {
        let jt = JoinTransform::new(
            "STATE_CODE",
            "state",
            "state_code",
            &[],
            polars::prelude::JoinType::Left,
        );
        let results = Arc::new(RwLock::new(PipelineResults {
            results: HashMap::from([("STATE_CODE".to_string(), DummyData::state_code())]),
        }));

        let actual = jt
            .run_lazy(DummyData::player_data(), results)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(
            actual,
            df![
                "csid" => [8872631, 82938842, 86543102],
                "playerId" => ["abcd", "88ef", "1988"],
                "shootsCatches" => ["L", "R", "L"],
                "state" => ["TN", "DL", "GA"],
                "name" => df![
                    "first" => ["Darren", "Hunter", "Varya"],
                    "last" => ["Hutnaby", "O'Connor", "Zeb"],
                ].unwrap().into_struct(PlSmallStr::from_str("name")),
                "state_right" => ["Tamil Nadu", "Delhi", "Goa"],
                "tin" => [33, 7, 30],
            ]
            .unwrap()
        );
    }

    #[test]
    fn valid_basic_full_join_transform() {
        let jt = JoinTransform::new(
            "PLAYER_DATA",
            "csid",
            "csid",
            &[
                SelectField::new("csid", "csid"),
                SelectField::new("name", "name"),
                SelectField::new("shoots", "shootsCatches"),
            ],
            polars::prelude::JoinType::Full,
        );
        let results = Arc::new(RwLock::new(PipelineResults {
            results: HashMap::from([("PLAYER_DATA".to_string(), DummyData::player_data())]),
        }));

        let orig_df = DummyData::player_scores().filter(col("csid").neq(lit(82938842)));
        let actual = jt.run_lazy(orig_df, results).unwrap().collect().unwrap();
        assert_eq!(
            actual.fill_null(FillNullStrategy::Zero).unwrap(),
            df![
                "csid" => [86543102, 86543102, 86543102, 8872631, 0],
                "game" => [1, 2, 3, 1, 0],
                "scores" => [43, 50, 12, 19, 0],
                "csid_right" => [86543102, 86543102, 86543102, 8872631, 82938842],
                "name" => df![
                    "first" => ["Varya", "Varya", "Varya", "Darren", "Hunter"],
                    "last" => ["Zeb", "Zeb", "Zeb", "Hutnaby", "O'Connor"],
                ].unwrap().into_struct(PlSmallStr::from_str("name")),
                "shoots" => ["L", "L", "L", "L", "R"]
            ]
            .unwrap()
        );
    }

    #[test]
    fn valid_basic_right_join_transform() {
        let jt = JoinTransform::new(
            "PLAYER_SCORES",
            "csid",
            "csid",
            &[SelectField::new("csid", "csid"), SelectField::new("scores", "scores")],
            polars::prelude::JoinType::Right,
        );
        let results = Arc::new(RwLock::new(PipelineResults {
            results: HashMap::from([("PLAYER_SCORES".to_string(), DummyData::player_scores())]),
        }));

        let orig_df = DummyData::player_data().select([col("csid"), col("name"), col("shootsCatches")]);
        let actual = jt.run_lazy(orig_df, results).unwrap().collect().unwrap();
        assert_eq!(
            actual,
            df![
                "name" => df![
                    "first" => ["Hunter", "Hunter", "Varya", "Hunter", "Varya", "Varya", "Darren"],
                    "last" => ["O'Connor", "O'Connor", "Zeb", "O'Connor", "Zeb", "Zeb", "Hutnaby"],
                ].unwrap().into_struct(PlSmallStr::from_str("name")),
                "shootsCatches" => ["R", "R", "L", "R", "L", "L", "L"],
                "csid" => [82938842, 82938842, 86543102, 82938842, 86543102, 86543102, 8872631],
                "scores" => [20, 3, 43, -7, 50, 12, 19],
            ]
            .unwrap()
        );
    }

    #[test]
    fn valid_right_join_transform() {
        let jt = JoinTransform::new(
            "PLAYER_SCORES",
            "csid",
            "csid",
            &[SelectField::new("csid", "csid"), SelectField::new("scores", "scores")],
            polars::prelude::JoinType::Right,
        );
        let results = Arc::new(RwLock::new(PipelineResults {
            results: HashMap::from([("PLAYER_SCORES".to_string(), DummyData::player_scores())]),
        }));

        let orig_df = DummyData::player_data().select([col("csid"), col("name"), col("shootsCatches")]);
        let actual = jt.run_lazy(orig_df, results).unwrap().collect().unwrap();
        assert_eq!(
            actual,
            df![
                "name" => df![
                    "first" => ["Hunter", "Hunter", "Varya", "Hunter", "Varya", "Varya", "Darren"],
                    "last" => ["O'Connor", "O'Connor", "Zeb", "O'Connor", "Zeb", "Zeb", "Hutnaby"],
                ].unwrap().into_struct(PlSmallStr::from_str("name")),
                "shootsCatches" => ["R", "R", "L", "R", "L", "L", "L"],
                "csid" => [82938842, 82938842, 86543102, 82938842, 86543102, 86543102, 8872631],
                "scores" => [20, 3, 43, -7, 50, 12, 19],
            ]
            .unwrap()
        );
    }
}
