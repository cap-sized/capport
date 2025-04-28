use std::fmt::Formatter;
use std::sync::{Arc, RwLock};

use polars_lazy::frame::LazyFrame;
use polars_sql::SQLContext;

use crate::transform::common::Transform;
use crate::{
    pipeline::results::PipelineResults,
    util::error::{CpError, CpResult},
};

#[derive(Clone, Debug, PartialEq, Eq)]

pub struct SqlTransform {
    pub sql: String,
}

impl SqlTransform {
    pub const fn keyword() -> &'static str {
        "sql"
    }
    pub fn new(sql: &str) -> SqlTransform {
        SqlTransform { sql: sql.to_string() }
    }
}

impl Transform for SqlTransform {
    fn run_lazy(&self, curr: LazyFrame, results: Arc<RwLock<PipelineResults<LazyFrame>>>) -> CpResult<LazyFrame> {
        let mut context = SQLContext::new();
        let b = results.read()?;
        for df_name in b.keys() {
            let df = match b.get_unchecked(&df_name) {
                Some(x) => x,
                None => {
                    return Err(CpError::ComponentError(
                        "Dataframe not found in results: ",
                        df_name.to_string(),
                    ));
                }
            };
            context.register(&df_name, df);
        }
        context.register("self", curr);

        let new_frame = context
            .execute(&self.sql)
            .map_err(|e| CpError::ComponentError("polars_sql error", e.to_string()))?;

        Ok(new_frame)
    }

    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", &self.sql)
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::results::PipelineResults;
    use crate::transform::sql::SqlTransform;
    use crate::transform::sql::Transform;
    use crate::util::common::DummyData;
    use crate::util::json::vec_str_json_to_df;
    use polars::prelude::*;
    use polars_lazy::frame::LazyFrame;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    #[test]
    fn sql_transform() {
        let sample_df = vec_str_json_to_df(&[DummyData::shift_charts()]).unwrap().lazy();

        let transform = SqlTransform::new("select data.* from (SELECT unnest(data) FROM self) unnested_data");
        let results = Arc::new(RwLock::new(PipelineResults::<LazyFrame>::default()));

        let actual_df = transform
            .run_lazy(sample_df.clone(), results)
            .unwrap()
            .collect()
            .unwrap();
        let expected_df = sample_df
            .clone()
            .explode(["data"])
            .unnest(["data"])
            .drop(["total"])
            .collect()
            .unwrap();

        let actual_sorted = actual_df.sort(["id"], SortMultipleOptions::default()).unwrap();
        let expected_sorted = expected_df.sort(["id"], SortMultipleOptions::default()).unwrap();

        assert_eq!(actual_sorted, expected_sorted);
    }

    #[test]
    fn sql_transform_join() {
        let df1 = df![
            "id" => [1, 2, 2],
            "val_1" => [5, 6, 7],
        ]
        .unwrap()
        .lazy();
        let df2 = df![
            "id" => [2, 2, 3],
            "val_2" => [8, 9, 10],
        ]
        .unwrap()
        .lazy();
        let expected_df = df![
            "id" => [2, 2, 2, 2],
            "val_1" => [6, 6, 7, 7],
            "val_2" => [8, 9, 8, 9],
        ]
        .unwrap();

        let results = Arc::new(RwLock::new(PipelineResults::new(HashMap::from([(
            "df2".to_string(),
            df2,
        )]))));

        let transform =
            SqlTransform::new("select self.id, self.val_1, df2.val_2 from self join df2 on self.id = df2.id");

        let actual_df = transform.run_lazy(df1, results).unwrap().collect().unwrap();

        let actual_sorted = actual_df
            .sort(["id", "val_1", "val_2"], SortMultipleOptions::default())
            .unwrap();
        let expected_sorted = expected_df
            .sort(["id", "val_1", "val_2"], SortMultipleOptions::default())
            .unwrap();

        assert_eq!(actual_sorted, expected_sorted);
    }
}
