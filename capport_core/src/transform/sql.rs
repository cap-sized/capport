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
    fn run_lazy(&self, curr: LazyFrame, _results: Arc<RwLock<PipelineResults<LazyFrame>>>) -> CpResult<LazyFrame> {
        let mut context = SQLContext::new();

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
}
