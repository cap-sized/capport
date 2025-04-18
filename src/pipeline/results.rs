use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    sync::Mutex,
};

use polars::frame::DataFrame;
use polars_lazy::frame::LazyFrame;

use crate::util::error::{CpError, CpResult, SubResult};

#[derive(Clone)]
pub struct PipelineResults {
    pub lazyframes: HashMap<String, LazyFrame>,
}

impl Debug for PipelineResults {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.to_dataframes())
    }
}

impl PartialEq for PipelineResults {
    fn eq(&self, other: &Self) -> bool {
        self.to_dataframes().unwrap() == other.to_dataframes().unwrap()
    }
}

impl Default for PipelineResults {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineResults {
    pub fn new() -> PipelineResults {
        PipelineResults {
            lazyframes: HashMap::new(),
        }
    }
    pub fn get_unchecked(&self, key: &str) -> Option<LazyFrame> {
        self.lazyframes.get(key).cloned()
    }
    pub fn insert(&mut self, key: &str, lf: LazyFrame) -> Option<LazyFrame> {
        let old = self.lazyframes.remove(key);
        self.lazyframes.insert(key.to_owned(), lf);
        old
    }
    pub fn to_dataframes(&self) -> CpResult<HashMap<String, DataFrame>> {
        let mut dataframes = HashMap::new();
        for (key, lf) in &self.lazyframes {
            match lf.clone().collect() {
                Ok(x) => dataframes.insert(key.clone(), x),
                Err(e) => return Err(CpError::TableError(key.clone(), e)),
            };
        }
        Ok(dataframes)
    }
}

#[cfg(test)]
mod tests {
    use crate::util::common::DummyData;

    use super::PipelineResults;

    #[test]
    fn valid_pipeline_results_insert() {
        let mut results = PipelineResults::default();
        assert!(results.insert("foo", DummyData::player_data()).is_none());
        assert_eq!(
            results
                .insert("foo", DummyData::player_scores())
                .unwrap()
                .collect()
                .unwrap(),
            DummyData::player_data().collect().unwrap()
        );
        println!("{:?}", results);
    }
}
