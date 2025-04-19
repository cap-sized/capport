use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    sync::Mutex,
};

use polars::frame::DataFrame;
use polars_lazy::frame::LazyFrame;

use crate::util::error::{CpError, CpResult, SubResult};

#[derive(Clone)]
pub struct PipelineResults<ResultType> {
    pub results: HashMap<String, ResultType>,
}

impl Debug for PipelineResults<LazyFrame> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.to_dataframes())
    }
}

impl PartialEq for PipelineResults<LazyFrame> {
    fn eq(&self, other: &Self) -> bool {
        self.to_dataframes().unwrap() == other.to_dataframes().unwrap()
    }
}

impl<T> Default for PipelineResults<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> PipelineResults<T> {
    pub fn new() -> PipelineResults<T> {
        PipelineResults {
            results: HashMap::new(),
        }
    }
}

impl PipelineResults<LazyFrame> {
    pub fn to_dataframes(&self) -> CpResult<HashMap<String, DataFrame>> {
        let mut dataframes = HashMap::new();
        for (key, lf) in &self.results {
            match lf.clone().collect() {
                Ok(x) => dataframes.insert(key.clone(), x),
                Err(e) => return Err(CpError::TableError(e)),
            };
        }
        Ok(dataframes)
    }
}

impl<T: Clone> PipelineResults<T> {
    pub fn get_unchecked(&self, key: &str) -> Option<T> {
        self.results.get(key).cloned()
    }
    pub fn insert(&mut self, key: &str, lf: T) -> Option<T> {
        let old = self.results.remove(key);
        self.results.insert(key.to_owned(), lf);
        old
    }
}

#[cfg(test)]
mod tests {
    use crate::util::common::DummyData;
    use polars_lazy::frame::LazyFrame;

    use super::PipelineResults;

    #[test]
    fn valid_pipeline_results_insert() {
        let mut results = PipelineResults::<LazyFrame>::default();
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

    #[test]
    fn valid_pipeline_results_insert_any() {
        let mut results = PipelineResults::<u8>::default();
        assert!(results.insert("foo", 8).is_none());
        assert_eq!(results.insert("foo", 8).unwrap(), 8);
    }
}
