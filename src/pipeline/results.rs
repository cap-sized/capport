use std::{collections::HashMap, sync::Mutex};

use polars_lazy::frame::LazyFrame;

#[derive(Clone)]
pub struct PipelineResults {
    pub dataframes: HashMap<String, LazyFrame>,
}

impl Default for PipelineResults {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineResults {
    pub fn new() -> PipelineResults {
        PipelineResults {
            dataframes: HashMap::new(),
        }
    }
    pub fn get_unchecked(&self, key: &str) -> Option<LazyFrame> {
        self.dataframes.get(key).cloned()
    }
    pub fn insert(&mut self, key: &str, lf: LazyFrame) -> Option<LazyFrame> {
        let old = self.dataframes.remove(key);
        self.dataframes.insert(key.to_owned(), lf);
        old
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
    }
}
