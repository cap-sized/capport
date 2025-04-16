use std::{collections::HashMap, sync::Mutex};

use polars_lazy::frame::LazyFrame;

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
