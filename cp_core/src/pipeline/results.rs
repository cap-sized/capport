use std::collections::HashMap;

use crate::frame::common::NamedSizedResult;


/// A simple results holder. Should never be actively passed around during execution. 
/// Only holds the results that have their listeners/broadcasters distributed at startup
pub struct PipelineResults<T> {
    results: HashMap<String, T>,
}

impl<'a, T> Default for PipelineResults<T>
where T: NamedSizedResult
 {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, T> PipelineResults<T> where T: NamedSizedResult {
    pub fn from(results: HashMap<String, T>) -> PipelineResults<T> {
        PipelineResults { results }
    }

    pub fn new() -> PipelineResults<T> {
        PipelineResults::from(HashMap::new())
    }
    pub fn keys(&self) -> Vec<String> {
        self.results.keys().map(|x| x.to_owned()).collect::<Vec<_>>()
    }
    pub fn get(&'a self, label: &str) -> Option<&'a T> {
        self.results.get(label)
    }
    pub fn insert(&'a mut self, label: &str, bufsize: usize) -> Option<&'a T> {
        self.results.insert(label.to_owned(), T::new(label, bufsize));
        self.results.get(label)
    }
}

#[cfg(test)]
mod tests {
    use crate::{frame::common::NamedSizedResult, pipeline::results::PipelineResults};

    #[test]
    fn get_insert_basic_noop_result() {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct Noop { label: String, bufsize: usize }
        impl NamedSizedResult for Noop {
            fn label(&self) -> &str {
                self.label.as_str()
            }
            fn new(label: &str, bufsize: usize) -> Self {
                Self { label: label.to_owned(), bufsize }
            }
        }
        let mut res = PipelineResults::<Noop>::new();
        assert_eq!(res.get("a"), None);
        let exp1 = Noop::new("a", 10);
        assert_eq!(res.insert("a", 10), Some(exp1.clone()).as_ref());
        assert_eq!(res.get("a"), Some(exp1.clone()).as_ref());
    }

}
