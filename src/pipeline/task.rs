use std::collections::HashMap;

use crate::util::common::CpDefault;

use super::common::PipelineTask;

pub type TaskDictionary = HashMap<String, PipelineTask>;

impl CpDefault for TaskDictionary {
    fn get_default() -> Self {
        let dictionary = HashMap::new();
        // let dictionary = HashMap::from([]);
        dictionary
    }
}
