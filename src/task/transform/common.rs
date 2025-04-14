use std::fmt;

use polars::prelude::*;
use polars_lazy::prelude::*;
use yaml_rust2::Yaml;

use crate::util::error::{CpResult, PlResult, SubResult};

pub trait Transform {
    fn to_lazy_map(&self, df: DataFrame) -> SubResult<LazyFrame>;
    fn map(&self, df: LazyFrame) -> SubResult<LazyFrame>;
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result;
}
