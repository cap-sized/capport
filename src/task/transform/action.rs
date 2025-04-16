use polars_lazy::dsl::Expr;
use serde::{Deserialize, Serialize};
use yaml_rust2::Yaml;

use crate::util::error::SubResult;

// fn parse_action(label:&str, action: &str, args: &Yaml, kwargs: Option<&Yaml>) -> SubResult<Action>;

pub trait Action {
    fn expr(&self) -> SubResult<Expr>;
}

#[derive(Serialize, Deserialize)]
pub struct FormatActionArgs {
    pub label: String,
    pub template: String,
    pub args: Vec<String>,
}
