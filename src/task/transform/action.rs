use polars_lazy::dsl::Expr;
use yaml_rust2::Yaml;

use crate::util::error::SubResult;

// fn parse_action(label:&str, action: &str, args: &Yaml, kwargs: Option<&Yaml>) -> SubResult<Action>;

pub trait Action {
    fn expr(&self) -> SubResult<Expr>;
}

pub struct FormatActArgs {
    pub label: String,
    pub template: String,
    pub args: Vec<String>
}


// pub struct ConcatActionArgs {
//     pub method: dyn Fn(&Vec<T>) -> U,
// }

// pub struct ConcatAction<T, U = T> {
//     pub method: dyn Fn(&Vec<T>) -> U,
// }