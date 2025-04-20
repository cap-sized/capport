use polars::prelude::*;
use serde::{Deserialize, Serialize};
use yaml_rust2::Yaml;

use crate::util::{common::yaml_to_str, error::SubResult};

use super::expr::parse_str_to_col_expr;

pub fn parse_action_to_expr(label: &str, action: &str, args: &Yaml) -> SubResult<Expr> {
    let yaml_str = yaml_to_str(args)?;
    match action {
        "format" => match serde_yaml_ng::from_str::<FormatActionArgs>(&yaml_str) {
            Ok(x) => x.expr().map(|x| x.alias(label)),
            Err(e) => Err(format!("Bad args for FormatActionArgs: {:?}", e)),
        },
        _ => Err(format!("Unrecognized action `{}`", action)),
    }
}

pub trait Action {
    fn expr(&self) -> SubResult<Expr>;
}

fn parse_vecstr_to_veccol(vecstr: &Vec<String>) -> SubResult<Vec<Expr>> {
    let subexprs = vecstr.iter().map(|x| parse_str_to_col_expr(x)).collect::<Vec<_>>();
    let mut args = vec![];
    for subexpr in subexprs {
        match subexpr {
            Some(x) => args.push(x),
            None => return Err(format!("Invalid subexpression found amongst cols: {:?}", &vecstr)),
        }
    }
    Ok(args)
}

#[derive(Serialize, Deserialize)]
pub struct ConcatActionArgs {
    pub separator: String,
    pub cols: Vec<String>,
}

impl ConcatActionArgs {
    pub fn new(separator: &str, cols: &[&str]) -> Self {
        Self {
            separator: separator.to_string(),
            cols: cols.iter().map(|x| x.to_owned().to_owned()).collect(),
        }
    }
}

impl Action for ConcatActionArgs {
    fn expr(&self) -> SubResult<Expr> {
        if self.cols.is_empty() {
            return Err("Invalid ConcatAction: no cols found to concat".to_owned());
        }
        let args = parse_vecstr_to_veccol(&self.cols)?;
        let template = self
            .cols
            .iter()
            .map(|_| "{}".to_owned())
            .collect::<Vec<String>>()
            .join(&self.separator);
        match format_str(&template, &args) {
            Ok(x) => Ok(x),
            Err(e) => Err(format!("Invalid concat: {:?}", e)),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct FormatActionArgs {
    pub template: String,
    pub cols: Vec<String>,
}

impl FormatActionArgs {
    pub fn new(template: &str, cols: &[&str]) -> Self {
        Self {
            template: template.to_string(),
            cols: cols.iter().map(|x| x.to_owned().to_owned()).collect(),
        }
    }
}

impl Action for FormatActionArgs {
    fn expr(&self) -> SubResult<Expr> {
        let args = parse_vecstr_to_veccol(&self.cols)?;
        match format_str(&self.template, &args) {
            Ok(x) => Ok(x),
            Err(e) => Err(format!("Invalid formatting: {:?}", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    use crate::transform::action::ConcatActionArgs;

    use super::{Action, FormatActionArgs};

    #[test]
    fn valid_format_action() {
        let faargs = FormatActionArgs::new("test-{}-{}", &["one", "two"]);
        let actual = faargs.expr().unwrap();
        let expected = format_str("test-{}-{}", [col("one"), col("two")]).unwrap();
        assert_eq!(actual, expected);
        let frame = df![
            "one" => [1.0, 1.1, 1.2],
            "two" => [2.0, 2.1, 2.2],
        ]
        .unwrap();
        let sel_cols = vec![actual.alias("label")];
        let final_frame = frame.lazy().select(sel_cols).collect().unwrap();
        assert_eq!(
            final_frame,
            df![
                "label" => [
                    "test-1.0-2.0",
                    "test-1.1-2.1",
                    "test-1.2-2.2",
                ]
            ]
            .unwrap()
        )
    }

    #[test]
    fn invalid_format_action_missing_blanks() {
        let faargs = FormatActionArgs::new("test-{}-", &["one", "two"]);
        faargs.expr().unwrap_err();
    }

    #[test]
    fn valid_concat_action() {
        let caargs = ConcatActionArgs::new(".", &["a", "b", "c"]);
        let actual = caargs.expr().unwrap();
        let expected = format_str("{}.{}.{}", [col("a"), col("b"), col("c")]).unwrap();
        assert_eq!(actual, expected);
        let frame = df![
        "a" => ["a", "a"],
        "b" => [13.0, 1.3],
        "c" => [true, true]
        ]
        .unwrap();
        let sel_cols = vec![actual.alias("label")];
        let final_frame = frame.lazy().select(sel_cols).collect().unwrap();
        assert_eq!(
            final_frame,
            df![
                "label" => [
                    "a.13.0.true",
                    "a.1.3.true",
                ]
            ]
            .unwrap()
        )
    }

    #[test]
    fn invalid_concat_action_empty_args() {
        let caargs = ConcatActionArgs::new("_", &[]);
        caargs.expr().unwrap_err();
    }
}
