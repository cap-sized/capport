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
        let subexprs = self.cols.iter().map(|x| parse_str_to_col_expr(x)).collect::<Vec<_>>();
        let mut args = vec![];
        for subexpr in subexprs {
            match subexpr {
                Some(x) => args.push(x),
                None => return Err(format!("Invalid subexpression found amongst cols: {:?}", &self.cols)),
            }
        }

        match format_str(&self.template, &args) {
            Ok(x) => Ok(x),
            Err(e) => return Err(format!("Invalid formatting: {:?}", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;

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
}
