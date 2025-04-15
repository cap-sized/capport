use polars::prelude::*;
use polars_lazy::prelude::*;
use yaml_rust2::Yaml;

use crate::{
    config::parser::common::YamlRead, pipeline::results::PipelineResults, util::{
        common::yaml_from_str,
        error::{CpResult, PlResult, SubResult},
    }
};

use super::common::Transform;

const COL_EXPR_DELIMITERS: [char; 3] = ['.', '@', '*'];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectTransform {
    pub selects: Vec<SelectField>,
}

impl SelectTransform {
    pub const fn keyword() -> &'static str {
        "select"
    }
    pub fn new(selects: Vec<SelectField>) -> SelectTransform {
        SelectTransform { selects }
    }
}

impl Transform for SelectTransform {
    fn run(&self, curr: LazyFrame, results: &PipelineResults) -> SubResult<LazyFrame> {
        let mut select_cols: Vec<Expr> = vec![];
        for select in &self.selects {
            match select.expr() {
                Ok(valid_expr) => select_cols.push(valid_expr),
                Err(err_msg) => return Err(format!("SelectTransform: {}", err_msg)),
            }
        }
        Ok(curr.select(select_cols))
    }
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", &self.selects)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectField {
    pub label: String,
    pub action: Option<String>,
    pub args: Yaml,
    pub kwargs: Option<Yaml>,
}

impl SelectField {
    pub fn new(label: &str, args: &str) -> SelectField {
        SelectField {
            label: label.to_string(),
            action: None,
            args: yaml_from_str(args).expect("Invalid yaml"),
            kwargs: None,
        }
    }

    pub fn from(label: &str, args: &str, action: Option<&str>, kwargs: Option<&str>) -> SelectField {
        SelectField {
            label: label.to_string(),
            action: action.map(|x| x.to_string()),
            args: yaml_from_str(args).unwrap_or_else(|| panic!("[args] invalid yaml to parse: {}", args)),
            kwargs: match kwargs {
                Some(x) => yaml_from_str(x),
                None => None,
            },
        }
    }

    pub fn expr(&self) -> SubResult<Expr> {
        // (.xxx : struct.field(xxx)) DONE
        // TODO: Parse expression
        // (@idx : list.get(idx))
        // (*xxx : list.eval(element().struct.field(xxx)))
        if self.action.is_none() {
            let rawexpr = self
                .args
                .to_str(format!("no string expression found for field {}", self.label))?;
            let mut fields: Vec<&str> = rawexpr.split_inclusive(&COL_EXPR_DELIMITERS).rev().collect();
            let col_expr = match parse_select_col_expr(&mut fields, |_: Option<Expr>, arg: &str| Some(col(arg)), None) {
                Some(expr) => expr,
                None => {
                    return Err(format!(
                        "invalid expression found for field {}: {}",
                        self.label, rawexpr
                    ));
                }
            };
            return Ok(col_expr.alias(&self.label));
        }
        Err(format!("Alternate action '{:?}' not yet supported", &self.action))
    }
}

pub fn parse_select_col_expr<F>(fields: &mut Vec<&str>, transformer: F, acc_expr: Option<Expr>) -> Option<Expr>
where
    F: Fn(Option<Expr>, &str) -> Option<Expr>,
{
    if fields.is_empty() || fields.last().unwrap().is_empty() {
        return acc_expr;
    }
    let head = fields.pop().unwrap();
    let next_transform = match head.chars().last().unwrap() {
        '.' => |left: Option<Expr>, next_arg: &str| Some(left.unwrap().struct_().field_by_name(next_arg)),
        unknown => |left: Option<Expr>, _: &str| left,
    };
    let field = head
        .strip_suffix(|delim: char| COL_EXPR_DELIMITERS.clone().contains(&delim))
        .unwrap_or(head);
    parse_select_col_expr(fields, next_transform, transformer(acc_expr, field))
}

#[cfg(test)]
mod tests {
    use polars::{df, docs::lazy};
    use polars_lazy::{dsl::col, frame::IntoLazy};
    use polars_lazy::prelude::Expr;

    use crate::{pipeline::results::PipelineResults, task::transform::select::{parse_select_col_expr, COL_EXPR_DELIMITERS}};

    use super::{SelectField, SelectTransform, Transform};

    #[test]
    fn parse_col_expr_one_level() {
        let mut fields: Vec<&str> = "args".split_inclusive(&COL_EXPR_DELIMITERS).rev().collect();
        let col_expr = parse_select_col_expr(&mut fields, |_: Option<Expr>, arg: &str| Some(col(arg)), None);
        // println!("{:?}", col_expr);
        assert_eq!(col_expr.unwrap(), col("args"));
    }

    #[test]
    fn parse_col_expr_dots() {
        let mut fields: Vec<&str> = "args.test.once".split_inclusive(&COL_EXPR_DELIMITERS).rev().collect();
        let col_expr = parse_select_col_expr(&mut fields, |_: Option<Expr>, arg: &str| Some(col(arg)), None);
        // println!("{:?}", col_expr);
        assert_eq!(
            col_expr.unwrap(),
            col("args")
                .struct_()
                .field_by_name("test")
                .struct_()
                .field_by_name("once")
        );
    }

    #[test]
    fn select_field_parse() {
        {
            let select_field = SelectField::new("test", "args");
            assert_eq!(select_field.expr().unwrap(), col("args").alias("test"));
        }
        {
            let select_field = SelectField::new("test2", "args.test.once");
            assert_eq!(
                select_field.expr().unwrap(),
                col("args")
                    .struct_()
                    .field_by_name("test")
                    .struct_()
                    .field_by_name("once")
                    .alias("test2")
            );
        }
    }

    #[test]
    fn select_transform_test() {
        let sample_df = df![
            "Price" => [2.3, 102.023, 19.88],
            "Instr" => ["ABAB", "TORO", "PKJT"],
        ]
        .unwrap().lazy();
        let transform = SelectTransform::new(vec![
            SelectField::new("price", "Price"),
            SelectField::new("instrument", "Instr"),
        ]);
        let results = PipelineResults::new();
        let actual_df = transform.run(sample_df, &results).unwrap().collect().unwrap();
        assert_eq!(
            actual_df,
            df![
                "price" => [2.3, 102.023, 19.88],
                "instrument" => ["ABAB", "TORO", "PKJT"],
            ]
            .unwrap()
        );
    }
}
