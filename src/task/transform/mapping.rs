use polars::prelude::*;
use polars_lazy::prelude::*;
use yaml_rust2::Yaml;

use crate::util::error::{CpResult, PlResult, SubResult};

use super::common::Transform;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MappingField {
    pub label: String,
    pub action: Option<String>,
    pub args: Yaml,
    pub kwargs: Option<Yaml>,
}

const COL_EXPR_DELIMITERS: [char; 3] = ['.', '@', '*'];

pub fn parse_mapping_col_expr<F>(fields: &mut Vec<&str>, transformer: F, acc_expr: Option<Expr>) -> Option<Expr>
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
    return parse_mapping_col_expr(fields, next_transform, transformer(acc_expr, field));
}

impl MappingField {
    pub fn new(label: &str, args: &str) -> MappingField {
        MappingField {
            label: label.to_string(),
            action: None,
            args: Yaml::from_str(args),
            kwargs: None,
        }
    }

    fn expr(&self) -> SubResult<Expr> {
        // (.xxx : struct.field(xxx)) DONE
        // TODO: Parse expression
        // (@idx : list.get(idx))
        // (*xxx : list.eval(element().struct.field(xxx)))
        if self.action.is_none() {
            let rawexpr = match self.args.as_str() {
                Some(ex) => ex.to_string(),
                None => return Err(format!("no string expression found for field {}", self.label)),
            };
            let mut fields: Vec<&str> = rawexpr.split_inclusive(&COL_EXPR_DELIMITERS).rev().collect();
            let col_expr = match parse_mapping_col_expr(&mut fields, |_: Option<Expr>, arg: &str| Some(col(arg)), None)
            {
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

impl MappingTransform {
    pub fn new(label: &str, mappings: Vec<MappingField>) -> MappingTransform {
        MappingTransform {
            label: label.to_string(),
            mappings: mappings,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MappingTransform {
    pub label: String,
    pub mappings: Vec<MappingField>,
}

impl Transform for MappingTransform {
    fn to_lazy_map(&self, df: DataFrame) -> SubResult<LazyFrame> {
        self.map(df.lazy())
    }
    fn map(&self, df: LazyFrame) -> SubResult<LazyFrame> {
        let mut mapping_cols: Vec<Expr> = vec![];
        for mapping in &self.mappings {
            match mapping.expr() {
                Ok(valid_expr) => mapping_cols.push(valid_expr),
                Err(err_msg) => return Err(format!("MappingTransform {}: {}", &self.label, err_msg)),
            }
        }
        Ok(df.select(mapping_cols))
    }
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}: {:?}", &self.label, &self.mappings)
    }
}

#[cfg(test)]
mod tests {
    use polars::{df, docs::lazy};
    use polars_lazy::dsl::col;
    use polars_lazy::prelude::Expr;

    use crate::task::transform::mapping::{COL_EXPR_DELIMITERS, parse_mapping_col_expr};

    use super::{MappingField, MappingTransform, Transform};

    #[test]
    fn parse_col_expr_one_level() {
        let mut fields: Vec<&str> = "args".split_inclusive(&COL_EXPR_DELIMITERS).rev().collect();
        let col_expr = parse_mapping_col_expr(&mut fields, |_: Option<Expr>, arg: &str| Some(col(arg)), None);
        // println!("{:?}", col_expr);
        assert_eq!(col_expr.unwrap(), col("args"));
    }

    #[test]
    fn parse_col_expr_dots() {
        let mut fields: Vec<&str> = "args.test.once".split_inclusive(&COL_EXPR_DELIMITERS).rev().collect();
        let col_expr = parse_mapping_col_expr(&mut fields, |_: Option<Expr>, arg: &str| Some(col(arg)), None);
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
    fn mapping_field_parse() {
        {
            let mapping_field = MappingField::new("test", "args");
            assert_eq!(mapping_field.expr().unwrap(), col("args").alias("test"));
        }
        {
            let mapping_field = MappingField::new("test2", "args.test.once");
            assert_eq!(
                mapping_field.expr().unwrap(),
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
    fn mapping_transform_test() {
        let sample_df = df![
            "Price" => [2.3, 102.023, 19.88],
            "Instr" => ["ABAB", "TORO", "PKJT"],
        ]
        .unwrap();
        let transform = MappingTransform::new(
            "lowercase_full_name",
            vec![
                MappingField::new("price", "Price"),
                MappingField::new("instrument", "Instr"),
            ],
        );
        let actual_df = transform.to_lazy_map(sample_df).unwrap().collect().unwrap();
        // println!("{:?}", actual_df);
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
