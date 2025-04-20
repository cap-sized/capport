use polars::prelude::*;

const COL_EXPR_DELIMITERS: [char; 3] = ['.', '@', '*'];

pub fn parse_str_to_col_expr(rawexpr: &str) -> Option<Expr> {
    let mut fields: Vec<&str> = rawexpr.split_inclusive(&COL_EXPR_DELIMITERS).rev().collect();
    parse_col_expr(&mut fields, |_: Option<Expr>, arg: &str| Some(col(arg)), None)
}

fn parse_col_expr<F>(fields: &mut Vec<&str>, transformer: F, acc_expr: Option<Expr>) -> Option<Expr>
where
    F: Fn(Option<Expr>, &str) -> Option<Expr>,
{
    if fields.is_empty() || fields.last().unwrap().is_empty() {
        return acc_expr;
    }
    let head = fields.pop().unwrap();
    let next_transform = match head.chars().last().unwrap() {
        // (.xxx : struct.field(xxx)) DONE
        // TODO: Parse expression
        // (@idx : list.get(idx))
        // (*xxx : list.eval(element().struct.field(xxx)))
        '.' => |left: Option<Expr>, next_arg: &str| Some(left.unwrap().struct_().field_by_name(next_arg)),
        _ => |left: Option<Expr>, _: &str| left,
    };
    let field = head
        .strip_suffix(|delim: char| COL_EXPR_DELIMITERS.clone().contains(&delim))
        .unwrap_or(head);
    parse_col_expr(fields, next_transform, transformer(acc_expr, field))
}

#[cfg(test)]
mod tests {

    use polars_lazy::dsl::col;
    use polars_lazy::prelude::Expr;

    use crate::transform::expr::{COL_EXPR_DELIMITERS, parse_col_expr};

    #[test]
    fn parse_col_expr_one_level() {
        let mut fields: Vec<&str> = "args".split_inclusive(&COL_EXPR_DELIMITERS).rev().collect();
        let col_expr = parse_col_expr(&mut fields, |_: Option<Expr>, arg: &str| Some(col(arg)), None);
        // println!("{:?}", col_expr);
        assert_eq!(col_expr.unwrap(), col("args"));
    }

    #[test]
    fn parse_col_expr_dots() {
        let mut fields: Vec<&str> = "args.test.once".split_inclusive(&COL_EXPR_DELIMITERS).rev().collect();
        let col_expr = parse_col_expr(&mut fields, |_: Option<Expr>, arg: &str| Some(col(arg)), None);
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
}
