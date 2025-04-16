use polars::prelude::*;
use polars_lazy::prelude::*;
use yaml_rust2::Yaml;

use crate::{
    config::parser::common::YamlRead,
    pipeline::results::PipelineResults,
    util::{
        common::yaml_from_str,
        error::{CpResult, PlResult, SubResult},
    },
};

use super::{common::Transform, expr::parse_str_to_col_expr};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectTransform {
    pub selects: Vec<SelectField>,
}

impl SelectTransform {
    pub const fn keyword() -> &'static str {
        "select"
    }
    pub fn new(selects: &[SelectField]) -> SelectTransform {
        SelectTransform {
            selects: selects.to_vec(),
        }
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
        if self.action.is_none() {
            let rawexpr = self
                .args
                .to_str(format!("no string expression found for field {}", self.label))?;
            let col_expr = match parse_str_to_col_expr(&rawexpr) {
                Some(x) => x,
                None => return Err(format!("Selected field cannot be parsed to polars Expr: {:?}", rawexpr)),
            };
            return Ok(col_expr.alias(&self.label));
        }
        Err(format!("Alternate action '{:?}' not yet supported", &self.action))
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::PlSmallStr;
    use polars::{df, docs::lazy};
    use polars_lazy::prelude::Expr;
    use polars_lazy::{dsl::col, frame::IntoLazy};

    use crate::pipeline::results::PipelineResults;

    use super::{SelectField, SelectTransform, Transform};

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
    fn valid_select_basic() {
        let sample_df = df![
            "Price" => [2.3, 102.023, 19.88],
            "Ticker" => ["ABAB", "TORO", "PKJT"],
        ]
        .unwrap()
        .lazy();
        let transform = SelectTransform::new(&[
            SelectField::new("price", "Price"),
            SelectField::new("instrument", "Ticker"),
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

    #[test]
    fn valid_select_nested() {
        let sample_df = df![
            "Instrument" => df![
                "Price" => [2.3, 102.023, 19.88],
                "Ticker" => ["ABAB", "TORO", "PKJT"],
            ].unwrap().into_struct(PlSmallStr::from_str("Instrument")),
            "Position" => [50, 2, -71],
        ]
        .unwrap()
        .lazy();
        let transform = SelectTransform::new(&[
            SelectField::new("instrument", "Instrument.Ticker"),
            SelectField::new("price", "Instrument.Price"),
            SelectField::new("position", "Position"),
        ]);
        let results = PipelineResults::new();
        let actual_df = transform.run(sample_df, &results).unwrap().collect().unwrap();
        assert_eq!(
            actual_df,
            df![
                "instrument" => ["ABAB", "TORO", "PKJT"],
                "price" => [2.3, 102.023, 19.88],
                "position" => [50, 2, -71],
            ]
            .unwrap()
        );
    }
}
