use std::sync::RwLock;

use polars::prelude::*;
use yaml_rust2::Yaml;

use crate::{
    pipeline::results::PipelineResults,
    util::{
        common::yaml_from_str,
        error::{CpError, CpResult, SubResult},
    },
};

use super::{action::parse_action_to_expr, common::Transform, expr::parse_str_to_col_expr};

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
    fn run_lazy(&self, curr: LazyFrame, _results: Arc<RwLock<PipelineResults<LazyFrame>>>) -> CpResult<LazyFrame> {
        let mut select_cols: Vec<Expr> = vec![];
        for select in &self.selects {
            match select.expr() {
                Ok(valid_expr) => select_cols.push(valid_expr),
                Err(err_msg) => return Err(CpError::ComponentError("SelectTransform", err_msg)),
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
}

impl SelectField {
    pub fn default(label: &str) -> SelectField {
        SelectField {
            label: label.to_string(),
            action: None,
            args: Yaml::Null,
        }
    }

    pub fn new(label: &str, args: &str) -> SelectField {
        SelectField {
            label: label.to_string(),
            action: None,
            args: yaml_from_str(args).unwrap_or(Yaml::Null),
        }
    }

    pub fn new_action(label: &str, action: &str, args: &str) -> SelectField {
        SelectField {
            label: label.to_string(),
            action: Some(action.to_string()),
            args: yaml_from_str(args).unwrap_or(Yaml::Null),
        }
    }

    pub fn from(label: &str, args: &str, action: Option<&str>) -> SelectField {
        SelectField {
            label: label.to_string(),
            action: action.map(|x| x.to_string()),
            args: yaml_from_str(args).unwrap_or_else(|| panic!("[args] invalid yaml to parse: {}", args)),
        }
    }

    pub fn expr(&self) -> SubResult<Expr> {
        if self.action.is_none() {
            let rawexpr = self.args.as_str().unwrap_or(&self.label);
            let col_expr = match parse_str_to_col_expr(rawexpr) {
                Some(x) => x,
                None => return Err(format!("Selected field cannot be parsed to polars Expr: {:?}", rawexpr)),
            };
            Ok(col_expr.alias(&self.label))
        } else {
            parse_action_to_expr(&self.label, self.action.clone().unwrap().as_str(), &self.args)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use polars::df;
    use polars::prelude::{LazyFrame, PlSmallStr};

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
        let results = Arc::new(RwLock::new(PipelineResults::<LazyFrame>::default()));
        let actual_df = transform.run_lazy(sample_df, results).unwrap().collect().unwrap();
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
        let results = Arc::new(RwLock::new(PipelineResults::<LazyFrame>::default()));
        let actual_df = transform.run_lazy(sample_df, results).unwrap().collect().unwrap();
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

    #[test]
    fn valid_select_use_defaults() {
        let sample_df = df![
            "Price" => [2.3, 102.023, 19.88],
            "Ticker" => ["ABAB", "TORO", "PKJT"],
        ]
        .unwrap()
        .lazy();
        let transform = SelectTransform::new(&[SelectField::default("Price"), SelectField::default("Ticker")]);
        let results = Arc::new(RwLock::new(PipelineResults::<LazyFrame>::default()));
        let actual_df = transform.run_lazy(sample_df, results).unwrap().collect().unwrap();
        assert_eq!(
            actual_df,
            df![
                "Price" => [2.3, 102.023, 19.88],
                "Instrument" => ["ABAB", "TORO", "PKJT"],
            ]
            .unwrap()
        );
    }

    #[test]
    fn valid_select_action_format() {
        let sample_df = df![
            "Market" => ["CE", "LA", "VE"],
            "Ticker" => ["ABAB", "TORO", "PKJT"],
        ]
        .unwrap()
        .lazy();
        let transform = SelectTransform::new(&[SelectField::new_action(
            "full",
            "format",
            "{template: \"{}_{}\", cols: [Market, Ticker]}",
        )]);
        let results = Arc::new(RwLock::new(PipelineResults::<LazyFrame>::default()));
        let actual_df = transform.run_lazy(sample_df, results).unwrap().collect().unwrap();
        assert_eq!(
            actual_df,
            df![
                "full" => ["CE_ABAB", "LA_TORO", "VE_PKJT"]
            ]
            .unwrap()
        );
    }

    #[test]
    fn invalid_select_action() {
        let sample_df = df![
            "Market" => ["CE", "LA", "VE"],
            "Ticker" => ["ABAB", "TORO", "PKJT"],
        ]
        .unwrap()
        .lazy();
        let transform = SelectTransform::new(&[SelectField::new_action(
            "full",
            "__format",
            "{template: \"{}_{}\", cols: [Market, Ticker]}",
        )]);
        let results = Arc::new(RwLock::new(PipelineResults::<LazyFrame>::default()));
        assert!(transform.run_lazy(sample_df, results).is_err());
    }
}
