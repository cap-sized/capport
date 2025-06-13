use polars::prelude::{Expr, col};
use serde::Deserialize;

use crate::{
    util::error::{CpError, CpResult},
    valid_or_insert_error,
};

use super::keyword::{Keyword, StrKeyword};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct FilterFields<T> {
    pub include: Vec<StrKeyword>,
    pub into: T,
}

impl<T> FilterFields<T> {
    pub fn emplace(&mut self, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        let mut new_include = vec![];
        for expr_kw in self.include.iter() {
            let mut expr = expr_kw.clone();
            expr.insert_value_from_context(context)?;
            new_include.push(expr);
        }
        self.include = new_include;
        Ok(())
    }
    pub fn validate(&self, errors: &mut Vec<CpError>) {
        for expr_kw in &self.include {
            valid_or_insert_error!(errors, expr_kw, "filter_include");
        }
    }
    pub fn get_include_str(&self) -> Vec<String> {
        let mut includes = vec![];
        for expr_kw in &self.include {
            includes.push(expr_kw.value().map(|x| x.to_owned()).expect("filter_include"));
        }
        includes
    }
    pub fn get_include_expr(&self) -> Vec<Expr> {
        let mut includes = vec![];
        for expr_kw in &self.include {
            includes.push(expr_kw.value().map(col).expect("filter_include"));
        }
        includes
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::{
        dtype::DType,
        keyword::{Keyword, StrKeyword},
    };

    use super::FilterFields;

    #[test]
    fn valid_filter_fields() {
        let config = "
include: [test, $one, two]
into: uint64
            ";

        let expected = FilterFields::<DType> {
            include: vec![
                StrKeyword::with_value("test".into()),
                StrKeyword::with_symbol("one"),
                StrKeyword::with_value("two".into()),
            ],
            into: DType(polars::prelude::DataType::UInt64),
        };
        let actual = serde_yaml_ng::from_str::<FilterFields<DType>>(config).unwrap();
        assert_eq!(expected, actual);
    }
    #[test]
    fn invalid_filter_fields_wrong_type() {
        let config = "
include: [test, $one, two]
into: $different
            ";

        assert!(serde_yaml_ng::from_str::<FilterFields::<DType>>(config).is_err());
        assert!(serde_yaml_ng::from_str::<FilterFields::<StrKeyword>>(config).is_ok());
    }
}
