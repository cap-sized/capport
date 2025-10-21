use polars::prelude::{DataType, Expr, concat_str, format_str};
use serde::Deserialize;

use crate::{
    parser::keyword::Keyword,
    util::error::{CpError, CpResult},
};

use super::{dtype::DType, keyword::{PolarsExprKeyword, StrKeyword}};

pub trait ExprAction {
    fn expr(&self) -> CpResult<polars::prelude::Expr>;
    fn validate(&self) -> CpResult<()>;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct FormatAction {
    pub template: StrKeyword,
    pub columns: Vec<PolarsExprKeyword>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ConcatAction {
    pub separator: StrKeyword,
    pub columns: Vec<PolarsExprKeyword>,
    pub ignore_nulls: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CastAction {
    pub dtype: DType,
    pub column: PolarsExprKeyword,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct IsEqAction {
    pub left: PolarsExprKeyword,
    pub right: PolarsExprKeyword,
}

impl ExprAction for ConcatAction {
    fn validate(&self) -> CpResult<()> {
        if self.separator.value().is_none() {
            return Err(CpError::TaskError(
                "ConcatActionConfig.separator not materialized",
                format!("symbol not replaced: {:?}", self.separator.symbol()),
            ));
        }
        for x in &self.columns {
            if x.value().is_none() {
                return Err(CpError::TaskError(
                    "ConcatActionConfig.columns[?] not materialized",
                    format!("symbol not replaced: {:?}", x.symbol()),
                ));
            }
        }
        Ok(())
    }
    fn expr(&self) -> CpResult<polars::prelude::Expr> {
        self.validate()?;
        if self.columns.is_empty() {
            return Err(CpError::TaskError(
                "`ConcatAction` TransformAction parsing failed",
                "no columns to concat".to_owned(),
            ));
        }
        let args: Vec<Expr> = self
            .columns
            .iter()
            .map(|x| x.value())
            .filter(|x| x.is_some())
            .map(|x| x.unwrap().to_owned().cast(DataType::String))
            .collect::<Vec<Expr>>();

        Ok(concat_str(
            &args,
            self.separator.value().unwrap(),
            self.ignore_nulls.unwrap_or(false),
        ))
    }
}

impl ExprAction for FormatAction {
    fn validate(&self) -> CpResult<()> {
        if self.template.value().is_none() {
            return Err(CpError::TaskError(
                "FormatActionConfig.template not materialized",
                format!("symbol not replaced: {:?}", self.template.symbol()),
            ));
        }
        for x in &self.columns {
            if x.value().is_none() {
                return Err(CpError::TaskError(
                    "FormatActionConfig.columns[?] not materialized",
                    format!("symbol not replaced: {:?}", x.symbol()),
                ));
            }
        }
        Ok(())
    }
    fn expr(&self) -> CpResult<polars::prelude::Expr> {
        self.validate()?;
        if self.columns.is_empty() {
            return Err(CpError::TaskError(
                "`FormatAction` TransformAction parsing failed",
                "no columns to concat".to_owned(),
            ));
        }
        let args: Vec<Expr> = self
            .columns
            .iter()
            .map(|x| x.value())
            .filter(|x| x.is_some())
            .map(|x| x.unwrap().to_owned().cast(DataType::String))
            .collect::<Vec<Expr>>();
        Ok(format_str(self.template.value().unwrap(), args)?)
    }
}

impl ExprAction for CastAction {
    fn validate(&self) -> CpResult<()> {
        if self.column.value().is_none() {
            return Err(CpError::TaskError(
                "CastActionConfig.column not materialized",
                format!("symbol not replaced: {:?}", self.column.symbol()),
            ));
        }
        Ok(())
    }
    fn expr(&self) -> CpResult<polars::prelude::Expr> {
        self.validate()?;
        Ok(self.column.value().unwrap().clone().cast(self.dtype.clone().0))
    }
}

impl ExprAction for IsEqAction {
    fn validate(&self) -> CpResult<()> {
        if self.left.value().is_none() {
            return Err(CpError::TaskError(
                "CastActionConfig.left not materialized",
                format!("symbol not replaced: {:?}", self.left.symbol()),
            ));
        }
        if self.right.value().is_none() {
            return Err(CpError::TaskError(
                "CastActionConfig.right not materialized",
                format!("symbol not replaced: {:?}", self.right.symbol()),
            ));
        }
        Ok(())
    }
    fn expr(&self) -> CpResult<polars::prelude::Expr> {
        self.validate()?;
        Ok(self.left.value().unwrap().clone().eq(self.right.value().unwrap().clone()))
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::col;

    use crate::parser::{action::{CastAction, ConcatAction, FormatAction}, dtype::DType, keyword::{Keyword, PolarsExprKeyword, StrKeyword}};

    use super::{IsEqAction, ExprAction};

    fn value_pl_kw() ->PolarsExprKeyword {
        PolarsExprKeyword::with_value(col("demo"))
    }

    fn c(val: &str) ->PolarsExprKeyword {
        PolarsExprKeyword::with_value(col(val))
    }

    fn s(val: &str) -> StrKeyword {
        StrKeyword::with_value(val.to_string())
    }

    fn empty_pl_kw() -> PolarsExprKeyword{
        PolarsExprKeyword::with_symbol("nope")
    }

    fn empty_s_kw() -> StrKeyword{
        StrKeyword::with_symbol("nope")
    }

    fn sample_dtype() -> DType { 
        DType(polars::prelude::DataType::Int8)
    }

    #[test]
    fn invalid_action_is_eq() {
        assert!((IsEqAction { left: value_pl_kw(), right: empty_pl_kw() }).validate().is_err());
        assert!((IsEqAction { left: empty_pl_kw(), right: value_pl_kw() }).validate().is_err());
    }

    #[test]
    fn invalid_action_cast() {
        assert!((CastAction { column: empty_pl_kw(), dtype: sample_dtype() }).validate().is_err());
    }

    #[test]
    fn invalid_action_format() {
        assert!((FormatAction { template: empty_s_kw(), columns: vec![ c("y"), c("e"), c("e"), c("t")] }).validate().is_err());
        assert!((FormatAction { template: s("okay"), columns: vec![ c("y"), empty_pl_kw(), c("e"), c("t")] }).validate().is_err());
        assert!((FormatAction { template: empty_s_kw(), columns: vec![] }).expr().is_err());
    }

    #[test]
    fn invalid_action_concat() {
        assert!((ConcatAction { separator: empty_s_kw(), columns: vec![ c("y"), c("e"), c("e"), c("t")] , ignore_nulls: Option::None}).validate().is_err());
        assert!((ConcatAction { separator: s(";"), columns: vec![ c("y"), empty_pl_kw(), c("e"), c("t")], ignore_nulls: Option::Some(true)}).validate().is_err());
        assert!((ConcatAction { separator: empty_s_kw(), columns: vec![] , ignore_nulls: Option::None}).expr().is_err());
    }
}
