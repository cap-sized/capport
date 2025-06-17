use polars::prelude::{DataType, Expr, concat_str, format_str};
use serde::Deserialize;

use crate::{
    parser::keyword::Keyword,
    util::error::{CpError, CpResult},
};

use super::keyword::{PolarsExprKeyword, StrKeyword};

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
