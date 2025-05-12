use polars::prelude::{col, concat_str, format_str, Expr};

use crate::{task::config::Keyword, util::error::{CpError, CpResult}};

use super::config::{ConcatActionConfig, FormatActionConfig};

pub trait TransformAction {
    fn expr(&self) -> CpResult<polars::prelude::Expr>;
    fn validate(&self) -> CpResult<()>;
}

impl TransformAction for ConcatActionConfig {
    fn validate(&self) -> CpResult<()> {
        if self.separator.value().is_none() {
            return Err(CpError::TaskError("ConcatActionConfig.separator not materialized", format!("symbol not replaced: {:?}", self.separator.symbol())));
        }
        for x in &self.columns {
            if x.value().is_none() {
                return Err(CpError::TaskError("ConcatActionConfig.columns[?] not materialized", format!("symbol not replaced: {:?}", x.symbol())));
            }
        }
        Ok(())
    }
    fn expr(&self) -> CpResult<polars::prelude::Expr> {
        self.validate()?;
        if self.columns.is_empty() {
            return Err(CpError::TaskError("`ConcatAction` TransformAction parsing failed", "no columns to concat".to_owned()));
        }
        let args: Vec<Expr> = self
            .columns
            .iter()
            .map(|x| col(x.value().unwrap().to_owned()))
            .collect::<Vec<Expr>>();

        Ok(concat_str(&args, self.separator.value().unwrap(), self.ignore_nulls.unwrap_or(false)))
    }
}

impl TransformAction for FormatActionConfig {
    fn validate(&self) -> CpResult<()> {
        if self.template.value().is_none() {
            return Err(CpError::TaskError("FormatActionConfig.template not materialized", format!("symbol not replaced: {:?}", self.template.symbol())));
        }
        for x in &self.columns {
            if x.value().is_none() {
                return Err(CpError::TaskError("FormatActionConfig.columns[?] not materialized", format!("symbol not replaced: {:?}", x.symbol())));
            }
        }
        Ok(())
    }
    fn expr(&self) -> CpResult<polars::prelude::Expr> {
        self.validate()?;
        if self.columns.is_empty() {
            return Err(CpError::TaskError("`FormatAction` TransformAction parsing failed", "no columns to concat".to_owned()));
        }
        let args: Vec<Expr> = self
            .columns
            .iter()
            .map(|x| col(x.value().unwrap().to_owned()))
            .collect::<Vec<Expr>>();
        Ok(format_str(self.template.value().unwrap(), args)?)
    }
}
