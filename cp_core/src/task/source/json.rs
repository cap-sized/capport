use crate::{parser::keyword::Keyword, util::error::CpError};

use super::{
    common::{Source, SourceConfig},
    config::JsonSourceConfig,
};

impl SourceConfig for JsonSourceConfig {
    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        match self.filepath.value() {
            Some(_) => {}
            None => errors.push(CpError::SymbolMissingValueError(
                "filepath",
                self.filepath.symbol().unwrap_or("?").to_owned(),
            )),
        }
        match self.output.value() {
            Some(_) => {}
            None => errors.push(CpError::SymbolMissingValueError(
                "output",
                self.output.symbol().unwrap_or("?").to_owned(),
            )),
        };
        errors
    }

    // TODO: Add model registry
    fn transform(&self) -> Box<dyn Source> {
        todo!()
    }
}
