use crate::parser::dtype::DType;
use crate::pipeline::context::DefaultPipelineContext;
use crate::task::transform::common::{Transform, TransformConfig};
use crate::util::error::{CpError, CpResult};
use polars::prelude::{DataType, Expr, LazyFrame};
use serde_yaml_ng::Mapping;
use std::sync::Arc;

use super::config::CastConfig;

pub struct CastTransform {
    include: Vec<Expr>,
}

impl Transform for CastTransform {
    fn run(&self, main: LazyFrame, _ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        Ok(main.with_columns(self.include.clone()))
    }
}

impl TransformConfig for CastConfig {
    fn emplace(&mut self, context: &Mapping) -> CpResult<()> {
        self.cast.emplace(context)
    }

    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        self.cast.validate(&mut errors);
        errors
    }

    fn transform(&self) -> Box<dyn Transform> {
        let dtype = self.cast.into.clone().unwrap_or(DType(DataType::UInt64));
        let include = self
            .cast
            .get_include_expr()
            .into_iter()
            .map(|x| x.cast(dtype.0.clone()))
            .collect();
        Box::new(CastTransform { include })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use polars::{
        df,
        prelude::{DataType, IntoLazy, col},
    };

    use crate::{
        pipeline::context::DefaultPipelineContext,
        task::transform::{common::TransformConfig, config::CastConfig},
    };

    #[test]
    fn valid_uniform_convert() {
        let main = df!(
            "id" => 0..100,
        )
        .unwrap()
        .lazy();
        let config: CastConfig = serde_yaml_ng::from_str(r#"cast: {include: [id]}"#).unwrap();
        let errors = config.validate();
        assert!(errors.is_empty());
        let node = config.transform();
        let ctx = Arc::new(DefaultPipelineContext::new());
        let result = node.run(main.clone(), ctx);
        let actual = result.unwrap().collect().unwrap();
        let expected = main.with_columns([col("id").cast(DataType::UInt64)]).collect().unwrap();
        assert_eq!(actual, expected);
    }
}
