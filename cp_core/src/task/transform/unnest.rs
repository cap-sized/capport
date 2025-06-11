use std::sync::Arc;

use polars::prelude::{Expr, LazyFrame};

use crate::{parser::keyword::Keyword, pipeline::context::DefaultPipelineContext, util::error::{CpError, CpResult}, valid_or_insert_error};

use super::{common::{Transform, TransformConfig}, config::UnnestTransformConfig};


pub struct UnnestTransform {
    col_list: Option<Expr>,
    col_struct: Option<Expr>,
    col_list_of_struct: Option<Expr>
}

impl Transform for UnnestTransform {
    fn run(&self, main: LazyFrame, _ctx: Arc<DefaultPipelineContext>) -> CpResult<LazyFrame> {
        let main = if let Some(expr) = self.col_list.clone() { main.explode([expr]) } else { main };
        let main = if let Some(expr) = self.col_struct.clone() { main.unnest([expr]) } else { main };
        let main = if let Some(expr) = self.col_list_of_struct.clone() { main.explode([expr.clone()]).unnest([expr]) } else { main };
        Ok(main)
    }
}

impl TransformConfig for UnnestTransformConfig {
    fn emplace(&mut self, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        if let Some(mut unnest_list) = self.unnest_list.take() {
            unnest_list.insert_value_from_context(context)?;
            let _ = self.unnest_list.insert(unnest_list);
        }
        if let Some(mut unnest_struct) = self.unnest_struct.take() {
            unnest_struct.insert_value_from_context(context)?;
            let _ = self.unnest_struct.insert(unnest_struct);
        }
        if let Some(mut unnest_list_of_struct) = self.unnest_list_of_struct.take() {
            unnest_list_of_struct.insert_value_from_context(context)?;
            let _ = self.unnest_list_of_struct.insert(unnest_list_of_struct);
        }
        Ok(())
    }

    fn validate(&self) -> Vec<CpError> {
        let mut errors = vec![];
        if let Some(unnest_list) = &self.unnest_list {
            valid_or_insert_error!(errors, unnest_list, "transform[unnest_list]");
        }
        if let Some(unnest_struct) = &self.unnest_struct {
            valid_or_insert_error!(errors, unnest_struct, "transform[unnest_struct]");
        }
        if let Some(unnest_list_of_struct) = &self.unnest_list_of_struct {
            valid_or_insert_error!(errors, unnest_list_of_struct, "transform[unnest_list_of_struct]");
        }
        errors
    }

    fn transform(&self) -> Box<dyn Transform> {
        Box::new(UnnestTransform { 
            col_list_of_struct: self.unnest_list_of_struct.clone().map(|x| x.value().expect("transform[unnest_list_of_struct]").clone()),
            col_list: self.unnest_list.clone().map(|x| x.value().expect("transform[unnest_list]").clone()),
            col_struct: self.unnest_struct.clone().map(|x| x.value().expect("transform[unnest_struct]").clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::keyword::{Keyword, PolarsExprKeyword};
    use crate::pipeline::context::DefaultPipelineContext;
    use crate::task::transform::common::TransformConfig;
    use crate::task::transform::config::UnnestTransformConfig;
    use crate::util::common::str_json_to_df;
    use crate::util::test::DummyData;
    use polars::prelude::{IntoLazy, col};
    use std::sync::Arc;

    #[test]
    fn valid_unnest_transform_basic_list_of_struct() {
        let main = str_json_to_df(&DummyData::meta_info()).unwrap().lazy();
        let config = UnnestTransformConfig {
            unnest_list_of_struct: Some(PolarsExprKeyword::with_value(col("players"))),
            unnest_list: None,
            unnest_struct: None
        };
        println!("main {:?}", main.clone().collect().unwrap());
        assert!(config.validate().is_empty());
        let drop = config.transform();
        let ctx = Arc::new(DefaultPipelineContext::new());
        let expected = main.clone().explode([col("players")]).unnest([col("players")]).collect().unwrap();
        println!("expected {:?}", expected);
        let actual = drop.run(main.clone(), ctx).unwrap().collect().unwrap();
        println!("actual {:?}", actual);
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_unnest_transform_basic_list() {
        let main = str_json_to_df(&DummyData::meta_info()).unwrap().lazy();
        let config = UnnestTransformConfig {
            unnest_list: Some(PolarsExprKeyword::with_value(col("players"))),
            unnest_list_of_struct: None,
            unnest_struct: None
        };
        println!("main {:?}", main.clone().collect().unwrap());
        assert!(config.validate().is_empty());
        let drop = config.transform();
        let ctx = Arc::new(DefaultPipelineContext::new());
        let expected = main.clone().explode([col("players")]).collect().unwrap();
        println!("expected {:?}", expected);
        let actual = drop.run(main.clone(), ctx).unwrap().collect().unwrap();
        println!("actual {:?}", actual);
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_unnest_transform_basic_struct() {
        let main = str_json_to_df(&DummyData::meta_info()).unwrap().lazy().explode([col("players")]);
        let config = UnnestTransformConfig {
            unnest_struct: Some(PolarsExprKeyword::with_value(col("players"))),
            unnest_list_of_struct: None,
            unnest_list: None
        };
        println!("main {:?}", main.clone().collect().unwrap());
        assert!(config.validate().is_empty());
        let drop = config.transform();
        let ctx = Arc::new(DefaultPipelineContext::new());
        let expected = main.clone().unnest([col("players")]).collect().unwrap();
        println!("expected {:?}", expected);
        let actual = drop.run(main.clone(), ctx).unwrap().collect().unwrap();
        println!("actual {:?}", actual);
        assert_eq!(actual, expected);
    }

    #[test]
    fn invalid_unnest_transform_basic() {
        let config = UnnestTransformConfig {
            unnest_list: None,
            unnest_struct: Some(PolarsExprKeyword::with_symbol("price")),
            unnest_list_of_struct: None,
        };
        assert_eq!(config.validate().len(), 1);
    }
}
