use std::collections::HashMap;

use polars::prelude::Schema;
use serde::Deserialize;

use crate::{
    parser::{
        dtype::DType,
        keyword::{Keyword, ModelFieldKeyword, StrKeyword},
        model::ModelConstraint,
    },
    util::error::CpResult,
};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ModelFieldInfo {
    pub dtype: DType,
    pub constraints: Option<Vec<ModelConstraint>>,
}

impl ModelFieldInfo {
    pub fn with_dtype(dtype: DType) -> Self {
        Self {
            dtype,
            constraints: None,
        }
    }
    pub fn new(dtype: DType, constraints: &[ModelConstraint]) -> Self {
        Self {
            dtype,
            constraints: Some(constraints.to_owned()),
        }
    }
}

pub type ModelFields = HashMap<StrKeyword, ModelFieldKeyword>;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ModelConfig {
    pub label: String,
    pub fields: ModelFields,
}

impl ModelConfig {
    pub fn schema(&self) -> CpResult<Schema> {
        let mut schema = Schema::with_capacity(self.fields.len());
        for (field_name, field_detail) in &self.fields {
            let name = field_name
                .value()
                .expect("value not present for model field_name")
                .as_str();
            let detail = field_detail.value().expect("value not present for model field_detail");
            schema.insert(name.into(), detail.dtype.0.clone());
        }
        Ok(schema)
    }
    pub fn substitute_model_fields(&self, context: &serde_yaml_ng::Mapping) -> CpResult<ModelFields> {
        let mut fields = HashMap::new();
        for (colname, coldetail) in &self.fields {
            let mut name = colname.clone();
            name.insert_value_from_context(context)?;
            let mut detail = coldetail.clone();
            detail.insert_value_from_context(context)?;
            fields.insert(name, detail);
        }
        Ok(fields)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use polars::prelude::{DataType, Field, TimeUnit};

    use crate::parser::{
        dtype::DType,
        keyword::{Keyword, ModelFieldKeyword, StrKeyword},
        model::ModelConstraint,
    };

    use super::{ModelConfig, ModelFieldInfo};

    macro_rules! test_build_field_values {
        ($key:expr, $value:expr) => {
            (
                StrKeyword::with_value(($key).to_owned()),
                ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(($value)))),
            )
        };
    }

    macro_rules! test_build_field_values_constraints {
        ($key:expr, $value:expr, $constraints:expr) => {
            (
                StrKeyword::with_value(($key).to_owned()),
                ModelFieldKeyword::with_value(ModelFieldInfo::new(DType(($value)), ($constraints))),
            )
        };
    }

    #[test]
    fn valid_model_full_config_with_constraints() {
        let model_config = "
label: OUTPUT
fields: 
    $sym: 
        dtype: int64
        constraints: []
    simple: 
        dtype: str
        constraints: [unique]
    complex: 
        dtype: { list: date }
        constraints: [foreign]
";
        let expected = ModelConfig {
            label: "OUTPUT".to_owned(),
            fields: HashMap::from([
                (
                    StrKeyword::with_symbol("sym"),
                    ModelFieldKeyword::with_value(ModelFieldInfo::new(DType(DataType::Int64), &[])),
                ),
                test_build_field_values_constraints!("simple", DataType::String, &[ModelConstraint::Unique]),
                test_build_field_values_constraints!(
                    "complex",
                    DataType::List(Box::new(DataType::Date)),
                    &[ModelConstraint::Foreign]
                ),
            ]),
        };
        let actual: ModelConfig = serde_yaml_ng::from_str(model_config).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn valid_model_full_config_no_constraints() {
        let model_config = "
label: OUTPUT
fields: 
    $sym: int64
    value: $dtype_sym
    simple: str
    complex: 
        list: date
    complex_nested: 
        list: 
            datetime: Asia/Tokyo
    complex_nested_collections: 
        list: 
            struct: 
                a: str
                b: bool
";
        let expected = ModelConfig {
            label: "OUTPUT".to_owned(),
            fields: HashMap::from([
                (
                    StrKeyword::with_symbol("sym"),
                    ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::Int64))),
                ),
                (
                    StrKeyword::with_value("value".to_owned()),
                    ModelFieldKeyword::with_symbol("dtype_sym"),
                ),
                test_build_field_values!("simple", DataType::String),
                test_build_field_values!("complex", DataType::List(Box::new(DataType::Date))),
                test_build_field_values!(
                    "complex_nested",
                    DataType::List(Box::new(DataType::Datetime(
                        TimeUnit::Milliseconds,
                        Some("Asia/Tokyo".into())
                    )))
                ),
                test_build_field_values!(
                    "complex_nested_collections",
                    DataType::List(Box::new(DataType::Struct(vec![
                        Field::new("a".into(), DataType::String),
                        Field::new("b".into(), DataType::Boolean),
                    ])))
                ),
            ]),
        };
        let actual: ModelConfig = serde_yaml_ng::from_str(model_config).unwrap();
        assert_eq!(actual, expected);
    }
}
