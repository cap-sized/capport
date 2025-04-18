use std::fmt;

use polars::prelude::*;

use crate::util::error::SubResult;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelField {
    pub label: String,
    pub dtype: polars::datatypes::DataType,
    pub constraints: Vec<String>, // TODO: Replace with constraints enum
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Model {
    pub name: String,
    pub fields: Vec<ModelField>,
}

impl fmt::Display for Model {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl Model {
    pub fn new(name: &str, fields: &[ModelField]) -> Model {
        Model {
            name: name.to_string(),
            fields: fields.to_vec(),
        }
    }
}

impl ModelField {
    pub fn new(label: &str, dtype: polars::datatypes::DataType, constraints: Option<&[&str]>) -> ModelField {
        ModelField {
            label: label.to_string(),
            dtype,
            constraints: constraints.unwrap_or_default().iter().map(|x| x.to_string()).collect(),
        }
    }

    fn expr(&self) -> Expr {
        col(&self.label).strict_cast(self.dtype.clone())
    }
}

pub trait Reshape {
    fn reshape(&self, df: LazyFrame) -> SubResult<LazyFrame>;
}

impl Reshape for Model {
    fn reshape(&self, df: LazyFrame) -> SubResult<LazyFrame> {
        Ok(df.select(self.fields.iter().map(|x| x.expr()).collect::<Vec<Expr>>()))
    }
}

#[cfg(test)]
mod tests {
    use polars::{df, prelude::DataType};
    use polars_lazy::frame::IntoLazy;

    use super::{Model, ModelField, Reshape};

    #[test]
    fn mapping_reshape_test() {
        let sample_df = df![
            "price" => [2.3, 102.023, 19.88],
            "instrument" => ["ABAB", "TORO", "PKJT"],
        ]
        .unwrap()
        .lazy();
        let model = Model::new(
            "pxtable",
            &[
                ModelField::new("price", DataType::Int32, None),
                ModelField::new("instrument", DataType::String, None),
            ],
        );
        let actual_mapped = model.reshape(sample_df).unwrap().collect().unwrap();
        // println!("{:?}", actual_mapped);
        assert_eq!(actual_mapped.column("price").unwrap().dtype(), &DataType::Int32);
        assert_eq!(actual_mapped.column("instrument").unwrap().dtype(), &DataType::String);
    }
}
