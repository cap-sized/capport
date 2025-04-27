use std::fmt;

use polars::prelude::*;
use serde::{Deserialize, Deserializer, de};

use crate::util::{
    common::{NYT, UTC},
    error::SubResult,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DType(pub polars::datatypes::DataType);

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct ModelField {
    pub label: String,
    pub dtype: DType,
    pub constraints: Option<Vec<String>>, // TODO: Replace with constraints enum
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
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
            dtype: DType(dtype),
            constraints: constraints.map(|x| x.iter().map(|x| x.to_string()).collect()),
        }
    }

    fn expr(&self) -> Expr {
        col(&self.label).strict_cast(self.dtype.clone().into())
    }
}

impl From<DType> for polars::datatypes::DataType {
    fn from(w: DType) -> Self {
        w.0
    }
}

impl<'de> Deserialize<'de> for DType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "int" => Ok(DType(DataType::Int64)),
            "int8" => Ok(DType(DataType::Int8)),
            "int16" => Ok(DType(DataType::Int16)),
            "int32" => Ok(DType(DataType::Int32)),
            "int64" => Ok(DType(DataType::Int64)),
            "uint8" => Ok(DType(DataType::UInt8)),
            "uint16" => Ok(DType(DataType::UInt16)),
            "uint32" => Ok(DType(DataType::UInt32)),
            "uint64" => Ok(DType(DataType::UInt64)),
            "float" => Ok(DType(DataType::Float32)),
            "double" => Ok(DType(DataType::Float64)),
            "str" => Ok(DType(DataType::String)),
            "char" => Ok(DType(DataType::UInt8)),
            "time" => Ok(DType(DataType::Time)),
            "date" => Ok(DType(DataType::Date)),
            "datetime_nyt" => Ok(DType(DataType::Datetime(
                TimeUnit::Milliseconds,
                Some(TimeZone::from_str(NYT)),
            ))),
            "datetime_utc" => Ok(DType(DataType::Datetime(
                TimeUnit::Milliseconds,
                Some(TimeZone::from_str(UTC)),
            ))),
            "list[str]" => Ok(DType(DataType::List(Box::new(DataType::String)))),
            "list[int]" => Ok(DType(DataType::List(Box::new(DataType::Int64)))),
            "list[double]" => Ok(DType(DataType::List(Box::new(DataType::Float64)))),
            s => Err(de::Error::custom(format!("Unknown dtype in model: {}", s))),
        }
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
