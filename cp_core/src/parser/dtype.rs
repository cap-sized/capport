use std::collections::HashMap;

use polars::prelude::{ArrowDataType, CompatLevel, DataType, Field, TimeUnit, TimeZone};
use serde::{Deserialize, Deserializer, Serialize, de};

use crate::util::{
    common::{NYT, UTC},
    error::{CpError, CpResult},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DType(pub polars::datatypes::DataType);

impl From<DType> for DataType {
    fn from(w: DType) -> Self {
        w.0
    }
}

impl From<DType> for ArrowDataType {
    fn from(w: DType) -> Self {
        // Hard coded to use newest compatibility
        w.0.to_arrow(CompatLevel::newest())
    }
}

impl Serialize for DType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.0 {
            DataType::String => serializer.serialize_str("str"),
            DataType::Float32 => serializer.serialize_str("float"),
            DataType::Float64 => serializer.serialize_str("double"),
            DataType::Boolean => serializer.serialize_str("bool"),
            enum_type => {
                if enum_type == DataType::Datetime(TimeUnit::Milliseconds, Some(TimeZone::from_str(NYT))).as_ref() {
                    return serializer.serialize_str("datetime_nyt");
                }
                if enum_type == DataType::Datetime(TimeUnit::Milliseconds, Some(TimeZone::from_str(UTC))).as_ref() {
                    return serializer.serialize_str("datetime_utc");
                }
                let repr = format!("{:?}", enum_type).trim().to_lowercase().to_owned();
                serializer.serialize_str(&repr)
            }
        }
    }
}

fn parse_dtype(dstr: &str) -> CpResult<DType> {
    match dstr {
        "bool" => Ok(DType(DataType::Boolean)),
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
        s => {
            let list_reg = regex::Regex::new("list\\[(.*)\\]").expect("valid list regex: list\\[(.*)\\]");
            if let Some(v) =  list_reg.captures(s) {
                let inner_str = &v[1];
                let inner = parse_dtype(inner_str)?;
                return Ok(DType(DataType::List(Box::new(inner.0))));
            };
            Err(CpError::ConfigError("dtype", format!("Unknown dtype in model: {}", s)))
        }
    }
}

impl<'de> Deserialize<'de> for DType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Helper {
            Short(String),
            Full(HashMap<String, serde_yaml_ng::Value>),
        }
        match Helper::deserialize(deserializer)? {
            Helper::Short(s) => match parse_dtype(s.as_str()) {
                Ok(v) => Ok(v),
                Err(e) => Err(de::Error::custom(e))
            },
            Helper::Full(full) => {
                if full.len() != 1 {
                    return Err(de::Error::custom(format!(
                        "To parse PolarsExprKeyword as an action, it must be a map with exactly one key. Found: {:?}",
                        full
                    )));
                }
                let mut kvpair = full.into_iter().collect::<Vec<_>>();
                let (type_name, type_args) = kvpair.pop().unwrap();
                let action: Result<CpResult<DataType>, serde_yaml_ng::Error> = match type_name.as_str() {
                    "struct" => serde_yaml_ng::from_value::<serde_yaml_ng::Mapping>(type_args).map(|map| {
                        let mut fields = vec![];
                        for (label, dtype) in map {
                            let label_str = serde_yaml_ng::from_value::<String>(label)?;
                            let dtype_struct = serde_yaml_ng::from_value::<DType>(dtype)?;
                            fields.push(Field::new(label_str.into(), dtype_struct.into()));
                        }
                        Ok(DataType::Struct(fields))
                    }),
                    "list" => {
                        serde_yaml_ng::from_value::<DType>(type_args).map(|dtype| Ok(DataType::List(Box::new(dtype.0))))
                    }
                    "datetime" => serde_yaml_ng::from_value::<String>(type_args)
                        .map(|tz| Ok(DataType::Datetime(TimeUnit::Milliseconds, Some(tz.into())))),
                    x => {
                        return Err(de::Error::custom(format!("Unrecognized datatype: {}", x)));
                    }
                };

                let datatype: CpResult<DataType> = match action {
                    Ok(x) => x,
                    Err(e) => return Err(de::Error::custom(format!("Bad dtype: {:?}", e))),
                };
                match datatype {
                    Ok(x) => Ok(DType(x)),
                    Err(e) => Err(de::Error::custom(format!("Bad dtype: {:?}", e))),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::{ArrowDataType, ArrowField, DataType, Field, TimeUnit, TimeZone};

    use crate::util::common::{NYT, UTC};

    use super::DType;

    fn example_dtype() -> Vec<DType> {
        [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::String,
            DataType::Time,
            DataType::Date,
            DataType::Datetime(TimeUnit::Milliseconds, Some(TimeZone::from_str(NYT))),
            DataType::Datetime(TimeUnit::Milliseconds, Some(TimeZone::from_str(UTC))),
        ]
        .map(DType)
        .into_iter()
        .collect::<Vec<_>>()
    }

    fn example_str() -> Vec<String> {
        [
            "bool",
            "int8",
            "int16",
            "int32",
            "int64",
            "uint8",
            "uint16",
            "uint32",
            "uint64",
            "float",
            "double",
            "str",
            "time",
            "date",
            "datetime_nyt",
            "datetime_utc",
        ]
        .map(|x| x.to_owned())
        .into_iter()
        .collect::<Vec<_>>()
    }

    #[test]
    fn valid_dtype_ser() {
        let actual_str = example_dtype()
            .iter()
            .map(|x| serde_yaml_ng::to_string(x).unwrap().trim().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(actual_str, example_str());
    }

    #[test]
    fn valid_dtype_de() {
        let actual_dtype = example_str()
            .iter()
            .map(|x| serde_yaml_ng::from_str::<DType>(x).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual_dtype, example_dtype());
        assert_eq!(DType(DataType::Int64), serde_yaml_ng::from_str::<DType>("int").unwrap());
    }

    #[test]
    fn valid_dtype_de_complex() {
        let configs = [
            "struct: {test: int64}",
            "list: str",
            "datetime: Europe/London",
            "struct: {test: {list: str}}",
            "list: {struct: {a: uint64, b: bool}}",
        ];
        let expected = [
            DataType::Struct(vec![Field::new("test".into(), DataType::Int64)]),
            DataType::List(Box::new(DataType::String)),
            DataType::Datetime(TimeUnit::Milliseconds, Some("Europe/London".into())),
            DataType::Struct(vec![Field::new(
                "test".into(),
                DataType::List(Box::new(DataType::String)),
            )]),
            DataType::List(Box::new(DataType::Struct(vec![
                Field::new("a".into(), DataType::UInt64),
                Field::new("b".into(), DataType::Boolean),
            ]))),
        ];
        let actual = configs.map(|c| serde_yaml_ng::from_str::<DType>(c).unwrap().0);
        assert_eq!(actual, expected);
    }

    #[test]
    fn other_dtype_ser() {
        assert_eq!(
            serde_yaml_ng::to_string(&DType(DataType::Binary)).unwrap().trim(),
            "binary"
        );
    }

    #[test]
    fn invalid_dtype_de() {
        assert!(serde_yaml_ng::from_str::<DType>("bad").is_err());
    }

    #[test]
    fn valid_dtype_to_datatype() {
        let dtype: DType = DType(DataType::Int8);
        assert_eq!(DataType::from(dtype), DataType::Int8)
    }

    #[test]
    fn valid_dtype_to_arrowdtype() {
        let int8_dtype: DType = DType(DataType::Int8);
        assert_eq!(ArrowDataType::from(int8_dtype.clone()), ArrowDataType::Int8);
        let nested = vec![Field::new("test".into(), int8_dtype.clone().into())];
        let nested_arrow = vec![ArrowField::new("test".into(), int8_dtype.clone().into(), true)];
        let struct_dtype: DType = DType(DataType::Struct(nested));
        assert_eq!(ArrowDataType::from(struct_dtype), ArrowDataType::Struct(nested_arrow));
    }
}
