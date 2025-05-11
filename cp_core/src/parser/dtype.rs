use polars::prelude::{DataType, TimeUnit, TimeZone};
use serde::{de, Deserialize, Deserializer, Serialize};

use crate::util::common::{NYT, UTC};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DType(pub polars::datatypes::DataType);

impl From<DType> for DataType {
    fn from(w: DType) -> Self {
        w.0
    }
}

impl Serialize for DType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer {
        match &self.0 {
            DataType::String => serializer.serialize_str("str"),
            DataType::Float32 => serializer.serialize_str("float"),
            DataType::Float64 => serializer.serialize_str("double"),
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
            // TODO: Handle parsing of list[xxx] types, more datetime regions
            s => Err(de::Error::custom(format!("Unknown dtype in model: {}", s))),
        }
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::{DataType, TimeUnit, TimeZone};

    use crate::util::common::{NYT, UTC};

    use super::DType;


    fn example_dtype() -> Vec<DType>{
        [
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
            DataType::Datetime(TimeUnit::Milliseconds, Some(TimeZone::from_str(UTC)))
        ].map(DType).into_iter().collect::<Vec<_>>()
    }

    fn example_str() -> Vec<String> {
        [
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
        ].map(|x| x.to_owned()).into_iter().collect::<Vec<_>>()
    }

    #[test]
    fn valid_dtype_ser() {
        let actual_str = example_dtype().iter().map(|x| serde_yaml_ng::to_string(x).unwrap().trim().to_owned()).collect::<Vec<_>>();
        assert_eq!(actual_str, example_str());
    }

    #[test]
    fn valid_dtype_de() {
        let actual_dtype = example_str().iter().map(|x| serde_yaml_ng::from_str::<DType>(x).unwrap()).collect::<Vec<_>>();
        assert_eq!(actual_dtype, example_dtype());
        assert_eq!(DType(DataType::Int64), serde_yaml_ng::from_str::<DType>("int").unwrap());
    }

    #[test]
    fn other_dtype_ser() {
        assert_eq!(serde_yaml_ng::to_string(&DType(DataType::Binary)).unwrap().trim(), "binary");
    }

    #[test]
    fn invalid_dtype_de() {
        assert!(serde_yaml_ng::from_str::<DType>("bad").is_err());
    }


}
