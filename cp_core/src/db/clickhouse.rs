use chrono::{DateTime, NaiveDate, Local, Utc, FixedOffset};
use clickhouse_rs::{types::{column::iter::StringIterator, Complex, DateTimeType, SqlType}, ClientHandle};
use polars::{frame::DataFrame, prelude::{DataType, IntoVec, NamedFrom}};

use crate::{model::common::ModelConfig, parser::{connection::NetworkConnection, dtype::DType}, util::error::{CpError, CpResult}};

fn ch_to_pl_type(sql_type: &SqlType) -> CpResult<DataType> {
    match sql_type {
        &SqlType::Bool => Ok(DataType::Boolean),
        &SqlType::UInt8 => Ok(DataType::UInt8),
        &SqlType::UInt16 => Ok(DataType::UInt16),
        &SqlType::UInt32 => Ok(DataType::UInt32),
        &SqlType::UInt64 => Ok(DataType::UInt64),
        &SqlType::Int8 => Ok(DataType::Int8),
        &SqlType::Int16 => Ok(DataType::Int16),
        &SqlType::Int32 => Ok(DataType::Int32),
        &SqlType::Int64 => Ok(DataType::Int64),
        &SqlType::String => Ok(DataType::String),
        &SqlType::FixedString(_) => Ok(DataType::String),
        &SqlType::Date => Ok(DataType::Date),
        &SqlType::Float32 => Ok(DataType::Float32),
        &SqlType::Float64 => Ok(DataType::Float64),
        &SqlType::Decimal(n, k) => Ok(DataType::Decimal(Some(n as usize), Some(k as usize))),
        &SqlType::Nullable(stype) => Ok(ch_to_pl_type(stype)?),
        &SqlType::Array(stype) => Ok(DataType::List(Box::new(ch_to_pl_type(stype)?.into()))),
        &SqlType::DateTime(dttype) => match dttype {
            DateTimeType::Chrono => Err(CpError::ComponentError("Clickhouse Types", "DateTime(Chrono) not supported by capport".to_owned())),
            DateTimeType::DateTime32 => Ok(DataType::Datetime(polars::prelude::TimeUnit::Nanoseconds, None)),
            DateTimeType::DateTime64(time_unit, tz) => Ok(DataType::Datetime(match time_unit {
                9 => polars::prelude::TimeUnit::Nanoseconds,
                6 => polars::prelude::TimeUnit::Microseconds,
                3 => polars::prelude::TimeUnit::Milliseconds,
                _ => return Err(CpError::ComponentError("Clickhouse Types", format!("DateTime({}, {}) not supported by capport", time_unit, tz))),
            }, Some(tz.to_string().into()))),
        },
        &SqlType::Map(_, _) => Err(CpError::ComponentError("Clickhouse Types", "Map not supported by capport".to_owned())),
        &SqlType::Ipv4 => Err(CpError::ComponentError("Clickhouse Types", "Ipv4 not supported by capport".to_owned())),
        &SqlType::Ipv6 => Err(CpError::ComponentError("Clickhouse Types", "Ipv6 not supported by capport".to_owned())),
        &SqlType::Uuid => Err(CpError::ComponentError("Clickhouse Types", "Uuid not supported by capport".to_owned())),
        &SqlType::Enum8(_) => Err(CpError::ComponentError("Clickhouse Types", "Enum8 not yet supported by capport".to_owned())),
        &SqlType::Enum16(_) => Err(CpError::ComponentError("Clickhouse Types", "Enum16 not yet supported by capport".to_owned())),
        &SqlType::SimpleAggregateFunction(_, _) => Err(CpError::ComponentError("Clickhouse Types", "SimpleAggregateFunction not yet supported by capport".to_owned())),
    }
}

macro_rules! custom_col {
    ($name:expr, $v:expr) => {
        polars::prelude::Series::new($name.into(), $v.as_slice())
    }
}

macro_rules! create_col {
    ($name:expr, $column:expr, $dtype:tt) => {
 polars::prelude::Series::new($name.into(), $column.iter::<$dtype>().into_iter().map(|x| x.into_iter().map(|y| y.to_owned())).flatten().collect::<Vec<$dtype>>().as_slice())
    }
}

macro_rules! derive_dt_type {
    ($tz:expr) => {
        match tz {
            "America/New_York" => FixedOffset::
        }
    }
}
// polars::prelude::Series::new($name.into(), $column.iter::<$dtype>()?.collect::<Vec<&$dtype>>().as_slice())

fn ch_to_pl(column: clickhouse_rs::types::Column<Complex>) -> CpResult<polars::prelude::Series> {
    let dtype = ch_to_pl_type(&column.sql_type())?;
    let name = column.name();

    // let fin: Vec<u32> = columns.into_iter().map(|x|x.to_owned()).collect();
    // let ss = column.iter::<&[u8]>().into_iter().map(|x| x.into_iter().map(|y| String::from_utf8_lossy(y).into_owned())).flatten().collect::<Vec<String>>();

    // let sss = column.iter::<&str>().into_iter().map(|x| x.into_iter().map(|y| y.to_owned())).flatten().collect::<Vec<String>>().as_slice();

    match dtype {
        DataType::Boolean => Ok(create_col!(name, column, bool)),
        DataType::UInt8 => Ok(create_col!(name, column, u8)),
        DataType::UInt16 => Ok(create_col!(name, column, u16)),
        DataType::UInt32 => Ok(create_col!(name, column, u32)),
        DataType::UInt64 => Ok(create_col!(name, column, u64)),
        DataType::Int8 => Ok(create_col!(name, column, i8)),
        DataType::Int16 => Ok(create_col!(name, column, i16)),
        DataType::Int32 => Ok(create_col!(name, column, i32)),
        DataType::Int64 => Ok(create_col!(name, column, i64)),
        DataType::Float32 => Ok(create_col!(name, column, f32)),
        DataType::Float64 => Ok(create_col!(name, column, f64)),
        DataType::String => Ok(custom_col!(name, column.iter::<&[u8]>().into_iter().map(|x| x.into_iter().map(|y| String::from_utf8_lossy(y).into_owned())).flatten().collect::<Vec<String>>())),
        DataType::Date => Ok(create_col!(name, column, NaiveDate)),
        // DataType::Datetime(tu, tz) => Ok(create_col!(name, column, DateTime<tz>)),
        _ => Err(CpError::ComponentError("Clickhouse to Polars", "Could not convert".to_owned()))
/*
        DataType::Boolean => Vec::from_iter(column.iter::<bool>()),
        DataType::UInt8 => Vec::from_iter(column.iter::<u8>()),
        DataType::UInt16 => Vec::from_iter(column.iter::<u16>()),
        DataType::UInt32 => Vec::from_iter(column.iter::<u32>()),
        DataType::UInt64 => Vec::from_iter(column.iter::<u64>()),
*/
    }
}


pub struct ClickhouseClient {
    pool: clickhouse_rs::Pool,
    db_name: String,
}

impl ClickhouseClient {
    pub fn new(network: NetworkConnection) -> ClickhouseClient {
        ClickhouseClient { pool: clickhouse_rs::Pool::new(network.to_uri()), db_name: network.db_name }
    }

    pub fn get_model_query(&self, model: ModelConfig, table: &str, extra_clauses: &str) -> CpResult<String> {
        let schema = model.schema().unwrap();
        let columns = schema.iter_names().map(|x| x.as_str()).collect::<Vec<&str>>();
        let col_list = columns.join(", ");
        Ok(format!("SELECT {} FROM {}.{} FINAL {}", col_list, &self.db_name, table, extra_clauses))
    }

    pub async fn query(&self, sql: &str) -> CpResult<()> {
        let mut dataframe = DataFrame::empty();
        let mut handle: ClientHandle = self.pool.get_handle().await?;
        let block = match handle.query(sql).fetch_all().await {
            Ok(x) => x,
            Err(e) => return Err(CpError::ConnectionError(format!("{:?}", e)))
        };
        for column in block.columns() {
            let name = column.name();
            let dtype = column.sql_type();
            column
        }
        Ok(())
    }
}
