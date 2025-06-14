use chrono::{DateTime, FixedOffset, NaiveDate, Utc};
use log::{debug, trace};
use polars::{
    frame::DataFrame,
    io::SerReader,
    prelude::{JsonReader, Schema, SchemaNamesAndDtypes},
};
use std::{collections::HashMap, io::Cursor};

use rand::{Rng, distr::Alphanumeric};

use crate::{
    context::envvar::get_env_var_str,
    logger::common::{DEFAULT_KEYWORD_CONFIG_DIR, DEFAULT_KEYWORD_OUTPUT_DIR},
};

use super::error::{CpError, CpResult};

pub const NYT: &str = "America/New_York";
pub const UTC: &str = "UTC";

pub type YamlValue = serde_yaml_ng::Value;

pub const RECOGNIZED_DATE_PATTERNS: [&str; 2] = ["%Y-%m-%d", "%Y.%m.%d"];
pub const RECOGNIZED_TIME_PATTERNS: [&str; 2] = ["%H:%M:%S", "%H.%M.%S"];

pub const DEFAULT_HTTP_REQ_MAX_RETRY: u8 = 8;
pub const DEFAULT_HTTP_REQ_INIT_RETRY_INTERVAL_MS: u64 = 1000;

pub enum EnvKeyType {
    Host,
    User,
    Password,
    DbName,
}

pub fn parse_date_str(datetime_str: &str) -> CpResult<NaiveDate> {
    for &pattern in &RECOGNIZED_DATE_PATTERNS {
        match NaiveDate::parse_from_str(datetime_str, pattern) {
            Ok(x) => return Ok(x),
            Err(e) => {
                debug!("Date string `{}` cannot be parsed with pattern `{}`", pattern, e);
                println!("Date string `{}` cannot be parsed with pattern `{}`", pattern, e);
            }
        }
    }
    Err(CpError::RawError(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "Invalid date `{}` parsed, only the following patterns are recognized: {:?}",
            datetime_str, &RECOGNIZED_DATE_PATTERNS
        ),
    )))
}

pub fn parse_datetime_str(datetime_str: &str) -> CpResult<DateTime<FixedOffset>> {
    match datetime_str.parse() {
        Ok(x) => Ok(x),
        Err(e) => Err(CpError::RawError(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid datetime `{}` parsed, {:?}", datetime_str, e),
        ))),
    }
}

pub fn get_fmt_time_str_now(fmt: &str) -> String {
    Utc::now().format(fmt).to_string()
}

pub fn get_utc_time_str_now() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, false)
}

pub fn get_full_path(abs_or_rel_path_str: &str, is_config: bool) -> CpResult<std::path::PathBuf> {
    let path = std::path::Path::new(abs_or_rel_path_str);
    if path.is_absolute() {
        trace!("Loading from absolute path: {:?}", path);
        return Ok(path.to_owned());
    }

    trace!("Received relative path: {:?}; appending root...", path);
    let base = get_env_var_str(if is_config {
        DEFAULT_KEYWORD_CONFIG_DIR
    } else {
        DEFAULT_KEYWORD_OUTPUT_DIR
    })?;
    trace!("Default root dir: {}", base.as_str());
    let full_path = std::path::Path::new(base.as_str()).join(path);
    trace!("Derived path: {:?} (from {:?})", full_path, path);
    Ok(full_path)
}

pub fn vec_str_json_to_df(vecstr: &[String]) -> CpResult<DataFrame> {
    let input = format!("[{}]", vecstr.join(", "));
    let reader = JsonReader::new(Cursor::new(input.trim()));
    Ok(reader.finish()?)
}

pub fn str_json_to_df(str: &str) -> CpResult<DataFrame> {
    let reader = JsonReader::new(Cursor::new(str.trim()));
    Ok(reader.finish()?)
}

pub fn explode_df(df: &DataFrame) -> CpResult<DataFrame> {
    let mut df = df.clone();
    let cols: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    for col in cols {
        if !df.column(&col)?.dtype().is_list() {
            continue;
        }
        df = df.explode([col])?;
    }
    Ok(df)
}

pub fn create_config_pack<I>(yaml_strs: I) -> HashMap<String, HashMap<String, serde_yaml_ng::Value>>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let configs = yaml_strs
        .into_iter()
        .map(|x| serde_yaml_ng::from_str::<HashMap<String, serde_yaml_ng::Mapping>>(x.as_ref()).expect("invalid yaml"));
    let mut map = HashMap::new();
    for config in configs {
        for (key, value) in config {
            if !map.contains_key(&key) {
                map.insert(key.clone(), HashMap::<String, serde_yaml_ng::Value>::new());
            }
            for (k, v) in value {
                map.get_mut(&key).unwrap().insert(k.as_str().unwrap().to_owned(), v);
            }
        }
    }
    map
}

pub fn yaml_from_str(s: &str) -> CpResult<serde_yaml_ng::Value> {
    Ok(serde_yaml_ng::from_str(s)?)
}

pub fn yaml_to_str(doc: &serde_yaml_ng::Value) -> CpResult<String> {
    Ok(serde_yaml_ng::to_string(doc)?)
}

pub fn rng_str(len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

pub fn format_schema(schema: &Schema) -> String {
    let mut rows = vec![format!("Schema:")];
    let max_len = schema.iter_names().fold(0, |len, name| std::cmp::max(len, name.len()));
    schema.iter_names_and_dtypes().for_each(|(name, dtype)| {
        rows.push(format!("\t{:width$}\t\t{}", name, dtype, width = max_len));
    });
    rows.join("\n")
}

/// This macro allows us to run n async tasks and gather the results
#[macro_export]
macro_rules! ctx_run_n_async {
    ($label:expr, $tasks:expr, $action:expr, $($i:expr),*) => {
        let tasks = ($tasks);
        let mut handles = Vec::with_capacity(tasks.len());
        for task in tasks {
            handles.push(async || ($action)(task, $($i.clone()),*).await);
        }

        let results = futures::future::join_all(handles.into_iter().map(|h| h())).await;
        for result in results {
            match result {
                Ok(_) => {}
                Err(e) => log::error!("{}: {:?}", ($label), e),
            }
        }
    };
}

/// This macro allows us to split evenly (best effort) the tasks into n_threads to be handled
#[macro_export]
macro_rules! ctx_run_n_threads {
    ($num_threads:expr, $slice:expr, $action:expr, $($i:expr),*) => {
        let slice = ($slice);
        let len = slice.len();
        let n = std::cmp::min(($num_threads) as usize, len);
        log::trace!("Started {} threads", n);
        let (quo, rem) = (len / n, len % n);
        let split = (quo + 1) * rem;
        match crossbeam::thread::scope(|scope| {
            let chunks = slice[..split].chunks(quo + 1).chain(slice[split..].chunks(quo));
            for chunk in chunks {
                let items = (chunk, $($i.clone()),*);
                scope.spawn(move |_| ($action)(items));
            }
        }) {
            Ok(_) => {
                log::trace!("Joined {} threads", n);
            }
            Err(e) => {
                log::error!("Thread err:\n{:?}", e);
            }
        };
    };
}

#[macro_export]
macro_rules! model_emplace {
    ( $val:expr, $ctx:expr, $context:expr ) => {
        if let Some(mut model_name) = ($val).model.clone() {
            model_name.insert_value_from_context($context)?;
            if let Some(name) = model_name.value() {
                let model = ($ctx).get_model(name)?;
                ($val).model_fields = Some(model.fields);
            }
            let _ = ($val).model.insert(model_name.clone());
        }
        if let Some(model_fields) = ($val).model_fields.take() {
            let model = $crate::model::common::ModelConfig {
                label: "".to_string(),
                fields: model_fields,
            };
            let fields = model.substitute_model_fields($context)?;
            let _ = ($val).model_fields.insert(fields);
        }
    };
}

#[macro_export]
macro_rules! db_url_emplace {
    ( $val:expr, $ctx:expr, $context:expr, $urlfmt:expr ) => {
        if let Some(mut url) = ($val).url.clone() {
            url.insert_value_from_context($context)?;
            let _ = ($val).url.insert(url);
        }
        if let Some(mut ev) = ($val).env_connection.clone() {
            ev.insert_value_from_context($context)?;
            if ($val).url.is_none() {
                if let Some(label) = ev.value() {
                    let url = [$urlfmt.to_owned(), ($ctx).get_connection(label)?.to_url()].join("");
                    let _ = ($val).url.insert(StrKeyword::with_value(url));
                }
            }
            let _ = ($val).env_connection.insert(ev);
        }
    };
}

#[macro_export]
macro_rules! valid_or_insert_error {
    ($errors:expr, $keyword:expr, $keyname:expr) => {
        match ($keyword).value() {
            Some(_) => {}
            None => ($errors).push(CpError::SymbolMissingValueError(
                ($keyname),
                ($keyword).symbol().unwrap_or("?").to_owned(),
            )),
        }
    };
}

#[macro_export]
macro_rules! result_or_insert_error {
    ($errors:expr, $optkey:expr, $ctx:expr) => {
        ($optkey).map(|j| {
            if ($ctx).extract_result(j).is_err() {
                ($errors).push(CpError::SymbolResultError(j.to_owned()));
            }
        });
    };
}

#[macro_export]
macro_rules! try_deserialize_stage {
    ($value:expr, $result:ty, $($type:ty),+) => {
        $(if let Ok(config) = serde_yaml_ng::from_value::<$type>($value.clone()) {
            Some(Box::new(config) as Box<$result>)
        }) else+
        else {
            None
        }
    };
}

#[macro_export]
macro_rules! async_st {
    ($lambda:expr) => {
        let mut rt_builder = tokio::runtime::Builder::new_current_thread();
        rt_builder.enable_all();
        let rt = rt_builder.build().unwrap();
        let event = $lambda;
        rt.block_on(event())
    };
}

#[macro_export]
macro_rules! async_mt {
    ($lambda:expr) => {
        let mut rt_builder = tokio::runtime::Builder::new_multi_thread();
        rt_builder.enable_all();
        let rt = rt_builder.build().unwrap();
        let event = $lambda;
        rt.block_on(event())
    };
}
