use std::io::Cursor;

use polars::{frame::DataFrame, io::SerReader, prelude::JsonReader};

use super::error::CpResult;

pub fn vec_str_json_to_df(vecstr: &Vec<String>) -> CpResult<DataFrame> {
    let input = format!("[{}]", vecstr.join(", "));
    let reader = JsonReader::new(Cursor::new(input.trim()));
    Ok(reader.finish()?)
}

#[cfg(test)]
mod tests {
    use crate::util::{common::DummyData, json::vec_str_json_to_df};

    #[test]
    fn valid_json_vec_parse() {
        let vecstr = DummyData::json_colors();
        assert_eq!(vec_str_json_to_df(&vecstr).unwrap(), DummyData::df_colors());
    }
}
