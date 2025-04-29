use chrono::{DateTime, FixedOffset, NaiveDate, Utc};
use log::debug;
use polars::{df, frame::DataFrame, prelude::PlSmallStr};
use polars_lazy::frame::{IntoLazy, LazyFrame};
use std::collections::HashMap;

use rand::{Rng, distr::Alphanumeric};

use crate::parser::common::YamlRead;

use super::error::{CpError, CpResult};

pub const NYT: &str = "America/New_York";
pub const UTC: &str = "UTC";

pub type YamlValue = serde_yaml_ng::Value;

pub const RECOGNIZED_DATE_PATTERNS: [&str; 2] = ["%Y-%m-%d", "%Y.%m.%d"];
pub const RECOGNIZED_TIME_PATTERNS: [&str; 2] = ["%H:%M:%S", "%H.%M.%S"];

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
    return Err(CpError::RawError(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "Invalid date `{}` parsed, only the following patterns are recognized: {:?}",
            datetime_str, &RECOGNIZED_DATE_PATTERNS
        ),
    )));
}

pub fn parse_datetime_str(datetime_str: &str) -> CpResult<DateTime<FixedOffset>> {
    match datetime_str.parse() {
        Ok(x) => return Ok(x),
        Err(e) => {
            return Err(CpError::RawError(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid datetime `{}` parsed, {:?}", datetime_str, e),
            )));
        }
    }
}

pub fn get_utc_time_str_now() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, false)
}

pub fn create_config_pack(
    yaml_str: &str,
    configurable: &str,
) -> HashMap<String, HashMap<String, serde_yaml_ng::Value>> {
    let configs: serde_yaml_ng::Value = serde_yaml_ng::from_str(yaml_str).unwrap();
    HashMap::from([(configurable.to_owned(), configs.to_str_map().unwrap())])
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

pub struct DummyData;
impl DummyData {
    pub fn state_code() -> LazyFrame {
        // https://ddvat.gov.in/docs/List%20of%20State%20Code.pdf
        df! [
            "state" => ["Karnataka", "Goa", "Tamil Nadu", "Delhi"],
            "state_code" => ["KA", "GA", "TN", "DL"],
            "tin" => [29, 30, 33, 7],
        ]
        .unwrap()
        .lazy()
    }

    pub fn player_scores() -> LazyFrame {
        df![
            "csid" => [82938842, 82938842, 86543102, 82938842, 86543102, 86543102, 8872631],
            "game" => [1, 2, 1, 3, 2, 3, 1],
            "scores" => [20, 3, 43, -7, 50, 12, 19],
        ]
        .unwrap()
        .lazy()
    }

    pub fn id_name_map() -> LazyFrame {
        df![
            "first_name" => ["Darren", "Hunter", "Varya"],
            "last_name" => ["Hutnaby", "O'Connor", "Zeb"],
            "id" => [8872631, 82938842, 86543102],
        ]
        .unwrap()
        .lazy()
    }

    pub fn player_data() -> LazyFrame {
        df![
            "csid" => [8872631, 82938842, 86543102],
            "playerId" => ["abcd", "88ef", "1988"],
            "shootsCatches" => ["L", "R", "L"],
            "state" => ["TN", "DL", "GA"],
            "name" => df![
                "first" => ["Darren", "Hunter", "Varya"],
                "last" => ["Hutnaby", "O'Connor", "Zeb"],
            ].unwrap().into_struct(PlSmallStr::from_str("name")),
        ]
        .unwrap()
        .lazy()
    }

    pub fn json_colors() -> Vec<String> {
        "
{ \"color\": \"red\", \"value\": \"#f00\" },
{ \"color\": \"green\", \"value\": \"#0f0\" },
{ \"color\": \"blue\", \"value\": \"#00f\" },
{ \"color\": \"cyan\", \"value\": \"#0ff\" },
{ \"color\": \"magenta\", \"value\": \"#f0f\" },
{ \"color\": \"yellow\", \"value\": \"#ff0\" },
{ \"color\": \"black\", \"value\": \"#000\" }
"
        .split("},")
        .filter(|x| !x.trim().is_empty())
        .map(|x| x.trim())
        .map(|x| {
            if x.ends_with("}") {
                x.to_owned()
            } else {
                format!("{} }}", x)
            }
        })
        .collect()
    }

    pub fn df_colors() -> DataFrame {
        df![
            "color" => [ "red", "green", "blue", "cyan", "magenta", "yellow", "black", ],
            "value" => [ "#f00", "#0f0", "#00f", "#0ff", "#f0f", "#ff0", "#000", ]
        ]
        .unwrap()
    }

    pub fn json_actions() -> Vec<String> {
        "
{\"id\": \"Open\"},
{\"id\": \"OpenNew\", \"label\": \"Open New\"},
{\"id\": \"ZoomIn\", \"label\": \"Zoom In\"},
{\"id\": \"ZoomOut\", \"label\": \"Zoom Out\"},
{\"id\": \"OriginalView\", \"label\": \"Original View\"},
{\"id\": \"Quality\"},
{\"id\": \"Pause\"},
{\"id\": \"Mute\"},
{\"id\": \"Find\", \"label\": \"Find...\"},
{\"id\": \"FindAgain\", \"label\": \"Find Again\"},
{\"id\": \"Copy\"},
{\"id\": \"CopyAgain\", \"label\": \"Copy Again\"},
{\"id\": \"CopySVG\", \"label\": \"Copy SVG\"},
{\"id\": \"ViewSVG\", \"label\": \"View SVG\"},
{\"id\": \"ViewSource\", \"label\": \"View Source\"},
{\"id\": \"SaveAs\", \"label\": \"Save As\"},
{\"id\": \"Help\"},
{\"id\": \"About\", \"label\": \"About Adobe CVG Viewer...\"}
"
        .split("},")
        .filter(|x| !x.trim().is_empty())
        .map(|x| x.trim())
        .map(|x| {
            if !x.ends_with("}") {
                format!("{} }}", x)
            } else {
                x.to_owned()
            }
        })
        .collect()
    }

    pub fn shift_charts() -> String {
        r###"{
  "data": [
    {
      "id": 13679994,
      "detailCode": 0,
      "duration": "00:33",
      "endTime": "01:57",
      "eventDescription": null,
      "eventDetails": null,
      "eventNumber": 166,
      "firstName": "Alec",
      "gameId": 2023020573,
      "hexValue": "#B9975B",
      "lastName": "Martinez",
      "period": 1,
      "playerId": 8474166,
      "shiftNumber": 1,
      "startTime": "01:24",
      "teamAbbrev": "VGK",
      "teamId": 54,
      "teamName": "Vegas Golden Knights",
      "typeCode": 517
    },
    {
      "id": 13679995,
      "detailCode": 0,
      "duration": "00:29",
      "endTime": "04:17",
      "eventDescription": null,
      "eventDetails": null,
      "eventNumber": 203,
      "firstName": "Alec",
      "gameId": 2023020573,
      "hexValue": "#B9975B",
      "lastName": "Martinez",
      "period": 1,
      "playerId": 8474166,
      "shiftNumber": 2,
      "startTime": "03:48",
      "teamAbbrev": "VGK",
      "teamId": 54,
      "teamName": "Vegas Golden Knights",
      "typeCode": 517
    }
  ],
  "total": 2
}"###
            .to_string()
    }

    pub fn meta_info() -> String {
        // sample response from https://api-web.nhle.com/v1/meta?players=8478401,8478402&teams=EDM,TOR
        r#"{
  "players": [
    {
      "playerId": 8478402,
      "playerSlug": "connor-mcdavid-8478402",
      "actionShot": "https://assets.nhle.com/mugs/actionshots/1296x729/8478402.jpg",
      "name": {
        "default": "Connor McDavid"
      },
      "currentTeams": [
        {
          "teamId": 22,
          "abbrev": "EDM",
          "force": true
        }
      ]
    },
    {
      "playerId": 8478401,
      "playerSlug": "pavel-zacha-8478401",
      "actionShot": "https://assets.nhle.com/mugs/actionshots/1296x729/8478401.jpg",
      "name": {
        "default": "Pavel Zacha"
      },
      "currentTeams": [
        {
          "teamId": 6,
          "abbrev": "BOS",
          "force": true
        }
      ]
    }
  ],
  "teams": [
    {
      "name": {
        "default": "Edmonton Oilers",
        "fr": "Oilers d'Edmonton"
      },
      "tricode": "EDM",
      "teamId": 22
    },
    {
      "name": {
        "default": "Toronto Maple Leafs",
        "fr": "Maple Leafs de Toronto"
      },
      "tricode": "TOR",
      "teamId": 10
    }
  ],
  "seasonStates": []
}"#
        .to_string()
    }
}
