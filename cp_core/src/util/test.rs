use polars::df;
use polars::frame::DataFrame;
use polars::prelude::{IntoLazy, LazyFrame, SortMultipleOptions};

pub fn sort_dataframe(df: DataFrame) -> DataFrame {
    let mut df = df.clone();

    let mut cols: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    cols.sort();
    df = df.select(&cols).unwrap();

    let sort_options = SortMultipleOptions {
        descending: vec![false; cols.len()],
        nulls_last: vec![false; cols.len()],
        multithreaded: true,
        maintain_order: false,
        limit: None,
    };
    df.sort(cols, sort_options).unwrap()
}

pub fn assert_frame_equal(df1: DataFrame, df2: DataFrame) {
    let df1 = sort_dataframe(df1);
    let df2 = sort_dataframe(df2);

    assert_eq!(df1, df2);
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
            ].unwrap().into_struct("name".into()),
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
