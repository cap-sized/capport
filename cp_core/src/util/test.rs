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

    pub fn df_instrument_prices() -> DataFrame {
        df![
            "id" => 0..7,
            "price" => [50.1, 62.3, 69.10, 88.8, 30.2, 29.2, 40.3],
        ]
        .unwrap()
    }

    pub fn df_instruments() -> DataFrame {
        df![
            "ric" => ["AAPL", "AMZN", "GOOG", "NVDA", "NOVA", "BABA", "SPOT"],
            "mkt" => ["amer", "amer", "amer", "amer", "emea", "apac", "emea"],
            "id" => 0..7,
        ]
        .unwrap()
    }

    pub fn json_instrument_prices() -> Vec<String> {
        vec![
            String::from(r#"{ "id": 0, "price": 50.1 }"#),
            String::from(r#"{ "id": 1, "price": 62.3 }"#),
            String::from(r#"{ "id": 2, "price": 69.10 }"#),
            String::from(r#"{ "id": 3, "price": 88.8 }"#),
            String::from(r#"{ "id": 4, "price": 30.2 }"#),
            String::from(r#"{ "id": 5, "price": 29.2 }"#),
            String::from(r#"{ "id": 6, "price": 40.3 }"#),
        ]
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

#[cfg(test)]
pub mod tests {
    use mysql::params;
    use mysql::prelude::Queryable;
    use polars::{df, frame::DataFrame};

    pub struct DbTools;
    impl DbTools {
        pub fn populate_pg_person(user: &str, password: &str, table: &str) -> DataFrame {
            let connect_str = format!("host=localhost user={} password={} dbname={}", user, password, user);
            let mut client = postgres::Client::connect(&connect_str, postgres::NoTls).unwrap();
            let create_cmd = format!(
                "
                CREATE TABLE IF NOT EXISTS {} (
                    id      SERIAL PRIMARY KEY,
                    name    TEXT NOT NULL,
                    data    BYTEA
                )
            ",
                table
            );
            let del_cmd = format!("TRUNCATE {}", table);
            let insert_cmd = format!("INSERT INTO {} (name, data) VALUES ($1, $2)", table);

            let name = "Ferris";
            let data = None::<&[u8]>;

            client
                .batch_execute(&create_cmd)
                .expect("failed create table via client");
            client.batch_execute(&del_cmd).expect("failed delete rows via client");
            client
                .execute(&insert_cmd, &[&name, &data])
                .expect("failed insert via client");
            df!(
                "name" => [name],
                "data" => [data]
            )
            .unwrap()
        }

        pub fn drop_pg(user: &str, password: &str, table: &str) {
            let connect_str = format!("host=localhost user={} password={} dbname={}", user, password, user);
            let mut client = postgres::Client::connect(&connect_str, postgres::NoTls).unwrap();
            let drop_cmd = format!("DROP TABLE {}", table);

            client.batch_execute(&drop_cmd).expect("failed drop table via client");
        }

        pub fn populate_my_accounts(user: &str, password: &str, db: &str, table: &str, port: usize) -> DataFrame {
            let url = format!("mysql://{}:{}@localhost:{}/{}", user, password, port, db);
            let pool = mysql::Pool::new(url.as_str()).expect("failed to create mysql connection");

            let mut conn = pool.get_conn().expect("conn");

            let create_cmd = format!(
                r"CREATE TABLE IF NOT EXISTS {} (
                id int not null,
                amt int not null,
                account_name text
            )",
                table
            );
            let del_cmd = format!("TRUNCATE {}", table);

            conn.query_drop(create_cmd).unwrap();
            conn.query_drop(del_cmd).unwrap();

            let payments = df!(
                "id" => [1, 3, 5, 7, 9],
                "amt" => [2, 4, 6, 8, 10],
                "account_name" => [None, Some("foo".to_owned()), None, None, Some("bar".to_owned())],
            )
            .unwrap();

            conn.exec_batch(
                r"INSERT INTO payments (id, amt, account_name)
                  VALUES (:id, :amt, :account_name)",
                [
                    mysql::params! { "id" => 1, "amt" => 2, "account_name" => None::<String>},
                    mysql::params! { "id" => 3, "amt" => 4, "account_name" => Some("foo".to_owned())},
                    mysql::params! { "id" => 5, "amt" => 6, "account_name" => None::<String>},
                    mysql::params! { "id" => 7, "amt" => 8, "account_name" => None::<String>},
                    mysql::params! { "id" => 9, "amt" => 10, "account_name" => Some("bar".to_owned())},
                ],
            )
            .unwrap();

            payments
        }

        pub fn drop_my(user: &str, password: &str, db: &str, table: &str, port: usize) {
            let url = format!("mysql://{}:{}@localhost:{}/{}", user, password, port, db);
            let pool = mysql::Pool::new(url.as_str()).expect("failed to create mysql connection");

            let mut conn = pool.get_conn().expect("conn");

            let drop_cmd = format!("DROP TABLE {}", table);

            conn.query_drop(drop_cmd).expect("failed drop table via client");
        }
    }
}
