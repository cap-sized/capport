use polars::frame::DataFrame;
use polars::prelude::SortMultipleOptions;

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
