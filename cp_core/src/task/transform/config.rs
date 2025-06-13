use std::collections::HashMap;

use serde::Deserialize;

use crate::parser::{
    dtype::DType,
    filter_fields::FilterFields,
    jtype::JType,
    keyword::{PolarsExprKeyword, StrKeyword},
};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RootTransformConfig {
    pub label: String,
    pub input: StrKeyword,
    pub output: StrKeyword,
    pub steps: Vec<serde_yaml_ng::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SelectTransformConfig {
    pub select: HashMap<StrKeyword, PolarsExprKeyword>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct WithColTransformConfig {
    pub with_columns: HashMap<StrKeyword, PolarsExprKeyword>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct JoinTransformConfig {
    pub join: _JoinTransformConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct _JoinTransformConfig {
    pub left_prefix: Option<StrKeyword>,
    pub left_on: Vec<StrKeyword>,
    /// the frame on the right to join on
    pub right: StrKeyword,
    pub right_select: Option<HashMap<StrKeyword, PolarsExprKeyword>>,
    pub right_prefix: Option<StrKeyword>,
    pub right_on: Vec<StrKeyword>,
    pub how: JType,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DropTransformConfig {
    pub drop: Vec<PolarsExprKeyword>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SqlTransformConfig {
    pub sql: String,
    pub sql_context: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnnestTransformConfig {
    pub unnest_struct: Option<PolarsExprKeyword>,
    pub unnest_list: Option<PolarsExprKeyword>,
    pub unnest_list_of_struct: Option<PolarsExprKeyword>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct UniformIdTypeConfig {
    pub uniform_id_type: FilterFields<Option<DType>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TimeConvertConfig {
    pub time: FilterFields<String>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use polars::prelude::{JoinType, col};

    use crate::{
        parser::{
            jtype::JType,
            keyword::{Keyword, PolarsExprKeyword, StrKeyword},
        },
        task::transform::config::{_JoinTransformConfig, UnnestTransformConfig},
    };

    use super::{DropTransformConfig, JoinTransformConfig, SelectTransformConfig, SqlTransformConfig};

    #[test]
    fn parse_transform_select() {
        let config = "
select:
    key: value
    key2: $symbol
    $symbol: value
    nested_key: nested.value
    123: isvalid
    nest.k: left_is_str
";
        let actual: SelectTransformConfig = serde_yaml_ng::from_str(config).unwrap();
        assert_eq!(
            actual,
            SelectTransformConfig {
                select: HashMap::from([
                    (
                        StrKeyword::with_value("key".to_owned()),
                        PolarsExprKeyword::with_value(col("value"))
                    ),
                    (
                        StrKeyword::with_value("key2".to_owned()),
                        PolarsExprKeyword::with_symbol("symbol")
                    ),
                    (
                        StrKeyword::with_symbol("symbol"),
                        PolarsExprKeyword::with_value(col("value"))
                    ),
                    (
                        StrKeyword::with_value("nested_key".to_owned()),
                        PolarsExprKeyword::with_value(col("nested").struct_().field_by_name("value"))
                    ),
                    (
                        StrKeyword::with_value("123".to_owned()),
                        PolarsExprKeyword::with_value(col("isvalid"))
                    ),
                    (
                        StrKeyword::with_value("nest.k".to_owned()),
                        PolarsExprKeyword::with_value(col("left_is_str"))
                    ),
                ])
            }
        );
    }

    #[test]
    fn parse_transform_join_basic_value() {
        let config = "
join:
    right: BASIC
    left_on: [col]
    right_on: [my_col]
    how: left
";
        let actual: JoinTransformConfig = serde_yaml_ng::from_str(config).unwrap();
        assert_eq!(
            actual,
            JoinTransformConfig {
                join: _JoinTransformConfig {
                    right: StrKeyword::with_value("BASIC".to_owned()),
                    left_on: vec![StrKeyword::with_value("col".to_owned())],
                    right_on: vec![StrKeyword::with_value("my_col".to_owned())],
                    left_prefix: None,
                    right_prefix: None,
                    right_select: None,
                    how: JType(JoinType::Left)
                }
            }
        );
    }

    #[test]
    fn parse_transform_join_basic_symbol() {
        let config = "
join:
    right: $BASIC
    left_on: [col, another]
    right_on: [$my_col, another]
    how: right
";
        let actual: JoinTransformConfig = serde_yaml_ng::from_str(config).unwrap();
        assert_eq!(
            actual,
            JoinTransformConfig {
                join: _JoinTransformConfig {
                    right: StrKeyword::with_symbol("BASIC"),
                    left_on: vec![
                        StrKeyword::with_value("col".to_owned()),
                        StrKeyword::with_value("another".to_owned())
                    ],
                    right_on: vec![
                        StrKeyword::with_symbol("my_col"),
                        StrKeyword::with_value("another".to_owned())
                    ],
                    left_prefix: None,
                    right_prefix: None,
                    right_select: None,
                    how: JType(JoinType::Right)
                }
            }
        );
    }

    #[test]
    fn parse_transform_join_with_prefixes() {
        let config = "
join:
    right: $BASIC
    left_on: [col]
    right_on: [$my_col]
    left_prefix: orig
    right_prefix: $orig
    how: inner
";
        let actual: JoinTransformConfig = serde_yaml_ng::from_str(config).unwrap();
        assert_eq!(
            actual,
            JoinTransformConfig {
                join: _JoinTransformConfig {
                    right: StrKeyword::with_symbol("BASIC"),
                    left_on: vec![StrKeyword::with_value("col".to_owned())],
                    right_on: vec![StrKeyword::with_symbol("my_col")],
                    left_prefix: Some(StrKeyword::with_value("orig".to_owned())),
                    right_prefix: Some(StrKeyword::with_symbol("orig")),
                    right_select: None,
                    how: JType(JoinType::Inner)
                }
            }
        );
    }

    #[test]
    fn parse_transform_join_with_right_select() {
        let config = "
join:
    right: $BASIC
    left_on: [col]
    right_on: [$my_col]
    right_select:
        test: $one
        $another: two
    how: full
";
        let actual: JoinTransformConfig = serde_yaml_ng::from_str(config).unwrap();
        assert_eq!(
            actual,
            JoinTransformConfig {
                join: _JoinTransformConfig {
                    right: StrKeyword::with_symbol("BASIC"),
                    left_on: vec![StrKeyword::with_value("col".to_owned())],
                    right_on: vec![StrKeyword::with_symbol("my_col")],
                    left_prefix: None,
                    right_prefix: None,
                    right_select: Some(HashMap::from([
                        (
                            StrKeyword::with_value("test".to_owned()),
                            PolarsExprKeyword::with_symbol("one")
                        ),
                        (
                            StrKeyword::with_symbol("another"),
                            PolarsExprKeyword::with_value(col("two"))
                        ),
                    ])),
                    how: JType(JoinType::Full)
                }
            }
        );
    }

    #[test]
    fn parse_transform_drop() {
        let config = "drop: [first, $second, third.fourth]";
        let actual: DropTransformConfig = serde_yaml_ng::from_str(config).unwrap();
        assert_eq!(
            actual,
            DropTransformConfig {
                drop: vec![
                    PolarsExprKeyword::with_value(col("first")),
                    PolarsExprKeyword::with_symbol("second"),
                    PolarsExprKeyword::with_value(col("third").struct_().field_by_name("fourth")),
                ]
            }
        );
    }

    #[test]
    fn parse_transform_sql() {
        let config = "sql: select col from data";
        let actual: SqlTransformConfig = serde_yaml_ng::from_str(config).unwrap();

        assert_eq!(
            actual,
            SqlTransformConfig {
                sql: "select col from data".to_string(),
                sql_context: None
            }
        );
    }

    #[test]
    fn parse_transform_unnest() {
        let config = "unnest_list: $col";
        let actual: UnnestTransformConfig = serde_yaml_ng::from_str(config).unwrap();

        assert_eq!(
            actual,
            UnnestTransformConfig {
                unnest_list: Some(PolarsExprKeyword::with_symbol("col")),
                unnest_struct: None,
                unnest_list_of_struct: None,
            }
        );
    }
}
