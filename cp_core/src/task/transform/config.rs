use std::collections::HashMap;

use serde::Deserialize;

use crate::parser::keyword::{PolarsExprKeyword, StrKeyword};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TransformTaskConfig {
    pub label: String,
    pub input: StrKeyword,
    pub output: StrKeyword,
    pub stages: Vec<serde_yaml_ng::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SelectTransformConfig {
    pub select: HashMap<StrKeyword, PolarsExprKeyword>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct JoinTransformConfig {
    pub join: _JoinTransformConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct _JoinTransformConfig {
    pub left_prefix: Option<StrKeyword>,
    pub left_on: StrKeyword,
    /// the frame on the right to join on
    pub right: StrKeyword,
    pub right_select: Option<HashMap<StrKeyword, PolarsExprKeyword>>,
    pub right_prefix: Option<StrKeyword>,
    pub right_on: StrKeyword,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DropTransformConfig {
    pub drop: Vec<PolarsExprKeyword>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use polars::prelude::col;

    use crate::{parser::keyword::{Keyword, PolarsExprKeyword, StrKeyword}, task::transform::config::_JoinTransformConfig};

    use super::{DropTransformConfig, JoinTransformConfig, SelectTransformConfig};

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
    left_on: col
    right_on: my_col
";
        let actual: JoinTransformConfig = serde_yaml_ng::from_str(config).unwrap();
        assert_eq!(
            actual,
            JoinTransformConfig {
                join: _JoinTransformConfig {
                    right: StrKeyword::with_value("BASIC".to_owned()),
                    left_on: StrKeyword::with_value("col".to_owned()),
                    right_on: StrKeyword::with_value("my_col".to_owned()),
                    left_prefix: None,
                    right_prefix: None,
                    right_select: None,
                }
            }
        );
    }

    #[test]
    fn parse_transform_join_basic_symbol() {
        let config = "
join:
    right: $BASIC
    left_on: col
    right_on: $my_col
";
        let actual: JoinTransformConfig = serde_yaml_ng::from_str(config).unwrap();
        assert_eq!(
            actual,
            JoinTransformConfig {
                join: _JoinTransformConfig {
                    right: StrKeyword::with_symbol("BASIC"),
                    left_on: StrKeyword::with_value("col".to_owned()),
                    right_on: StrKeyword::with_symbol("my_col"),
                    left_prefix: None,
                    right_prefix: None,
                    right_select: None,
                }
            }
        );
    }

    #[test]
    fn parse_transform_join_with_prefixes() {
        let config = "
join:
    right: $BASIC
    left_on: col
    right_on: $my_col
    left_prefix: orig
    right_prefix: $orig
";
        let actual: JoinTransformConfig = serde_yaml_ng::from_str(config).unwrap();
        assert_eq!(
            actual,
            JoinTransformConfig {
                join: _JoinTransformConfig {
                    right: StrKeyword::with_symbol("BASIC"),
                    left_on: StrKeyword::with_value("col".to_owned()),
                    right_on: StrKeyword::with_symbol("my_col"),
                    left_prefix: Some(StrKeyword::with_value("orig".to_owned())),
                    right_prefix: Some(StrKeyword::with_symbol("orig")),
                    right_select: None,
                }
            }
        );
    }

    #[test]
    fn parse_transform_join_with_right_select() {
        let config = "
join:
    right: $BASIC
    left_on: col
    right_on: $my_col
    right_select:
        test: $one
        $another: two
";
        let actual: JoinTransformConfig = serde_yaml_ng::from_str(config).unwrap();
        assert_eq!(
            actual,
            JoinTransformConfig {
                join: _JoinTransformConfig {
                    right: StrKeyword::with_symbol("BASIC"),
                    left_on: StrKeyword::with_value("col".to_owned()),
                    right_on: StrKeyword::with_symbol("my_col"),
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
}
