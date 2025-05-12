use std::{collections::HashMap, hash::Hash};

use polars::prelude::{Expr, lit};
use serde::{Deserialize, Deserializer, Serialize, de, ser};

use crate::parser::expr::parse_str_to_col_expr;
use crate::util::error::CpResult;

use super::action::{ConcatAction, FormatAction, TransformAction};

pub trait Keyword<'a, T> {
    fn value(&'a self) -> Option<&'a T>;
    fn symbol(&'a self) -> Option<&'a str>;
    fn with_value(value: T) -> Self;
    fn with_symbol(symbol: &str) -> Self;
    fn insert_value(&mut self, value: T);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrKeyword {
    symbol: Option<String>,
    value: Option<String>,
}

impl Hash for StrKeyword {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if let Some(sym) = self.symbol.as_ref() {
            sym.hash(state);
        } else if let Some(val) = self.symbol.as_ref() {
            val.hash(state);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolarsExprKeyword {
    symbol: Option<String>,
    value: Option<Expr>,
}

impl<'de> Deserialize<'de> for PolarsExprKeyword {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Helper {
            Short(String),
            Full(HashMap<String, serde_yaml_ng::Value>),
        }
        match Helper::deserialize(deserializer)? {
            Helper::Short(full) => {
                let chars = full.chars().collect::<Vec<_>>();
                match chars.first() {
                    Some('$') => Ok(PolarsExprKeyword::with_symbol(full[1..].trim())),
                    Some(_) => match parse_str_to_col_expr(full.trim()) {
                        Some(x) => Ok(PolarsExprKeyword::with_value(x)),
                        None => Err(de::Error::custom(format!(
                            "Failed to parse as PolarsExprKeyword: {}",
                            full
                        ))),
                    },
                    None => Err(de::Error::custom(format!("Invalid empty StrKeyword: {}", full))),
                }
            }
            Helper::Full(full) => {
                if full.len() != 1 {
                    return Err(de::Error::custom(format!(
                        "To parse PolarsExprKeyword as an action, it must be a map with exactly one pair. Found: {:?}",
                        full
                    )));
                }

                let mut kvpair = full.into_iter().collect::<Vec<_>>();
                let (action_name, action_args) = kvpair.pop().unwrap();
                let action: Result<CpResult<Expr>, serde_yaml_ng::Error> = match action_name.as_str() {
                    "format" => serde_yaml_ng::from_value::<FormatAction>(action_args).map(|x| x.expr()),
                    "concat" => serde_yaml_ng::from_value::<ConcatAction>(action_args).map(|x| x.expr()),
                    "uint64" => serde_yaml_ng::from_value::<u64>(action_args).map(|x| Ok(lit(x))),
                    "int64" => serde_yaml_ng::from_value::<i64>(action_args).map(|x| Ok(lit(x))),
                    "str" => serde_yaml_ng::from_value::<String>(action_args).map(|x| Ok(lit(x))),
                    x => {
                        return Err(de::Error::custom(format!("Unrecognized action: {}", x)));
                    }
                };
                let expr: CpResult<Expr> = match action {
                    Ok(x) => x,
                    Err(e) => return Err(de::Error::custom(format!("Bad action: {:?}", e))),
                };
                match expr {
                    Ok(x) => Ok(PolarsExprKeyword::with_value(x)),
                    Err(e) => Err(de::Error::custom(format!("Bad action: {:?}", e))),
                }
            }
        }
    }
}

impl Keyword<'_, String> for StrKeyword {
    fn with_value(value: String) -> Self {
        Self {
            symbol: None,
            value: Some(value),
        }
    }
    fn with_symbol(symbol: &str) -> Self {
        Self {
            symbol: Some(symbol.to_owned()),
            value: None,
        }
    }
    fn value(&'_ self) -> Option<&'_ String> {
        self.value.as_ref()
    }
    fn symbol(&'_ self) -> Option<&str> {
        self.symbol.as_deref()
    }
    fn insert_value(&mut self, value: String) {
        let _ = self.value.insert(value);
    }
}

impl Keyword<'_, Expr> for PolarsExprKeyword {
    fn with_value(value: Expr) -> Self {
        Self {
            symbol: None,
            value: Some(value),
        }
    }
    fn with_symbol(symbol: &str) -> Self {
        Self {
            symbol: Some(symbol.to_owned()),
            value: None,
        }
    }
    fn value(&'_ self) -> Option<&'_ Expr> {
        self.value.as_ref()
    }
    fn symbol(&'_ self) -> Option<&str> {
        self.symbol.as_deref()
    }
    fn insert_value(&mut self, value: Expr) {
        let _ = self.value.insert(value);
    }
}

impl Serialize for StrKeyword {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Some(symbol) = self.symbol() {
            let full_sym = format!("${}", symbol);
            return serializer.serialize_str(full_sym.as_str());
        }
        if let Some(value) = self.value() {
            return serializer.serialize_str(value);
        }
        Err(ser::Error::custom("Invalid empty canonical StrKeyword".to_string()))
    }
}

impl<'de> Deserialize<'de> for StrKeyword {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let full = String::deserialize(deserializer)?;
        let chars = full.chars().collect::<Vec<_>>();
        match chars.first() {
            Some('$') => Ok(StrKeyword::with_symbol(full[1..].trim())),
            Some(_) => Ok(StrKeyword::with_value(full.trim().to_string())),
            None => Err(de::Error::custom(format!("Invalid empty StrKeyword: {}", full))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use polars::prelude::{col, concat_str, format_str, lit};
    use serde::{Deserialize, Serialize};

    use super::{Keyword, PolarsExprKeyword, StrKeyword};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct StrKeywordExample {
        pub symbol: StrKeyword,
        pub value: StrKeyword,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct PolarsExprKeywordExample {
        pub symbol: PolarsExprKeyword,
        pub simple: PolarsExprKeyword,
        pub complex: PolarsExprKeyword,
    }

    fn default_str_keyword() -> StrKeywordExample {
        StrKeywordExample {
            symbol: StrKeyword::with_symbol("mysymb"),
            value: StrKeyword::with_value(String::from("val")),
        }
    }

    fn default_polars_expr_keyword() -> PolarsExprKeywordExample {
        PolarsExprKeywordExample {
            symbol: PolarsExprKeyword::with_symbol("mysymb"),
            simple: PolarsExprKeyword::with_value(col("test")),
            complex: PolarsExprKeyword::with_value(col("test").struct_().field_by_name("another")),
        }
    }

    #[test]
    fn str_keyword_ser() {
        let field = default_str_keyword();
        assert_eq!(serde_yaml_ng::to_string(&field.value).unwrap().trim(), "val");
        assert_eq!(serde_yaml_ng::to_string(&field.symbol).unwrap().trim(), "$mysymb");
    }

    #[test]
    fn str_keyword_de() {
        let myconfig = "{symbol: $mysymb, value: val}";
        let actual: StrKeywordExample = serde_yaml_ng::from_str(myconfig).unwrap();
        assert_eq!(actual, default_str_keyword());
    }

    #[test]
    fn pl_expr_keyword_str_de() {
        let myconfig = "{symbol: $mysymb, simple: test, complex: test.another}";
        let actual: PolarsExprKeywordExample = serde_yaml_ng::from_str(myconfig).unwrap();
        assert_eq!(actual, default_polars_expr_keyword());
    }

    #[test]
    fn pl_expr_keyword_format_action_de() {
        let action_config = "
format:
    template: \"Hi {} {} {}\"
    columns: [one, two, three.not.nested]
";
        let action: PolarsExprKeyword = serde_yaml_ng::from_str(action_config).unwrap();
        let expected_expr = format_str("Hi {} {} {}", ["one", "two", "three.not.nested"].map(col)).unwrap();
        assert_eq!(action.value().unwrap(), &expected_expr);
    }

    #[test]
    fn pl_expr_keyword_concat_action_de() {
        let action_config = "
concat:
    separator: \",\"
    columns: [one, two, three.not.nested]
";
        let action: PolarsExprKeyword = serde_yaml_ng::from_str(action_config).unwrap();
        let expected_expr = concat_str(["one", "two", "three.not.nested"].map(col), ",", false);
        assert_eq!(action.value().unwrap(), &expected_expr);
    }

    #[test]
    fn pl_expr_keyword_literal_action_de() {
        let action_config = "
- uint64: 34
- int64: 16
- str: test
";
        let actions: Vec<PolarsExprKeyword> = serde_yaml_ng::from_str(action_config).unwrap();
        assert_eq!(actions[0].value().unwrap(), &lit(34u64));
        assert_eq!(actions[1].value().unwrap(), &lit(16i64));
        assert_eq!(actions[2].value().unwrap(), &lit("test"));
    }

    #[test]
    fn valid_hash_str_keyword() {
        let mut map = HashMap::<StrKeyword, usize>::new();
        map.insert(StrKeyword::with_value("bro".to_string()), 1);
        map.insert(StrKeyword::with_symbol("$bro"), 1);
        assert_eq!(map.len(), 2);
        map.insert(StrKeyword::with_value("bro".to_string()), 2);
        assert_eq!(map.len(), 2);
        assert_eq!(
            map.get(&StrKeyword::with_value("bro".to_string())).unwrap().to_owned(),
            2usize
        );
    }
}
