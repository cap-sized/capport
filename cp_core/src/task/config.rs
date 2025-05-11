use polars::prelude::Expr;
use serde::{ser, de, Deserialize, Serialize};

use crate::parser::expr::parse_str_to_col_expr;

pub trait Keyword<'a, T> {
    fn value(&'a self) -> Option<&'a T>;
    fn symbol(&'a self) -> Option<&'a str>;
    fn with_value(value: T) -> Self;
    fn with_symbol(symbol: &str) -> Self;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrKeyword {
    symbol: Option<String>,
    value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolarsExprKeyword {
    symbol: Option<String>,
    value: Option<Expr>,
}

impl Keyword<'_, String> for StrKeyword {
    fn with_value(value: String) -> Self {
        Self { symbol: None, value: Some(value) }
    }
    fn with_symbol(symbol: &str) -> Self {
        Self { symbol: Some(symbol.to_owned()), value: None }
    }
    fn value(&'_ self) -> Option<&'_ String> {
        self.value.as_ref()
    }
    fn symbol(&'_ self) -> Option<&str> {
        self.symbol.as_ref().map(|x| x.as_str())
    }
}

impl Keyword<'_, Expr> for PolarsExprKeyword  {
    fn with_value(value: Expr) -> Self {
        Self { symbol: None, value: Some(value) }
    }
    fn with_symbol(symbol: &str) -> Self {
        Self { symbol: Some(symbol.to_owned()), value: None }
    }
    fn value(&'_ self) -> Option<&'_ Expr> {
        self.value.as_ref()
    }
    fn symbol(&'_ self) -> Option<&str> {
        self.symbol.as_ref().map(|x| x.as_str())
    }
}

impl Serialize for StrKeyword {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer {
        
        if let Some(symbol) = self.symbol() {
            let full_sym = format!("${}", symbol);
            return serializer.serialize_str(full_sym.as_str());
        }
        if let Some(value) = self.value() {
            return serializer.serialize_str(value);
        }
        return Err(ser::Error::custom(format!("Invalid empty canonical StrKeyword")))
    }
}

impl <'de>Deserialize<'de> for StrKeyword {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de> {
        let full = String::deserialize(deserializer)?;
        let chars = full.chars().collect::<Vec<_>>();
        match chars.first() {
            Some('$') => Ok(StrKeyword::with_symbol(full[1..].trim().as_ref())),
            Some(_) => Ok(StrKeyword::with_value(full.trim().to_string())),
            None => Err(de::Error::custom(format!("Invalid empty StrKeyword: {}", full)))
        }
    }
}

impl <'de>Deserialize<'de> for PolarsExprKeyword {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de> {
        let full = String::deserialize(deserializer)?;
        let chars = full.chars().collect::<Vec<_>>();
        match chars.first() {
            Some('$') => Ok(PolarsExprKeyword::with_symbol(full[1..].trim().as_ref())),
            Some(_) => match parse_str_to_col_expr(full.trim().as_ref()) {
                Some(x) => Ok(PolarsExprKeyword::with_value(x)),
                None => Err(de::Error::custom(format!("Failed to parse as PolarsExprKeyword: {}", full)))
            }
            None => Err(de::Error::custom(format!("Invalid empty StrKeyword: {}", full)))
        }
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::col;
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
        let actual : StrKeywordExample = serde_yaml_ng::from_str(myconfig).unwrap();
        assert_eq!(actual, default_str_keyword());
    }

    #[test]
    fn pl_expr_keyword_de() {
        let myconfig = "{symbol: $mysymb, simple: test, complex: test.another}";
        let actual : PolarsExprKeywordExample = serde_yaml_ng::from_str(myconfig).unwrap();
        assert_eq!(actual, default_polars_expr_keyword());
    }
}
