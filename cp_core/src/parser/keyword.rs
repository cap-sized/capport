use std::{collections::HashMap, hash::Hash};

use polars::prelude::{Expr, lit};
use serde::{Deserialize, Deserializer, Serialize, de, ser};

use crate::parser::dtype::DType;
use crate::util::error::{CpError, CpResult};
use crate::{model::common::ModelFieldInfo, parser::expr::parse_str_to_col_expr};

use super::action::{ConcatAction, ExprAction, FormatAction};

/// The keyword trait is shared by task configuration "keywords" which contain either a value or a
/// symbol to be replaced by a value in the stage config (which invoke the task, which invokes the
/// underlying method the task configures).
pub trait Keyword<'a, T> {
    fn value(&'a self) -> Option<&'a T>;
    fn symbol(&'a self) -> Option<&'a str>;
    fn with_value(value: T) -> Self;
    fn with_symbol(symbol: &str) -> Self;
    fn and_symbol(self, symbol: &str) -> Self;
    fn insert_value(&mut self, value: T);
    fn insert_value_from_context(&mut self, context: &serde_yaml_ng::Mapping) -> CpResult<()>;
}

/// Keyword that a string value
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StrKeyword {
    symbol: Option<String>,
    value: Option<String>,
}

impl Hash for StrKeyword {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if let Some(sym) = self.symbol.as_ref() {
            sym.hash(state);
        } else if let Some(val) = self.value.as_ref() {
            val.hash(state);
        }
    }
}

/// Keyword that yields a polars expression. Valid expressions are strings or actions (parsed from map).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolarsExprKeyword {
    symbol: Option<String>,
    value: Option<Expr>,
}

/// Keyword that yields a column definition expression. Valid expressions are dtype or complete field info (parsed from map).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelFieldKeyword {
    symbol: Option<String>,
    value: Option<ModelFieldInfo>,
}

impl<'de> Deserialize<'de> for ModelFieldKeyword {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Helper {
            Short(StrKeyword),
            ValueDType(DType),
            Full(ModelFieldInfo),
        }
        match Helper::deserialize(deserializer)? {
            Helper::Short(str_kw) => {
                if let Some(sym) = str_kw.symbol() {
                    return Ok(ModelFieldKeyword::with_symbol(sym));
                }
                if let Some(val) = str_kw.value() {
                    return match serde_yaml_ng::from_str::<DType>(val) {
                        Ok(x) => Ok(ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(x))),
                        Err(e) => Err(de::Error::custom(format!(
                            "Model field dtype {:?} is not valid: {:?}",
                            str_kw, e
                        ))),
                    };
                }
                Err(de::Error::custom(format!(
                    "Model field dtype is not a symbol or value: {:?}",
                    str_kw
                )))
            }
            Helper::ValueDType(dtype) => Ok(ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(dtype))),
            Helper::Full(model_field_info) => Ok(ModelFieldKeyword::with_value(model_field_info)),
        }
    }
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
                        "To parse PolarsExprKeyword as an action, it must be a map with exactly one key. Found: {:?}",
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
                    Err(e) => {
                        log::error!("Error parsing action: {:?}", e);
                        return Err(de::Error::custom(format!("Bad action: {:?}", e)));
                    }
                };
                match expr {
                    Ok(x) => Ok(PolarsExprKeyword::with_value(x)),
                    Err(e) => {
                        log::error!("Error deriving expr from action: {:?}", e);
                        Err(de::Error::custom(format!("Bad action: {:?}", e)))
                    }
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
    fn and_symbol(mut self, symbol: &str) -> Self {
        let _ = self.symbol.insert(symbol.to_owned());
        self
    }
    fn insert_value(&mut self, value: String) {
        let _ = self.value.insert(value);
    }
    fn insert_value_from_context(&mut self, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        if self.value().is_some() {
            log::debug!("value already exists: {:?}", self.value());
            return Ok(());
        }
        let symbol = self.symbol().expect("no symbol or value");
        let value = match context.get(symbol) {
            Some(x) => x,
            None => {
                return Err(CpError::ConfigError(
                    "value not found for variable",
                    format!("value of `{}` not found in context: {:?}", symbol, context),
                ));
            }
        };
        match serde_yaml_ng::from_value::<String>(value.clone()) {
            Ok(x) => {
                let _ = self.value.insert(x);
            }
            Err(e) => {
                return Err(CpError::ConfigError(
                    "invalid value",
                    format!(
                        "value of `{}: {:?}` is not string or otherwise invalid: {:?}",
                        symbol, value, e
                    ),
                ));
            }
        }
        Ok(())
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
    fn and_symbol(mut self, symbol: &str) -> Self {
        let _ = self.symbol.insert(symbol.to_owned());
        self
    }
    fn insert_value(&mut self, value: Expr) {
        let _ = self.value.insert(value);
    }
    fn insert_value_from_context(&mut self, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        if self.value().is_some() {
            log::debug!("value already exists: {:?}", self.value());
            return Ok(());
        }
        let symbol = self.symbol().expect("no symbol or value");
        let value = match context.get(symbol) {
            Some(x) => x,
            None => {
                return Err(CpError::ConfigError(
                    "value not found for variable",
                    format!("value of `{}` not found in context: {:?}", symbol, context),
                ));
            }
        };
        match serde_yaml_ng::from_value::<PolarsExprKeyword>(value.clone()) {
            Ok(x) => match x.value() {
                Some(value) => {
                    let _ = self.value.insert(value.clone());
                }
                None => {
                    return Err(CpError::ConfigError(
                        "invalid value",
                        format!("substituted value of `{}: {:?}` doesn't exist", symbol, x),
                    ));
                }
            },
            Err(e) => {
                return Err(CpError::ConfigError(
                    "invalid value",
                    format!(
                        "value of `{}: {:?}` is not string or otherwise invalid: {:?}",
                        symbol, value, e
                    ),
                ));
            }
        }
        Ok(())
    }
}

impl Keyword<'_, ModelFieldInfo> for ModelFieldKeyword {
    fn with_value(value: ModelFieldInfo) -> Self {
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
    fn value(&'_ self) -> Option<&'_ ModelFieldInfo> {
        self.value.as_ref()
    }
    fn symbol(&'_ self) -> Option<&str> {
        self.symbol.as_deref()
    }
    fn and_symbol(mut self, symbol: &str) -> Self {
        let _ = self.symbol.insert(symbol.to_owned());
        self
    }
    fn insert_value(&mut self, value: ModelFieldInfo) {
        let _ = self.value.insert(value);
    }
    fn insert_value_from_context(&mut self, context: &serde_yaml_ng::Mapping) -> CpResult<()> {
        if self.value().is_some() {
            log::debug!("value already exists: {:?}", self.value());
            return Ok(());
        }
        log::debug!("symbol: {:?}", self.symbol());
        let symbol = self.symbol().expect("no symbol or value");
        let value = match context.get(symbol) {
            Some(x) => x,
            None => {
                return Err(CpError::ConfigError(
                    "value not found for variable",
                    format!("value of `{}` not found in context: {:?}", symbol, context),
                ));
            }
        };
        match serde_yaml_ng::from_value::<ModelFieldKeyword>(value.clone()) {
            Ok(x) => match x.value() {
                Some(value) => {
                    let _ = self.value.insert(value.clone());
                }
                None => {
                    return Err(CpError::ConfigError(
                        "invalid value",
                        format!("substituted value of `{}: {:?}` doesn't exist", symbol, x),
                    ));
                }
            },
            Err(e) => {
                return Err(CpError::ConfigError(
                    "invalid value",
                    format!(
                        "value of `{}: {:?}` is not string or otherwise invalid: {:?}",
                        symbol, value, e
                    ),
                ));
            }
        }
        Ok(())
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

    use polars::prelude::{DataType, col, concat_str, format_str, lit};
    use serde::{Deserialize, Serialize};

    use crate::{
        model::common::ModelFieldInfo,
        parser::{dtype::DType, model::ModelConstraint},
    };

    use super::{Keyword, ModelFieldKeyword, PolarsExprKeyword, StrKeyword};

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
        assert!(serde_yaml_ng::to_string(&StrKeyword::default()).is_err());
    }

    #[test]
    fn str_keyword_de() {
        let myconfig = "{symbol: $mysymb, value: val}";
        let actual: StrKeywordExample = serde_yaml_ng::from_str(myconfig).unwrap();
        assert_eq!(actual, default_str_keyword());
        // value is empty str
        assert!(serde_yaml_ng::from_str::<StrKeywordExample>("symbol: $mysymb, value: \"\"").is_err());
        // symbol is whitespace
        assert!(serde_yaml_ng::from_str::<StrKeywordExample>("symbol: \"\n\", value: value").is_err());
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
    fn pl_expr_keyword_invalid_action_de() {
        [
            "NotALiteral: test",
            "
not_another_action: 
    myargs: dontmatter
",
            "
format: 
    template: \"test {} {} {}\"
    columns: [too, many, actions]
concat:
    separator: \",\"
    columns: [too, many, actions]
",
            "
format: 
    template: \"test {} {} {}\"
    columns: [format, is, invalid, here, too, few, brackets]
",
            "[not, a, struct, or, str]",
        ]
        .iter()
        .for_each(|x| assert!(serde_yaml_ng::from_str::<PolarsExprKeyword>(x).is_err()));
    }

    #[test]
    fn valid_hash_str_keyword() {
        let mut map = HashMap::<StrKeyword, usize>::new();
        map.insert(StrKeyword::with_value("bro".to_string()), 1);
        map.insert(StrKeyword::with_symbol("bro"), 1);
        assert_eq!(map.len(), 2);
        map.insert(StrKeyword::with_value("bro".to_string()), 2);
        assert_eq!(map.len(), 2);
        assert_eq!(
            map.get(&StrKeyword::with_value("bro".to_string())).unwrap().to_owned(),
            2usize
        );
        assert_eq!(map.get(&StrKeyword::with_symbol("bro")).unwrap().to_owned(), 1usize);
    }

    #[test]
    fn valid_str_keyword_insert() {
        let mut strk = StrKeyword::with_symbol("test");
        strk.insert_value("actual_value".to_string());
        assert_eq!(strk.value().unwrap(), "actual_value");
    }

    #[test]
    fn valid_pl_expr_keyword_insert() {
        let mut strk = PolarsExprKeyword::with_symbol("test");
        strk.insert_value(col("actual_value"));
        assert_eq!(strk.value().unwrap(), &col("actual_value"));
    }

    #[test]
    fn valid_model_field_keyword_de() {
        let config = [
            "$test",
            "int64",
            "dtype: int64",
            "
dtype: str
constraints: [unique]
",
            "
dtype: str
constraints: [foreign, unique]
",
        ];
        let expected = [
            ModelFieldKeyword::with_symbol("test"),
            ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::Int64))),
            ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::Int64))),
            ModelFieldKeyword::with_value(ModelFieldInfo::new(DType(DataType::String), &[ModelConstraint::Unique])),
            ModelFieldKeyword::with_value(ModelFieldInfo::new(
                DType(DataType::String),
                &[ModelConstraint::Foreign, ModelConstraint::Unique],
            )),
        ];
        let actual = config
            .iter()
            .map(|x| serde_yaml_ng::from_str::<ModelFieldKeyword>(x).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn invalid_model_field_keyword_de() {
        let configs = [
            "bad",
            "",
            "
constraints: [unique]
",
            "
dtype: str
constraints: [not]
",
            "
dtype: str
constraints: [$nosymbols]
",
        ];
        for config in configs {
            assert!(serde_yaml_ng::from_str::<ModelFieldKeyword>(config).is_err());
        }
    }
}
