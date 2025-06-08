use connectorx::prelude::CXQuery;
use polars::prelude::Expr;
use serde::Deserialize;

use crate::{
    db_url_emplace,
    model::common::{ModelConfig, ModelFields},
    model_emplace,
    parser::keyword::Keyword,
    pipeline::context::{DefaultPipelineContext, PipelineContext},
    util::error::CpResult,
};

use super::keyword::StrKeyword;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SqlConnection {
    pub url: Option<StrKeyword>,
    pub env_connection: Option<StrKeyword>, // use a preset
    pub dfname: StrKeyword,
    pub table: StrKeyword,
    pub sql: Option<StrKeyword>,
    pub model: Option<StrKeyword>,
    pub model_fields: Option<ModelFields>,
    pub strict: Option<bool>,
}

impl SqlConnection {
    pub fn emplace(
        &mut self,
        ctx: &DefaultPipelineContext,
        context: &serde_yaml_ng::Mapping,
        url_prefix: &str,
    ) -> CpResult<()> {
        db_url_emplace!(self, ctx, context, url_prefix);
        self.dfname.insert_value_from_context(context)?;
        if let Some(mut sql) = self.sql.take() {
            sql.insert_value_from_context(context)?;
            let _ = self.sql.insert(sql);
        }
        model_emplace!(self, ctx, context);
        Ok(())
    }

    pub fn src_query(&self) -> Vec<CXQuery> {
        let query = self.sql.clone().unwrap_or_else(|| {
            let selector = if self.strict.unwrap_or(false) && self.model_fields.is_some() {
                let vals = self
                    .model_fields
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|mf| mf.0.value().expect("sql.model_fields").as_str())
                    .collect::<Vec<_>>();
                vals.join(", ")
            } else {
                "*".to_owned()
            };
            StrKeyword::with_value(format!(
                "SELECT {} from {}",
                selector,
                self.table.value().expect("sql.table")
            ))
        });
        vec![CXQuery::from(query.value().expect("sql.sql").as_str())]
    }

    pub fn schema(&self) -> Option<Vec<Expr>> {
        self.model_fields.as_ref().map(|x| {
            ModelConfig {
                label: "".to_string(),
                fields: x.clone(),
            }
            .columns()
            .expect("failed to build schema")
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use polars::prelude::DataType;

    use crate::{
        model::common::ModelFieldInfo,
        parser::{
            dtype::DType,
            keyword::{Keyword, ModelFieldKeyword, StrKeyword},
        },
    };

    use super::SqlConnection;

    fn get_connections() -> [SqlConnection; 2] {
        [
            SqlConnection {
                table: StrKeyword::with_symbol("table"),
                sql: None,
                env_connection: None,
                url: None,
                model: None,
                dfname: StrKeyword::with_value("output".to_owned()),
                model_fields: None,
                strict: Some(true),
            },
            SqlConnection {
                table: StrKeyword::with_value("table".to_string()),
                sql: Some(StrKeyword::with_symbol("test")),
                env_connection: Some(StrKeyword::with_value("fallback".to_owned())),
                url: Some(StrKeyword::with_symbol("first_priority")),
                model: Some(StrKeyword::with_value("mymod".to_owned())),
                dfname: StrKeyword::with_symbol("actual"),
                model_fields: Some(HashMap::from([(
                    StrKeyword::with_symbol("test"),
                    ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::Int8))),
                )])),
                strict: None,
            },
        ]
    }

    fn get_connection_configs() -> [&'static str; 2] {
        [
            "
table: $table
# sql query is not used here
dfname: output
strict: true
",
            "
dfname: $actual
# sql query is not used here
sql: $test
table: table
url: $first_priority
env_connection: fallback
model: mymod
model_fields: 
    $test: int8
",
        ]
    }

    #[test]
    fn valid_connection_config() {
        let configs = get_connection_configs();
        let locals = get_connections();
        for i in 0..2 {
            assert_eq!(locals[i], serde_yaml_ng::from_str::<SqlConnection>(configs[i]).unwrap());
        }
    }
}
