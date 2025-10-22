use serde::Deserialize;

use crate::{
    model::common::ModelFields, model_emplace, pipeline::context::{DefaultPipelineContext, PipelineContext}, util::error::{CpError, CpResult}
};

use super::{connection::NetworkConnection, keyword::{Keyword, StrKeyword}, merge_type::MergeTypeEnum};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SqlConnection {
    pub label: StrKeyword,
    pub user: StrKeyword,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SqlReqConnection {
    pub conn: SqlConnection,
    pub query_column: StrKeyword,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SqlGetModelConnection {
    pub conn: SqlConnection,
    pub table: StrKeyword,
    pub model: Option<StrKeyword>,
    pub model_fields: Option<ModelFields>,
    pub extra_clauses: Option<StrKeyword>
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SqlSendConnection {
    pub conn: SqlConnection,
    pub table: StrKeyword,
    pub input: StrKeyword,
    pub merge_type: Option<MergeTypeEnum>
}

impl SqlConnection {
    pub fn emplace(
        &mut self, 
        context: &serde_yaml_ng::Mapping,
    ) -> CpResult<()> {
        self.label.insert_value_from_context(context)?;
        self.user.insert_value_from_context(context)?;
        Ok(())
    }
    pub fn get_connection(&self, ctx: &DefaultPipelineContext) -> CpResult<NetworkConnection> {
        let user = match self.user.value() {
            Some(x) => x,
            None => return Err(CpError::ConfigError("Missing user value in SqlReqConnection", self.user.symbol().unwrap_or("<symbol>").to_owned()))
        };
        let conn = match self.label.value() {
            Some(x) => x,
            None => return Err(CpError::ConfigError("Missing connection value in SqlReqConnection", self.user.symbol().unwrap_or("<symbol>").to_owned()))
        };
        ctx.get_connection(conn, user)
    }
}

impl SqlGetModelConnection {
    pub fn emplace(
        &mut self, 
        ctx: &DefaultPipelineContext, 
        context: &serde_yaml_ng::Mapping,
    ) -> CpResult<()> {
        self.conn.emplace(context)?;
        self.table.insert_value_from_context(context)?;
        if let Some(mut extra_clauses) = self.extra_clauses.take() {
            extra_clauses.insert_value_from_context(context)?;
            let _ = self.extra_clauses.insert(extra_clauses);
        }
        model_emplace!(self, ctx, context);
        Ok(())
    }
    pub fn get_connection(&self, ctx: &DefaultPipelineContext) -> CpResult<NetworkConnection> {
        self.conn.get_connection(ctx)
    }
}

impl SqlReqConnection {
    pub fn emplace(
        &mut self,
        context: &serde_yaml_ng::Mapping,
    ) -> CpResult<()> {
        self.conn.emplace(context)?;
        self.query_column.insert_value_from_context(context)?;
        Ok(())
    }
    pub fn get_connection(&self, ctx: &DefaultPipelineContext) -> CpResult<NetworkConnection> {
        self.conn.get_connection(ctx)
    }
}

impl SqlSendConnection {
    pub fn emplace(
        &mut self,
        context: &serde_yaml_ng::Mapping,
    ) -> CpResult<()> {
        self.conn.emplace(context)?;
        self.table.insert_value_from_context(context)?;
        self.input.insert_value_from_context(context)?;
        Ok(())
    }
    pub fn get_connection(&self, ctx: &DefaultPipelineContext) -> CpResult<NetworkConnection> {
        self.conn.get_connection(ctx)
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::DataType;

    use crate::{model::common::{ModelFieldInfo, ModelFields}, parser::{dtype::DType, keyword::{Keyword, ModelFieldKeyword, StrKeyword}, merge_type::MergeTypeEnum, sql_connection::{SqlConnection, SqlGetModelConnection, SqlReqConnection, SqlSendConnection}}};


    #[test]
    fn valid_sql_req_connection_config() {
        let config = "
conn: 
    label: abc
    user: $user
query_column: $col
        ";
        assert_eq!(SqlReqConnection {
            conn: SqlConnection {
                label: StrKeyword::with_value("abc".to_owned()),
                user: StrKeyword::with_symbol("user"),
            },
            query_column: StrKeyword::with_symbol("col"),
        }, serde_yaml_ng::from_str(config).unwrap());
    }

    #[test]
    fn valid_sql_send_connection_config() {
        let config = "
conn: 
    label: abc
    user: $user
table: data
input: data # this is allowed!
merge_type: INsert
        ";
        assert_eq!(SqlSendConnection {
            conn: SqlConnection {
                label: StrKeyword::with_value("abc".to_owned()),
                user: StrKeyword::with_symbol("user"),
            },
            table: StrKeyword::with_value("data".to_owned()),
            input: StrKeyword::with_value("data".to_owned()),
            merge_type: Some(MergeTypeEnum::Insert)
        }, serde_yaml_ng::from_str(config).unwrap());
    }

    #[test]
    fn valid_sql_model_connection_config() {
        let config = "
conn: 
    label: abc
    user: $user
table: data
model_fields:
    a: $to_fill
    b: str
        ";
        assert_eq!(SqlGetModelConnection {
            conn: SqlConnection {
                label: StrKeyword::with_value("abc".to_owned()),
                user: StrKeyword::with_symbol("user"),
            },
            table: StrKeyword::with_value("data".to_owned()),
            model : None,
            model_fields: Some(ModelFields::from([
                    (StrKeyword::with_value("a".to_owned()), ModelFieldKeyword::with_symbol("to_fill")),
                    (StrKeyword::with_value("b".to_owned()), ModelFieldKeyword::with_value(ModelFieldInfo::with_dtype(DType(DataType::String)))),
            ])),
            extra_clauses: None
        }, serde_yaml_ng::from_str(config).unwrap());
    }
}
