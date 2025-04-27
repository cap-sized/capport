use crate::{
    transform::sql::SqlTransform,
    util::error::{CpError, CpResult},
};

pub fn parse_sql_transform(node: serde_yaml_ng::Value) -> CpResult<SqlTransform> {
    let sql = node.as_str();
    if sql.is_none() {
        return Err(CpError::ConfigError(
            "Invalid value for sql field",
            format!("Must be of type string, received{:?}", sql),
        ));
    }
    Ok(SqlTransform {
        sql: sql.unwrap().to_string(),
    })
}
