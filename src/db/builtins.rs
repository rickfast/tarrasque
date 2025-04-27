use crate::db::data::Value;
use crate::db::error::{DbError, ErrorCode};
use std::collections::HashMap;

pub type Function = fn(Vec<Option<Value>>) -> Value;

pub type FilterFunction = fn(Vec<Option<Value>>) -> Result<bool, DbError>;

pub fn eq(args: Vec<Option<Value>>) -> Result<bool, DbError> {
    if args.len() != 2 {
        return Err(DbError::new(
            ErrorCode::Invalid,
            "equals function requires exactly 2 arguments".to_string(),
        ));
    }

    match (args[0].as_ref(), args[1].as_ref()) {
        (Some(a), Some(b)) => Ok(a.eq(b)),
        (None, Some(_)) => Ok(false),
        (Some(_), None) => Ok(false),
        (None, None) => Ok(true),
    }
}

pub fn neq(args: Vec<Option<Value>>) -> Result<bool, DbError> {
    eq(args).map(|result| !result)
}

fn build_filters() -> HashMap<String, FilterFunction> {
    let mut filters: HashMap<String, FilterFunction> = HashMap::new();

    filters.insert("eq".to_string(), eq);

    filters
}
