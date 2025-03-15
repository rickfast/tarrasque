pub mod data;
mod dialect;
mod visitor;
mod execution;
mod parse;
pub mod schema;

use crate::cql::request::query::Query;
use crate::cql::response::error::Error as CqlError;
use crate::cql::response::result::Result as CqlResult;
use crate::db::dialect::CassandraDialect;
use sqlparser::parser::Parser;

static DIALECT: CassandraDialect = CassandraDialect {};

#[derive(Copy, Clone)]
pub struct Database {}

impl Database {
    pub fn new() -> Self {
        Self {}
    }

    pub fn query(self, query: Query) -> Result<CqlResult, CqlError> {
        match Parser::parse_sql(&DIALECT, query.query.as_str()) {
            Ok(statements) => Err(CqlError::new(1, String::from("Unknown Statement"))),
            Err(error) => {
                unimplemented!()
            }
        }
    }
}
