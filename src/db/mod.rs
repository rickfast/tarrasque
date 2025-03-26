pub mod data;
mod dialect;
pub mod error;
mod execution;
mod parse;
pub mod schema;
mod visitor;

use crate::cql::request::query::Query;
use crate::db::data::Value;
use crate::db::dialect::CassandraDialect;
use crate::db::error::DbError;
use crate::db::execution::execute;
use crate::db::parse::ParsedStatement::{Create, Select};
use crate::db::parse::{parse, ParsedStatement};
use crate::db::schema::Keyspace;
use fjall::{Config, Keyspace as FjallKeyspace};
use std::collections::HashMap;

static DIALECT: CassandraDialect = CassandraDialect {};

#[derive(Clone)]
pub struct Database {
    keyspaces: HashMap<String, Keyspace>,
    fjall: FjallKeyspace,
}

pub struct Results {
    pub result: Box<dyn Iterator<Item = Vec<Value>>>,
}

impl Database {
    pub fn new(keyspaces: HashMap<String, Keyspace>) -> Self {
        Self {
            keyspaces,
            fjall: FjallKeyspace::open(Config::new("/tmp/x")).unwrap(),
        }
    }

    pub fn query(self, query: Query) -> Result<Results, DbError> {
        let parsed_query = parse(query.query, self.keyspaces.get("default").unwrap().clone())?;

        match parsed_query {
            Select(query) => {
                let results = execute(&self.fjall, query)?;
                Ok(Results {
                    result: Box::new(results),
                })
            }
            Create(_) => {
                unimplemented!("Create table not implemented")
            }
        }
    }
}
