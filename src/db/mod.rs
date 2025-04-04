pub mod data;
mod dialect;
pub mod error;
mod execution;
mod parse;
pub mod schema;
mod visitor;

use std::cell::RefCell;
use crate::cql::request::query::Query;
use crate::db::data::Value;
use crate::db::dialect::CassandraDialect;
use crate::db::error::DbError;
use crate::db::execution::execute_select;
use crate::db::parse::ParsedStatement::{Create, Select};
use crate::db::parse::{parse, ParsedStatement};
use crate::db::schema::{Keyspace, TableMetadata, Tables};
use fjall::{Config, Keyspace as FjallKeyspace};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

static DIALECT: CassandraDialect = CassandraDialect {};

pub struct Database<'db> {
    pub name: &'db str,
    pub tables: &'db Arc<Mutex<Tables>>,
    pub fjall: &'db FjallKeyspace,
}

pub struct Results {
    pub result: Box<dyn Iterator<Item = Vec<Value>>>,
}

impl<'db> Database<'_> {
    pub fn query(&self, query: Query) -> Result<Results, DbError> {
        let parsed_query = parse(query.query, self.tables)?;

        match parsed_query {
            Select(query) => {
                let results = execute_select(&self.fjall, query)?;
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
