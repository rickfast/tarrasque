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
use crate::db::execution::{execute_create_table, execute_select};
use crate::db::parse::parse;
use crate::db::parse::ParsedStatement::{Create, Select};
use crate::db::schema::Tables;
use fjall::Keyspace as FjallKeyspace;
use std::sync::Arc;
use tokio::sync::RwLock;

static DIALECT: CassandraDialect = CassandraDialect {};

pub struct Database<'db> {
    pub name: &'db str,
    pub tables: &'db Arc<RwLock<Tables>>,
    pub fjall: &'db FjallKeyspace,
}

pub struct Results {
    pub result: Box<dyn Iterator<Item = Vec<Value>>>,
}

impl<'db> Database<'_> {
    pub async fn query(&self, query: Query) -> Result<Results, DbError> {
        let parsed_query = parse(query.query, self.tables).await?;

        match parsed_query {
            Select(query) => {
                let results = execute_select(&self.fjall, query)?;
                Ok(Results {
                    result: Box::new(results),
                })
            }
            Create(table_metadata) => {
                let results =
                    execute_create_table(&table_metadata, &Arc::clone(self.tables)).await?;
                Ok(Results {
                    result: Box::new(results),
                })
            }
        }
    }
}
