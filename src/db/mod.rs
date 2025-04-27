mod builtins;
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
use crate::db::execution::{execute_create_table, execute_insert, execute_select};
use crate::db::parse::parse;
use crate::db::parse::ParsedStatement::{Create, Insert, Select};
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
    pub result: Box<dyn Iterator<Item = Vec<Option<Value>>>>,
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
            Insert(insert) => {
                let results = execute_insert(&self.fjall, insert)?;
                Ok(Results {
                    result: Box::new(results),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cql::request::query::{Consistency, QueryOptions};
    use fjall::Config;

    #[tokio::test]
    async fn query_create_insert_select() {
        // Step 1: Set up the database
        let tables = Arc::new(RwLock::new(Tables::new()));
        let fjall = FjallKeyspace::open(Config::new("/tmp/test_db")).unwrap();
        let database = Database {
            name: "test_db",
            tables: &tables,
            fjall: &fjall,
        };

        // Step 2: Create a table
        let create_table_query = "
            CREATE TABLE users (
                id INT PRIMARY KEY,
                name VARCHAR,
                age INT,
                is_active BOOLEAN
            )
        ";
        let create_result = database
            .query(Query {
                query: create_table_query.to_string(),
                query_options: QueryOptions {
                    consistency: Consistency::One,
                    values: None,
                    skip_metadata: false,
                    page_size: None,
                    paging_state: None,
                    timestamp: None,
                },
            })
            .await;
        assert!(create_result.is_ok());

        // Step 3: Insert data
        let insert_query_1 = "
            INSERT INTO users (id, name, age, is_active)
            VALUES (1, 'Alice', 30, true)
        ";
        let insert_result_1 = database
            .query(Query {
                query: insert_query_1.to_string(),
                query_options: QueryOptions {
                    consistency: Consistency::One,
                    values: None,
                    skip_metadata: false,
                    page_size: None,
                    paging_state: None,
                    timestamp: None,
                },
            })
            .await;
        assert!(insert_result_1.is_ok());

        let insert_query_2 = "
            INSERT INTO users (id, name, age, is_active)
            VALUES (2, 'Bob', NULL, NULL)
        ";
        let insert_result_2 = database
            .query(Query {
                query: insert_query_2.to_string(),
                query_options: QueryOptions {
                    consistency: Consistency::One,
                    values: None,
                    skip_metadata: false,
                    page_size: None,
                    paging_state: None,
                    timestamp: None,
                },
            })
            .await;
        assert!(insert_result_2.is_ok());

        // Step 4: Query the data
        let select_query = "SELECT id, name, age, is_active FROM users";
        let select_result = database
            .query(Query {
                query: select_query.to_string(),
                query_options: QueryOptions {
                    consistency: Consistency::One,
                    values: None,
                    skip_metadata: false,
                    page_size: None,
                    paging_state: None,
                    timestamp: None,
                },
            })
            .await;
        assert!(select_result.is_ok());

        let mut result_iter = select_result.unwrap().result;

        // Step 5: Verify the data
        let row1 = result_iter.next().unwrap();
        assert_eq!(row1[0], Some(Value::Int(1)));
        assert_eq!(row1[1], Some(Value::Varchar("Alice".to_string())));
        assert_eq!(row1[2], Some(Value::Int(30)));
        assert_eq!(row1[3], Some(Value::Boolean(true)));

        let row2 = result_iter.next().unwrap();
        assert_eq!(row2[0], Some(Value::Int(2)));
        assert_eq!(row2[1], Some(Value::Varchar("Bob".to_string())));
        assert_eq!(row2[2], None); // Null value
        assert_eq!(row2[3], None); // Null value
    }

    #[tokio::test]
    async fn query_single_record() {
        // Step 1: Set up the database
        let tables = Arc::new(RwLock::new(Tables::new()));
        let fjall = FjallKeyspace::open(Config::new("/tmp/test_db_single")).unwrap();
        let database = Database {
            name: "test_db",
            tables: &tables,
            fjall: &fjall,
        };

        // Step 2: Create a table
        let create_table_query = "
            CREATE TABLE users (
                id INT PRIMARY KEY,
                name VARCHAR,
                age INT,
                is_active BOOLEAN
            )
        ";
        let create_result = database
            .query(Query {
                query: create_table_query.to_string(),
                query_options: QueryOptions {
                    consistency: Consistency::One,
                    values: None,
                    skip_metadata: false,
                    page_size: None,
                    paging_state: None,
                    timestamp: None,
                },
            })
            .await;
        assert!(create_result.is_ok());

        // Step 3: Insert data
        let insert_query = "
            INSERT INTO users (id, name, age, is_active)
            VALUES (1, 'Alice', 30, true)
        ";
        let insert_result = database
            .query(Query {
                query: insert_query.to_string(),
                query_options: QueryOptions {
                    consistency: Consistency::One,
                    values: None,
                    skip_metadata: false,
                    page_size: None,
                    paging_state: None,
                    timestamp: None,
                },
            })
            .await;
        assert!(insert_result.is_ok());

        // Step 4: Query a single record
        let select_query = "SELECT id, name, age, is_active FROM users WHERE id = 1";
        let select_result = database
            .query(Query {
                query: select_query.to_string(),
                query_options: QueryOptions {
                    consistency: Consistency::One,
                    values: None,
                    skip_metadata: false,
                    page_size: None,
                    paging_state: None,
                    timestamp: None,
                },
            })
            .await;
        assert!(select_result.is_ok());

        let mut result_iter = select_result.unwrap().result;

        // Step 5: Verify the data
        let row = result_iter.next().unwrap();
        assert_eq!(row[0], Some(Value::Int(1)));
        assert_eq!(row[1], Some(Value::Varchar("Alice".to_string())));
        assert_eq!(row[2], Some(Value::Int(30)));
        assert_eq!(row[3], Some(Value::Boolean(true)));

        // Ensure no additional rows are returned
        assert!(result_iter.next().is_none());
    }
}
